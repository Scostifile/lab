package shardkv3_0

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"fmt"
	"sync/atomic"
	"time"
)
import "sync"

type Op struct {
	OpType string "operation type(eg. put/append/gc/get)"
	Key    string "key for normal, config num for gc"
	Value  string
	Cid    int64 "cid for put/append, operation uid for get/gc"
	SeqNum int   "seqnum for put/append, shard for gc"
}

const (
	ConfigureMonitorTimeout = 100 * time.Millisecond
	MigrationMonitorTimeout = 150 * time.Millisecond
	GCMonitorTimeout        = 150 * time.Millisecond

	GETOp    = "Get"
	PUTOp    = "Put"
	APPENDOp = "Append"
)

type ShardKV struct {
	mu      sync.RWMutex
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	makeEnd func(string) *labrpc.ClientEnd
	gid     int
	sc      *shardctrler.Clerk

	maxRaftState     int // snapshot if log grows this big
	lastIncludeIndex int // record the lastIncludeIndex to prevent stateMachine from rollback

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config

	stateMachines  map[int]*Shard                // KV stateMachines
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}

func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for kv.Killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandRequest{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationResponse{})
	labgob.Register(ShardOperationRequest{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		dead:             0,
		rf:               raft.Make(servers, me, persister, applyCh),
		applyCh:          applyCh,
		makeEnd:          makeEnd,
		gid:              gid,
		sc:               shardctrler.MakeClerk(ctrlers),
		lastIncludeIndex: 0,
		maxRaftState:     maxRaftState,
		currentConfig:    shardctrler.Config{},
		lastConfig:       shardctrler.Config{},
		stateMachines:    make(map[int]*Shard),
		lastOperations:   make(map[int64]OperationContext),
		notifyChans:      make(map[int]chan *CommandResponse),
	}

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapshotToInstall(snapshot)
	}

	// start applier goroutine to apply committed logs to stateMachine
	go kv.applier()
	// start configuration monitor goroutine to fetch latest configuration
	go kv.Monitor(kv.configureAction, ConfigureMonitorTimeout)
	// start migration monitor goroutine to pull related shards
	go kv.Monitor(kv.migrationAction, MigrationMonitorTimeout)
	// start gc monitor goroutine to delete useless shards in remote groups
	go kv.Monitor(kv.gcAction, GCMonitorTimeout)
	// start entry-in-currentTerm monitor goroutine to advance commitIndex by appending empty entries in current term periodically to avoid live locks
	go kv.Monitor(kv.checkEntryInCurrentTermAction, EmptyEntryDetectorTimeout)

	DPrintf("{Node %v}{Group %v} has started", kv.rf.Me(), kv.gid)
	return kv
}

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *ShardKV) applier() {
	for kv.Killed() == false {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v}{Group %v} tries to apply message %v", kv.rf.Me(), kv.gid, message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastIncludeIndex {
					DPrintf("{Node %v}{Group %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), kv.gid, message, kv.lastIncludeIndex)
					kv.mu.Unlock()
					continue
				}

				var response *CommandResponse
				command := message.Command.(Command)
				switch command.Op {
				case Operation:
					operation := command.Data.(CommandRequest)
					response = kv.applyOperation(&message, &operation)
				case Configuration:
					nextConfig := command.Data.(shardctrler.Config)
					response = kv.applyConfiguration(&nextConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationResponse)
					response = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					shardsInfo := command.Data.(ShardOperationRequest)
					response = kv.applyDeleteShards(&shardsInfo)
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getWaitChannel(message.CommandIndex)
					ch <- response
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastIncludeIndex = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

func (kv *ShardKV) getWaitChannel(raftIndex int) chan Op {
	kv.mu.Lock()
	channelRaftIndex, exist := kv.waitApplyChannel[raftIndex]
	if !exist {
		kv.waitApplyChannel[raftIndex] = make(chan Op, 1)
		channelRaftIndex = kv.waitApplyChannel[raftIndex]
	}
	kv.mu.Unlock()
	return channelRaftIndex
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	originOp := Op{"Get", args.Key, "", Nrand(), 0}
	reply.Err, reply.Value = kv.raftStartOperation(originOp)
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originOp := Op{args.Op, args.Key, args.Value, args.Cid, args.SeqNum}
	reply.Err, _ = kv.raftStartOperation(originOp)
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
