package shardkv

import (
	"6.824/labrpc"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"

const (
	CONSENSUS_TIMEOUT   = 500
	CONFIGCHECK_TIMEOUT = 150
	SENDSHARDS_TIMEOUT  = 150
	NShards             = shardctrler.NShards

	GETOp          = "Get"
	PUTOp          = "Put"
	APPENDOp       = "Append"
	MIGRATESHARDOp = "Migrate"
	NEWCONFIGOp    = "NewConfig"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation           string
	Key                 string
	Value               string
	ClientId            int64
	RequestId           int
	Config_NEWCONFIG    shardctrler.Config
	MigrateData_MIGRATE []ShardComponent
	ConfigNum_MIGRATE   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32
	mck  *shardctrler.Clerk

	kvDB             []ShardComponent // every Shard has it's independent data
	waitApplyChannel map[int]chan Op

	lastIncludeIndex int
	config           shardctrler.Config
	migratingShard   [NShards]bool // use to lock the migrating shards that others can't operate
}

func (kv *ShardKV) CheckShardState(clientNum int, shardIndex int) (bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num == clientNum &&
			kv.config.Shards[shardIndex] == kv.gid,
		!kv.migratingShard[shardIndex]
}

func (kv *ShardKV) CheckMigrateState(shardComponent []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shardData := range shardComponent {
		if kv.migratingShard[shardData.ShardIndex] {
			return false
		}
	}
	return true
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
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	shardIndex := key2shard(args.Key)
	ifResponse, ifAvailable := kv.CheckShardState(args.ConfigNum, shardIndex)
	if !ifResponse {
		reply.Err = ErrWrongGroup
		return
	}
	if !ifAvailable {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: GETOp,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	raftIndex, _, _ := kv.rf.Start(op)
	channelRaftIndex := kv.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		_, ifLeaderState := kv.rf.GetState()
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) &&
			ifLeaderState {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channelRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyChannel, raftIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shardIndex := key2shard(args.Key)
	ifResponse, ifAvailable := kv.CheckShardState(args.ConfigNum, shardIndex)

	if !ifResponse {
		reply.Err = ErrWrongGroup
		return
	}
	if !ifAvailable {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	raftIndex, _, _ := kv.rf.Start(op)
	channelRaftIndex := kv.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channelRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyChannel, raftIndex)
	kv.mu.Unlock()
}

// "MigrateShard" RPC Handler
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()

	if args.ConfigNum > myConfigNum {
		reply.Err = ErrConfigNum
		reply.ConfigMum = myConfigNum
		return
	}

	if args.ConfigNum < myConfigNum {
		reply.Err = OK
		return
	}

	if kv.CheckMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}

	op := Op{
		Operation:           MIGRATESHARDOp,
		MigrateData_MIGRATE: args.MigrateData,
		ConfigNum_MIGRATE:   args.ConfigNum,
	}
	raftIndex, _, _ := kv.rf.Start(op)
	channelRaftIndex := kv.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		_, ifLeader := kv.rf.GetState()
		kv.mu.Lock()
		tempConfigNum := kv.config.Num
		kv.mu.Unlock()

		if args.ConfigNum <= tempConfigNum &&
			kv.CheckMigrateState(args.MigrateData) &&
			ifLeader {
			reply.ConfigMum = tempConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-channelRaftIndex:
		kv.mu.Lock()
		tempConfigNum := kv.config.Num
		kv.mu.Unlock()

		if raftCommitOp.ConfigNum_MIGRATE == args.ConfigNum &&
			args.ConfigNum <= tempConfigNum &&
			kv.CheckMigrateState(args.MigrateData) {
			reply.ConfigMum = tempConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyChannel, raftIndex)
	kv.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.kvDB = make([]ShardComponent, NShards)
	for shard := 0; shard < NShards; shard++ {
		kv.kvDB[shard] = ShardComponent{
			shard,
			make(map[string]string),
			make(map[int64]int),
		}
	}

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitApplyChannel = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapshotToInstall(snapshot)
	}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ReadRaftApplyCommandLoop()
	go kv.PullNewConfigLoop()
	go kv.SendShardToOtherGroupLoop()

	return kv
}
