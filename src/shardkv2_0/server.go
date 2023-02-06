package shardkv2_0

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
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

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	mck            *shardctrler.Clerk
	previousConfig shardctrler.Config
	currentConfig  shardctrler.Config
	persist        *raft.Persister
	db             map[string]string
	chMap          map[int]chan Op
	cid2Seq        map[int64]int

	configMap                    map[int]shardctrler.Config
	configNum2migrateOutShardsDB map[int]map[int]map[string]string "cfg num -> (shard -> db)"
	configNum2ComeInShards       map[int]map[int]bool              "shard->true"
	validShards                  map[int]bool                      "to record which shard i can offer service"
	garbage                      map[int]map[int]bool              "cfg number -> shards"

	killCh chan bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	originOp := Op{"Get", args.Key, "", Nrand(), 0}
	reply.Err, reply.Value = kv.raftStartOperation(originOp)
}
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originOp := Op{args.Op, args.Key, args.Value, args.Cid, args.SeqNum}
	reply.Err, _ = kv.raftStartOperation(originOp)
}

func (kv *ShardKV) getMessageFromRaft(ch chan Op, index int) Op {
	select {
	case notifyArg, ok := <-ch:
		if ok {
			close(ch)
		}
		kv.mu.Lock()
		delete(kv.chMap, index)
		kv.mu.Unlock()
		return notifyArg
	case <-time.After(time.Duration(1000) * time.Millisecond):
		return Op{}
	}
}
func (kv *ShardKV) getChannelWithIndex(idx int, createIfNotExists bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		if !createIfNotExists {
			return nil
		}
		kv.chMap[idx] = make(chan Op, 1)
	}
	return kv.chMap[idx]
}

func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.OpType == b.OpType && a.SeqNum == b.SeqNum && a.Cid == b.Cid
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	select {
	case <-kv.killCh:
	default:
	}
	kv.killCh <- true
}

func (kv *ShardKV) raftStartOperation(originOp Op) (Err, string) {
	index, _, isLeader := kv.rf.Start(originOp)
	if isLeader {
		ch := kv.getChannelWithIndex(index, true)
		op := kv.getMessageFromRaft(ch, index)
		if equalOp(originOp, op) {
			return OK, op.Value
		}
		if op.OpType == ErrWrongGroup {
			return ErrWrongGroup, ""
		}
	}
	return ErrWrongLeader, ""
}

func sendMessageToChannel(notifyCh chan Op, op Op) {
	select {
	case <-notifyCh:
	default:
	}
	notifyCh <- op
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	// Your initialization code here.
	kv.persist = persister
	// Use something like this to talk to the shardmaster:
	kv.mck = shardctrler.MakeClerk(kv.masters)
	kv.previousConfig = shardctrler.Config{}
	kv.currentConfig = shardctrler.Config{}

	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.cid2Seq = make(map[int64]int)
	kv.configMap = make(map[int]shardctrler.Config)

	kv.configNum2migrateOutShardsDB = make(map[int]map[int]map[string]string)
	kv.configNum2ComeInShards = make(map[int]map[int]bool)
	kv.validShards = make(map[int]bool)
	kv.garbage = make(map[int]map[int]bool)

	kv.readSnapShot(kv.persist.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool, 1)
	go kv.processLoop(kv.tryPollNewCfg, 100)
	go kv.processLoop(kv.tryPullShard, 150)
	go kv.processLoop(kv.tryGC, 100)
	go func() {
		for {
			select {
			case <-kv.killCh:
				return
			case applyMsg := <-kv.applyCh:
				if !applyMsg.CommandValid {
					kv.readSnapShot(applyMsg.Snapshot)
					continue
				}
				kv.apply(applyMsg)
			}
		}
	}()
	return kv
}
