package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Debug             = false
	CONSENSUS_TIMEOUT = 500 // ms
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB          map[string]string
	waitApplyCh   map[int]chan Op
	lastRequestId map[int64]int

	lastIncludeIndex int
}

func (kv *KVServer) getWaitChannel(raftIndex int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	channelForRaftIndex, exist := kv.waitApplyCh[raftIndex]
	if !exist {
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		channelForRaftIndex = kv.waitApplyCh[raftIndex]
	}
	//fmt.Printf("%v: getWaitChannel raftIndex=%v exist=%v\n", kv.me, raftIndex, exist)
	return channelForRaftIndex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		//fmt.Printf("KVServer:%v Get GetState=not leader\n", kv.me)
		return
	}

	op := Op{
		"Get",
		args.Key,
		"",
		args.ClientId,
		args.RequestId,
	}
	raftIndex, _, _ := kv.rf.Start(op)

	channelForRaftIndex := kv.getWaitChannel(raftIndex)
	//fmt.Printf("KVServer:%v Get Start key=%v value=%v\n", kv.me, op.Key, op.Value)
	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		//fmt.Printf("KVServer:%v Get timeout key=%v value=%v\n", kv.me, op.Key, op.Value)
		_, ifLeader := kv.rf.GetState()
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) && ifLeader {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
				//fmt.Printf("KVServer:%v Get OK key=%v value=%v\n", kv.me, op.Key, op.Value)
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-channelForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			value, exist := kv.ExecuteGetOpOnKVDB(op)
			if exist {
				reply.Err = OK
				reply.Value = value
				//fmt.Printf("KVServer:%v Get OK key=%v value=%v\n", kv.me, op.Key, op.Value)
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
				//fmt.Printf("KVServer:%v Get nokey key=%v value=%v\n", kv.me, op.Key, op.Value)
			}
		} else {
			reply.Err = ErrWrongLeader
			//fmt.Printf("KVServer:%v Get wrongleader key=%v value=%v\n", kv.me, op.Key, op.Value)
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		//fmt.Printf("KVServer:%v PutAppend to incorrect leader\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		args.Op,
		args.Key,
		args.Value,
		args.ClientId,
		args.RequestId,
	}

	raftIndex, _, _ := kv.rf.Start(op)
	//fmt.Printf("KVServer:%v PutAppend Start raftIndex=%v key=%v value=%v\n", kv.me, raftIndex, args.Key, args.Value)
	channelForRaftIndex := kv.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Err = OK
			fmt.Printf("KVServer:%v raftIndex=%v duplicate command timeout!!!! key=%v, value=%v\n", kv.me, raftIndex, args.Key, args.Value)
		} else {
			//fmt.Printf("KVServer:%v timeout execute!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", kv.me)
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-channelForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int)

	//kv.lastIncludeIndex = -1

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapshotToInstallL(snapshot)
	}

	go kv.ReadRaftApplyCommandLoop()
	return kv
}
