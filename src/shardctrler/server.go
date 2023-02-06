package shardctrler

import (
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const (
	CONSENSUS_TIMEOUT = 5000 // ms
	QueryOp           = "query"
	JoinOp            = "join"
	LeaveOp           = "leave"
	MoveOp            = "move"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxRaftState     int
	waitApplyCh      map[int]chan Op // index(raft) -> channel
	lastRequestId    map[int64]int   // clientId -> requestId
	lastIncludeIndex int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operation    string
	ClientId     int64
	RequestId    int
	Num_Query    int
	Servers_Join map[int][]string
	Gids_Leave   []int
	Shard_Move   int
	Gid_Move     int
}

func (sc *ShardCtrler) getWaitChannel(raftIndex int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	channelForRaftIndex, exist := sc.waitApplyCh[raftIndex]
	if !exist {
		sc.waitApplyCh[raftIndex] = make(chan Op, 1)
		channelForRaftIndex = sc.waitApplyCh[raftIndex]
	}
	return channelForRaftIndex
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation:    JoinOp,
		ClientId:     args.ClientId,
		RequestId:    args.RequestId,
		Servers_Join: args.Servers,
	}
	raftIndex, _, _ := sc.rf.Start(op)
	channelForRaftIndex := sc.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channelForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation:  LeaveOp,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
		Gids_Leave: args.GIDs,
	}
	raftIndex, _, _ := sc.rf.Start(op)
	channelForRaftIndex := sc.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channelForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation:  MoveOp,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
		Shard_Move: args.Shard,
		Gid_Move:   args.GID,
	}
	raftIndex, _, _ := sc.rf.Start(op)
	channelForRaftIndex := sc.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channelForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: QueryOp,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		Num_Query: args.Num,
	}
	raftIndex, _, _ := sc.rf.Start(op)
	channelForRaftIndex := sc.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		_, ifLeader := sc.rf.GetState()
		if sc.ifRequestDuplicate(op.ClientId, op.RequestId) && ifLeader {
			reply.Config = sc.ExecuteQueryOnController(op)
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channelForRaftIndex:
		if raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId {
			reply.Config = sc.ExecuteQueryOnController(op)
			reply.Err = OK
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, raftIndex)
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.maxRaftState = -1

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.waitApplyCh = make(map[int]chan Op)
	sc.lastRequestId = make(map[int64]int)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sc.ReadSnapshotToInstall(snapshot)
	}
	go sc.ReadRaftApplyCommandLoop()

	return sc
}
