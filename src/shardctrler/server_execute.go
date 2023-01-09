package shardctrler

import (
	"6.824/raft"
)

func (sc *ShardCtrler) ReadRaftApplyCommandLoop() {
	for message := range sc.applyCh {
		if message.CommandValid {
			sc.GetCommandFromRaft(message)
		}
		if message.SnapshotValid {
			sc.GetSnapshotFromRaft(message)
		}
	}
}

func (sc *ShardCtrler) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= sc.lastIncludeIndex {
		return
	}
	if !sc.ifRequestDuplicate(op.ClientId, op.RequestId) {
		if op.Operation == JoinOp {
			sc.ExecuteJoinOnController(op)
		}
		if op.Operation == LeaveOp {
			sc.ExecuteLeaveOnController(op)
		}
		if op.Operation == MoveOp {
			sc.ExecuteMoveOnController(op)
		}
	}

	if sc.maxRaftState != -1 {
		sc.IfNeedToSendSnapshotCommand(message.CommandIndex, 9)
	}

	sc.SendMessageToWaitChannel(op, message.CommandIndex)
}

func (sc *ShardCtrler) SendMessageToWaitChannel(op Op, raftIndex int) {
	sc.mu.Lock()
	ch, exist := sc.waitApplyCh[raftIndex]
	sc.mu.Unlock()
	if exist {
		ch <- op
	}
}

func (sc *ShardCtrler) ExecuteQueryOnController(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	if op.Num_Query == -1 || op.Num_Query >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	} else {
		return sc.configs[op.Num_Query]
	}
}

func (sc *ShardCtrler) ExecuteJoinOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, *sc.MakeJoinConfig(op.Servers_Join))
}

func (sc *ShardCtrler) ExecuteLeaveOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, *sc.MakeLeaveConfig(op.Gids_Leave))
}

func (sc *ShardCtrler) ExecuteMoveOnController(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestId[op.ClientId] = op.RequestId
	sc.configs = append(sc.configs, *sc.MakeMoveConfig(op.Shard_Move, op.Gid_Move))
}

func (sc *ShardCtrler) ifRequestDuplicate(newClientId int64, newRequestId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastRequestId, ifClientInRecord := sc.lastRequestId[newClientId]
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}
