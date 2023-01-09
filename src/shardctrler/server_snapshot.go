package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
	"fmt"
)

func (sc *ShardCtrler) IfNeedToSendSnapshotCommand(raftIndex int, proportion int) {
	if sc.rf.GetRaftStateSize() > (sc.maxRaftState * proportion / 10) {
		snapshot := sc.MakeSnapshot()
		sc.rf.Snapshot(raftIndex, snapshot)
	}
}

func (sc *ShardCtrler) GetSnapshotFromRaft(message raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		sc.ReadSnapshotToInstall(message.Snapshot)
		sc.lastIncludeIndex = message.SnapshotIndex
	}
}

func (sc *ShardCtrler) MakeSnapshot() []byte {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.lastRequestId)
	data := w.Bytes()
	return data
}

func (sc *ShardCtrler) ReadSnapshotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistConfig []Config
	var persistLastRequestId map[int64]int

	if d.Decode(&persistConfig) != nil ||
		d.Decode(&persistLastRequestId) != nil {
		fmt.Printf("KVSERVER %d read persister got a problem!!!!!!!!!!\n", sc.me)
	} else {
		sc.configs = persistConfig
		sc.lastRequestId = persistLastRequestId
	}
}
