package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
	"fmt"
)

func (kv *ShardCtrler) IfNeedToSendSnapshotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxRaftState * proportion / 10) {
		snapshot := kv.MakeSnapshot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *ShardCtrler) GetSnapshotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapshotToInstall(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
	}
}

func (kv *ShardCtrler) MakeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.configs)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

func (kv *ShardCtrler) ReadSnapshotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistConfig []Config
	var persistLastRequestId map[int64]int

	if d.Decode(&persistConfig) != nil ||
		d.Decode(&persistLastRequestId) != nil {
		fmt.Printf("KVSERVER %d read persister got a problem!!!!!!!!!!\n", kv.me)
	} else {
		kv.configs = persistConfig
		kv.lastRequestId = persistLastRequestId
	}
}
