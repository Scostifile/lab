package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
	"log"
)

func (kv *KVServer) ifNeedToSendSnapshotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		snapshot := kv.MakeSnapshot()
		kv.rf.Snapshot(raftIndex, snapshot)
		//fmt.Printf("KVServer snapshot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")
	}
}

func (kv *KVServer) GetSnapshotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		kv.ReadSnapshotToInstall(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
	}
}

func (kv *KVServer) MakeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.lastRequestId)
	data := w.Bytes()
	return data
}

func (kv *KVServer) ReadSnapshotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistKVDB map[string]string
	var persistLastRequestId map[int64]int

	if d.Decode(&persistKVDB) != nil || d.Decode(&persistLastRequestId) != nil {
		log.Fatalf("%v ReadSnapshotToInstall failed!", kv.me)
	} else {
		kv.kvDB = persistKVDB
		kv.lastRequestId = persistLastRequestId
	}
}
