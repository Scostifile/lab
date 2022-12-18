package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
)

func (kv *KVServer) ifNeedToSendSnapshotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		//fmt.Printf("KVServer:%v before make a snapshot, its size:%v\n", kv.me, kv.rf.GetRaftStateSize())
		snapshot := kv.MakeSnapshot()
		//fmt.Printf("KVServer:%v make a snapshot DB:%v\n", kv.me, kv.kvDB["0"])
		kv.rf.Snapshot(raftIndex, snapshot)
		//.Printf("KVServer:%v after make a snapshot, its size:%v\n", kv.me, kv.rf.GetRaftStateSize())
	}
}

func (kv *KVServer) GetSnapshotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		if kv.rf.GetRaftStateSize() > 8000 {
			fmt.Printf("%v: GetSnapshotFromRaft before size is %v\n", kv.me, kv.rf.GetRaftStateSize())
		}
		kv.ReadSnapshotToInstallL(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
		if kv.rf.GetRaftStateSize() > 8000 {
			fmt.Printf("%v: GetSnapshotFromRaft after size is %v\n", kv.me, kv.rf.GetRaftStateSize())
		}
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

func (kv *KVServer) ReadSnapshotToInstallL(snapshot []byte) {
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
		//fmt.Printf("KVServer:%v read a snapshot to install DB:%v\n", kv.me, kv.kvDB["0"])
		kv.lastRequestId = persistLastRequestId
	}
}
