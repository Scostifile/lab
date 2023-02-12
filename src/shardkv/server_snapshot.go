package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
)

func (kv *ShardKV) IfNeedToSendSnapshotCommand(raftIndex int, proportion int) {
	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
		//fmt.Printf("%v: gid=%v IfNeedToSendSnapshotCommand KVDB=%v\n", kv.me, kv.gid, kv.kvDB)
		snapshot := kv.MakeSnapshot()
		kv.rf.Snapshot(raftIndex, snapshot)
	}
}

func (kv *ShardKV) GetSnapshotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		//fmt.Printf("%v: gid=%v GetSnapshotFromRaft KVDB=%v\n", kv.me, kv.gid, kv.kvDB)
		kv.ReadSnapshotToInstall(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
	}
}

func (kv *ShardKV) MakeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.config)
	e.Encode(kv.migratingShard)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) ReadSnapshotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistKVDB []ShardComponent
	var persistConfig shardctrler.Config
	var persistMigratingShard [NShards]ShardStatus

	if d.Decode(&persistKVDB) != nil ||
		d.Decode(&persistConfig) != nil ||
		d.Decode(&persistMigratingShard) != nil {
		fmt.Printf("KVSERVER %d read persister got a problem!!!!!!!!!!", kv.me)
	} else {
		kv.kvDB = persistKVDB
		kv.config = persistConfig
		kv.migratingShard = persistMigratingShard
	}
}
