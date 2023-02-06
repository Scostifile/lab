package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
)

func (kv *ShardKV) IfNeedToSendSnapshotCommand(raftIndex int, proportion int) {
	kv.mu.Lock()
	raftSize := kv.rf.GetRaftStateSize()
	//configNum := kv.config.Num
	kv.mu.Unlock()
	if raftSize > (kv.maxraftstate * proportion / 10) {
		snapshot := kv.MakeSnapshot()
		kv.rf.Snapshot(raftIndex, snapshot)
		//if kv.gid == 101 {
		//	fmt.Printf("%v: gid=%v IfNeedToSendSnapshotCommand need to snap in configNum=%v------raftSize=%v lenOfLog=%v log=%v\n",
		//		kv.me, kv.gid, configNum, raftSize, kv.rf.LenOfLog(), kv.rf.GetLog())
		//	kv.mu.Lock()
		//	fmt.Printf("----kvdb=%v\n", kv.kvDB)
		//	fmt.Printf("-----len(kvDB[1])=%v\n", len(kv.kvDB[1].KVDBOfShard))
		//	kv.mu.Unlock()
		//}
	}
}

func (kv *ShardKV) GetSnapshotFromRaft(message raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		//if kv.gid == 101 {
		//	fmt.Printf("%v: gid=%v GetSnapshotFromRaft begin------raftSize=%v\n", kv.me, kv.gid, kv.rf.GetRaftStateSize())
		//}
		kv.ReadSnapshotToInstall(message.Snapshot)
		kv.lastIncludeIndex = message.SnapshotIndex
		//if kv.gid == 101 {
		//	fmt.Printf("%v: gid=%v GetSnapshotFromRaft end------raftSize=%v\n", kv.me, kv.gid, kv.rf.GetRaftStateSize())
		//}
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
