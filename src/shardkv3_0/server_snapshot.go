package shardkv2_0

import (
	"6.824/labgob"
	"6.824/shardctrler"
	"bytes"
	"log"
)

//
//import (
//	"6.824/labgob"
//	"6.824/raft"
//	"6.824/shardctrler"
//	"bytes"
//	"fmt"
//)
//
//func (kv *ShardKV) IfNeedToSendSnapshotCommand(raftIndex int, proportion int) {
//	if kv.rf.GetRaftStateSize() > (kv.maxraftstate * proportion / 10) {
//		snapshot := kv.MakeSnapshot()
//		kv.rf.Snapshot(raftIndex, snapshot)
//	}
//}
//
//func (kv *ShardKV) GetSnapshotFromRaft(message raft.ApplyMsg) {
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
//		kv.ReadSnapshotToInstall(message.Snapshot)
//		kv.lastIncludeIndex = message.SnapshotIndex
//	}
//}
//
//func (kv *ShardKV) MakeSnapshot() []byte {
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//	e.Encode(kv.kvDB)
//	e.Encode(kv.config)
//	e.Encode(kv.migratingShard)
//	data := w.Bytes()
//	return data
//}
//
//func (kv *ShardKV) ReadSnapshotToInstall(snapshot []byte) {
//	if snapshot == nil || len(snapshot) < 1 {
//		return
//	}
//
//	r := bytes.NewBuffer(snapshot)
//	d := labgob.NewDecoder(r)
//
//	var persistKVDB []ShardComponent
//	var persistConfig shardctrler.Config
//	var persistMigratingShard [NShards]bool
//
//	if d.Decode(&persistKVDB) != nil ||
//		d.Decode(&persistConfig) != nil ||
//		d.Decode(&persistMigratingShard) != nil {
//		fmt.Printf("KVSERVER %d read persister got a problem!!!!!!!!!!", kv.me)
//	} else {
//		kv.kvDB = persistKVDB
//		kv.config = persistConfig
//		kv.migratingShard = persistMigratingShard
//	}
//}

func (kv *ShardKV) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	var toOutShards map[int]map[int]map[string]string
	var comeInShards map[int]map[int]bool
	var myShards map[int]bool
	var garbage map[int]map[int]bool
	var cfg shardctrler.Config
	var preCfg shardctrler.Config
	var configMap map[int]shardctrler.Config
	if d.Decode(&db) != nil ||
		d.Decode(&cid2Seq) != nil ||
		d.Decode(&comeInShards) != nil ||
		d.Decode(&toOutShards) != nil ||
		d.Decode(&myShards) != nil ||
		d.Decode(&cfg) != nil ||
		d.Decode(&preCfg) != nil ||
		d.Decode(&garbage) != nil ||
		d.Decode(&configMap) != nil {
		log.Fatalf("readSnapShot ERROR for server %v", kv.me)
	} else {
		kv.db, kv.cid2Seq, kv.currentConfig, kv.previousConfig = db, cid2Seq, cfg, preCfg
		kv.configNum2migrateOutShardsDB, kv.configNum2ComeInShards, kv.validShards, kv.garbage = toOutShards, comeInShards, myShards, garbage
		kv.configMap = configMap
	}
}

func (kv *ShardKV) needSnapShot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	threshold := 10
	return kv.maxraftstate > 0 &&
		kv.maxraftstate-kv.persist.RaftStateSize() < kv.maxraftstate/threshold
}

func (kv *ShardKV) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cid2Seq)
	e.Encode(kv.configNum2ComeInShards)
	e.Encode(kv.configNum2migrateOutShardsDB)
	e.Encode(kv.validShards)
	e.Encode(kv.currentConfig)
	e.Encode(kv.previousConfig)
	e.Encode(kv.garbage)
	e.Encode(kv.configMap)
	kv.mu.Unlock()
	kv.rf.Snapshot(index, w.Bytes())
}
