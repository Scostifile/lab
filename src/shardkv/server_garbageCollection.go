package shardkv

func (kv *ShardKV) ExecuteGarbageCollectionOpOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	if op.ConfigNum_MIGRATE != myConfig.Num {
		return
	}
	//fmt.Printf("%v: gid=%v ExecuteGarbageCollectionOpOnServer migrateShards=%v in configNum=%v begin KVDB=%v\n",
	//	kv.me, kv.gid, kv.migratingShard, kv.config.Num, kv.kvDB)

	// data that is migrated out and need to be cleaned
	for _, shardComponent := range op.MigrateData_MIGRATE {
		if kv.migratingShard[shardComponent.ShardIndex] == BePulling {
			kv.kvDB[shardComponent.ShardIndex] = ShardComponent{
				shardComponent.ShardIndex,
				make(map[string]string),
				make(map[int64]int),
			}
			kv.migratingShard[shardComponent.ShardIndex] = Normal
		}
	}
	//fmt.Printf("%v: gid=%v ExecuteGarbageCollectionOpOnServer rf.raftSize=%v in configNum= %v KVDB=%v\n",
	//	kv.me, kv.gid, kv.rf.GetRaftStateSize(), kv.config.Num, kv.kvDB)

	//fmt.Printf("%v: gid=%v ExecuteGarbageCollectionOpOnServer migrateShards=%v in configNum=%v after KVDB=%v\n",
	//	kv.me, kv.gid, kv.migratingShard, kv.config.Num, kv.kvDB)
}
