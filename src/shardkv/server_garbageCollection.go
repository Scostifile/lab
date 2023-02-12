package shardkv

func (kv *ShardKV) ExecuteGarbageCollectionOpOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.ConfigNum_MIGRATE != kv.config.Num {
		return
	}

	// data that is migrated out and need to be cleaned
	for _, shardComponent := range op.MigrateData_MIGRATE {
		if kv.migratingShard[shardComponent.ShardIndex] == BePulling {
			kv.migratingShard[shardComponent.ShardIndex] = Normal
			kv.kvDB[shardComponent.ShardIndex] = ShardComponent{
				shardComponent.ShardIndex,
				make(map[string]string),
				make(map[int64]int),
			}
		}
	}
}
