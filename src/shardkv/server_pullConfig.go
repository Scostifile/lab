package shardkv

func (kv *ShardKV) isCompleteMigration() bool {
	// all migrate shard should be finished
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for shard := 0; shard < NShards; shard++ {
		if kv.migratingShard[shard] != Normal {
			return false
		}
	}
	return true
}

func (kv *ShardKV) PullNewConfigLoop() {
	if kv.isCompleteMigration() {
		kv.mu.Lock()
		lastConfigNum := kv.config.Num
		kv.mu.Unlock()

		newConfig := kv.mck.Query(lastConfigNum + 1)
		if newConfig.Num == lastConfigNum+1 {
			op := Op{
				Operation:        NEWCONFIGOp,
				Config_NEWCONFIG: newConfig,
			}
			if _, ifLeader := kv.rf.GetState(); ifLeader {
				kv.rf.Start(op)
			}
		}
	}
}
