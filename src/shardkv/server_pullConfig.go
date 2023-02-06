package shardkv

import (
	"time"
)

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
	for !kv.Killed() {
		time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
		_, ifLeader := kv.rf.GetState()
		if !ifLeader || !kv.isCompleteMigration() {
			//kv.mu.Lock()
			//fmt.Printf("%v: gid=%v PullNewConfigLoop configShard=%v in currentConfigNum=%v miagrateShard=%v \n",
			//	kv.me, kv.gid, kv.config.Shards, kv.config.Num, kv.migratingShard)
			//kv.mu.Unlock()
			continue
		}

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
