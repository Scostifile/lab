package shardkv2_0

import (
	"strconv"
	"sync"
)

func (kv *ShardKV) GarbageCollection(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	if _, ok := kv.configNum2migrateOutShardsDB[args.ConfigNum]; !ok {
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.configNum2migrateOutShardsDB[args.ConfigNum][args.Shard]; !ok {
		kv.mu.Unlock()
		return
	}
	gcOp := Op{"GC", strconv.Itoa(args.ConfigNum), "", Nrand(), args.Shard}
	kv.mu.Unlock()
	reply.Err, _ = kv.raftStartOperation(gcOp)
}

func (kv *ShardKV) tryGC() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbage) == 0 {
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for cfgNum, shards := range kv.garbage {
		cfg := kv.configMap[cfgNum]
		for shard := range shards {
			wait.Add(1)
			go func(shard int, cfgNum int) {
				defer wait.Done()
				args := MigrateArgs{shard, cfgNum}
				gid := cfg.Shards[shard]
				for _, server := range cfg.Groups[gid] {
					srv := kv.make_end(server)
					reply := MigrateReply{}
					if ok := srv.Call("ShardKV.GarbageCollection", &args, &reply); ok && reply.Err == OK {
						kv.mu.Lock()
						delete(kv.garbage[cfgNum], shard)
						if len(kv.garbage[cfgNum]) == 0 {
							delete(kv.garbage, cfgNum)
						}
						kv.mu.Unlock()
					}
				}
			}(shard, cfgNum)
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) garbageCollectionProcess(cfgNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.configNum2migrateOutShardsDB[cfgNum]; exist {
		delete(kv.configNum2migrateOutShardsDB[cfgNum], shard)
		if len(kv.configNum2migrateOutShardsDB[cfgNum]) == 0 {
			delete(kv.configNum2migrateOutShardsDB, cfgNum)
		}
	}
}
