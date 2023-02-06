package shardkv2_0

import (
	"6.824/shardctrler"
	"fmt"
	"log"
	"sync"
)

//
//import (
//	"fmt"
//	"time"
//)
//
//func (kv *ShardKV) PullNewConfigLoop() {
//	for !kv.Killed() {
//		_, ifLeader := kv.rf.GetState()
//		if !ifLeader {
//			time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
//			continue
//		}
//
//		kv.mu.Lock()
//		lastConfigNum := kv.config.Num
//		kv.mu.Unlock()
//
//		newConfig := kv.mck.Query(lastConfigNum + 1)
//		if newConfig.Num == lastConfigNum+1 {
//			op := Op{
//				Operation:        NEWCONFIGOp,
//				Config_NEWCONFIG: newConfig,
//			}
//			if _, ifLeader := kv.rf.GetState(); ifLeader {
//				kv.rf.Start(op)
//			}
//		}
//
//		time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
//	}
//}
//
//func (kv *ShardKV) SendShardToOtherGroupLoop() {
//	for !kv.Killed() {
//		time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
//		_, ifLeader := kv.rf.GetState()
//		if !ifLeader {
//			//time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
//			continue
//		}
//
//		noMigrating := true
//		kv.mu.Lock()
//		for shard := 0; shard < NShards; shard++ {
//			if kv.migratingShard[shard] {
//				noMigrating = false
//				break
//			}
//		}
//		kv.mu.Unlock()
//
//		if noMigrating {
//			//time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
//			continue
//		}
//
//		ifNeedSend, sendData := kv.ifHaveSendData()
//		if !ifNeedSend {
//			//time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
//			continue
//		}
//
//		kv.sendShardComponent(sendData)
//		//time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
//	}
//}
//
//func (kv *ShardKV) ifHaveSendData() (bool, map[int][]ShardComponent) {
//	sendData := kv.MakeSendShardComponent()
//	if len(sendData) == 0 {
//		return false, make(map[int][]ShardComponent)
//	}
//	return true, sendData
//}
//
//func (kv *ShardKV) MakeSendShardComponent() map[int][]ShardComponent {
//	// kv.config already be updated
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//	sendData := make(map[int][]ShardComponent)
//	for shardIndex := 0; shardIndex < NShards; shardIndex++ {
//		nowOwner := kv.config.Shards[shardIndex]
//		if kv.migratingShard[shardIndex] && kv.gid != nowOwner {
//			if nowOwner == 0 {
//				fmt.Print("nowOwer = 0!!!!!!!!!!!!!!!!!!\n")
//			}
//			tempComponent := ShardComponent{
//				ShardIndex:      shardIndex,
//				KVDBOfShard:     make(map[string]string),
//				ClientRequestId: make(map[int64]int),
//			}
//			CloneShardComponent(&tempComponent, kv.kvDB[shardIndex])
//			sendData[nowOwner] = append(sendData[nowOwner], tempComponent)
//		}
//	}
//	return sendData
//}
//
//func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
//	for gid, shardComponents := range sendData {
//		if _, isLeader := kv.rf.GetState(); !isLeader {
//			return
//		}
//		kv.mu.Lock()
//		args := &MigrateShardArgs{
//			ConfigNum:   kv.config.Num,
//			MigrateData: make([]ShardComponent, 0),
//		}
//		groupServers := kv.config.Groups[gid]
//		kv.mu.Unlock()
//
//		for _, component := range shardComponents {
//			tempComponent := ShardComponent{
//				ShardIndex:      component.ShardIndex,
//				KVDBOfShard:     make(map[string]string),
//				ClientRequestId: make(map[int64]int),
//			}
//			CloneShardComponent(&tempComponent, component)
//			args.MigrateData = append(args.MigrateData, tempComponent)
//		}
//
//		go kv.migrateProcess(groupServers, args)
//	}
//}
//
//func (kv *ShardKV) migrateProcess(groupServers []string, args *MigrateShardArgs) {
//	for _, groupMember := range groupServers {
//		if _, isLeader := kv.rf.GetState(); !isLeader {
//			return
//		}
//		peersEnd := kv.make_end(groupMember)
//		migrateReply := MigrateShardReply{}
//		ok := peersEnd.Call("ShardKV.MigrateShard", args, &migrateReply)
//		kv.mu.Lock()
//		myConfigNum := kv.config.Num
//		kv.mu.Unlock()
//		if ok && migrateReply.Err == OK {
//			if _, isLeader := kv.rf.GetState(); !isLeader {
//				return
//			}
//			if myConfigNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData) {
//				//fmt.Printf("%v: migrateProcess myConfigNum=%v arg.ConfigNum=%v\n",
//				//	kv.me, myConfigNum, args.ConfigNum)
//				return
//			} else {
//				op := Op{
//					Operation:           MIGRATESHARDOp,
//					MigrateData_MIGRATE: args.MigrateData,
//					ConfigNum_MIGRATE:   args.ConfigNum,
//				}
//				kv.rf.Start(op)
//				return
//			}
//		}
//	}
//}

func (kv *ShardKV) tryPollNewCfg() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.configNum2ComeInShards) > 0 {
		kv.mu.Unlock()
		return
	}
	next := kv.currentConfig.Num + 1
	kv.mu.Unlock()
	cfg := kv.mck.Query(next)
	if cfg.Num == next {
		kv.rf.Start(cfg) //sync followers with new cfg
	}
}

func (kv *ShardKV) tryPullShard() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.configNum2ComeInShards) == 0 {
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	currentConfig := kv.currentConfig
	previousCfg := kv.previousConfig
	if currentConfig.Num != previousCfg.Num+1 {
		log.Fatalf("currentConfigNum=%v != previousConfigNum=%v\n", currentConfig.Num, previousCfg.Num)
	}
	for shard, _ := range kv.configNum2ComeInShards[currentConfig.Num] {
		wait.Add(1)
		go func(shard int) {
			defer wait.Done()
			args := MigrateArgs{shard, currentConfig.Num}
			gid := previousCfg.Shards[shard]
			for _, server := range previousCfg.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK {
					kv.rf.Start(reply)
					break
				}
			}
		}(shard)
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum > kv.currentConfig.Num {
		return
	}
	reply.Err, reply.ConfigNum, reply.Shard = OK, args.ConfigNum, args.Shard
	reply.DB, reply.Cid2Seq = kv.cloneDBAndDeduplicateMap(args.ConfigNum, args.Shard)
}

func (kv *ShardKV) updateDBWithMigrateInData(migrationData MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if migrationData.ConfigNum != kv.currentConfig.Num {
		fmt.Printf("%v: gid=%v migrate.configNum=%v != currentConfigNum=%v!\n",
			kv.me, kv.gid, migrationData.ConfigNum, kv.currentConfig.Num)
		return
	}
	delete(kv.configNum2ComeInShards[migrationData.ConfigNum], migrationData.Shard)
	if len(kv.configNum2ComeInShards[migrationData.ConfigNum]) == 0 {
		delete(kv.configNum2ComeInShards, migrationData.ConfigNum)
	}
	//this check is necessary, to avoid use  kv.cfg.Num-1 to update kv.cfg.Num's shard
	if _, exist := kv.validShards[migrationData.Shard]; !exist {
		kv.validShards[migrationData.Shard] = true
		for k, v := range migrationData.DB {
			kv.db[k] = v
		}
		for k, v := range migrationData.Cid2Seq {
			kv.cid2Seq[k] = Max(v, kv.cid2Seq[k])
		}
		if _, ok := kv.garbage[migrationData.ConfigNum]; !ok {
			kv.garbage[migrationData.ConfigNum] = make(map[int]bool)
		}
		kv.garbage[migrationData.ConfigNum][migrationData.Shard] = true
	}
}

func (kv *ShardKV) updateNewConfigShards(newConfig shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if newConfig.Num <= kv.currentConfig.Num { //only consider newer config
		return
	}
	//fmt.Printf("%v: updateNewConfigShards begin!!! gid=%v newConfigNum=%v kv.cfg.num=%v validShards=%v newConfigShards=%v\n",
	//	kv.me, kv.gid, newConfig.Num, kv.currentConfig.Num, kv.validShards, newConfig.Shards)
	kv.configMap[newConfig.Num] = newConfig
	oldCfg, toOutShards := kv.currentConfig, kv.validShards
	kv.previousConfig = oldCfg
	kv.validShards, kv.currentConfig = make(map[int]bool), newConfig
	if _, exist := kv.configNum2ComeInShards[oldCfg.Num]; exist {
		log.Fatalf("exist!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1\n")
		return
	}
	kv.configNum2ComeInShards[newConfig.Num] = make(map[int]bool)

	// update comeInShards
	for shard, gid := range newConfig.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := toOutShards[shard]; ok || oldCfg.Num == 0 {
			kv.validShards[shard] = true
			delete(toOutShards, shard)
		} else {
			kv.configNum2ComeInShards[newConfig.Num][shard] = true
		}
	}
	if len(kv.configNum2ComeInShards[newConfig.Num]) == 0 {
		delete(kv.configNum2ComeInShards, newConfig.Num)
	}

	if len(toOutShards) > 0 { // prepare data that needed migration
		kv.configNum2migrateOutShardsDB[newConfig.Num] = make(map[int]map[string]string)
		for shard := range toOutShards {
			outDB := make(map[string]string)
			for k, v := range kv.db {
				if key2shard(k) == shard {
					outDB[k] = v
					delete(kv.db, k)
				}
			}
			kv.configNum2migrateOutShardsDB[newConfig.Num][shard] = outDB
		}
	}
}
