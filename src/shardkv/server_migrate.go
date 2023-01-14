package shardkv

import "time"

func (kv *ShardKV) PullNewConfigLoop() {
	for !kv.Killed() {
		_, ifLeader := kv.rf.GetState()
		if !ifLeader {
			time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
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

		time.Sleep(CONFIGCHECK_TIMEOUT * time.Millisecond)
	}
}

func (kv *ShardKV) SendShardToOtherGroupLoop() {
	for !kv.Killed() {
		_, ifLeader := kv.rf.GetState()
		if !ifLeader {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		noMigrating := true
		kv.mu.Lock()
		for shard := 0; shard < NShards; shard++ {
			if kv.migratingShard[shard] {
				noMigrating = false
				break
			}
		}
		kv.mu.Unlock()

		if noMigrating {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		ifNeedSend, sendData := kv.ifHaveSendData()
		if !ifNeedSend {
			time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
			continue
		}

		kv.sendShardComponent(sendData)
		time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
	}
}

func (kv *ShardKV) ifHaveSendData() (bool, map[int][]ShardComponent) {
	sendData := kv.MakeSendShardComponent()
	if len(sendData) == 0 {
		return false, make(map[int][]ShardComponent)
	}
	return true, sendData
}

func (kv *ShardKV) MakeSendShardComponent() map[int][]ShardComponent {
	// kv.config already be update
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendData := make(map[int][]ShardComponent)
	for shard := 0; shard < NShards; shard++ {
		nowOwner := kv.config.Shards[shard]
		if kv.migratingShard[shard] && kv.gid != nowOwner {
			tempComponent := ShardComponent{
				ShardIndex:      shard,
				KVDBOfShard:     make(map[string]string),
				ClientRequestId: make(map[int64]int),
			}
			CloneShardComponent(&tempComponent, kv.kvDB[shard])
			sendData[nowOwner] = append(sendData[nowOwner], tempComponent)
		}
	}
	return sendData
}

func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
	for gid, shardComponents := range sendData {
		kv.mu.Lock()
		args := &MigrateShardArgs{
			ConfigNum:   kv.config.Num,
			MigrateData: make([]ShardComponent, 0),
		}
		groupServers := kv.config.Groups[gid]
		kv.mu.Unlock()

		for _, component := range shardComponents {
			tempComponent := ShardComponent{
				ShardIndex:      component.ShardIndex,
				KVDBOfShard:     make(map[string]string),
				ClientRequestId: make(map[int64]int),
			}
			CloneShardComponent(&tempComponent, component)
			args.MigrateData = append(args.MigrateData, tempComponent)
		}

		go kv.migrateProcess(groupServers, args)
	}
}

func (kv *ShardKV) migrateProcess(groupServers []string, args *MigrateShardArgs) {
	for _, groupMember := range groupServers {
		peersEnd := kv.make_end(groupMember)
		migrateReply := MigrateShardReply{}
		ok := peersEnd.Call("ShardKV.MigrateShard", args, &migrateReply)
		kv.mu.Lock()
		myConfigNum := kv.config.Num
		kv.mu.Unlock()
		if ok && migrateReply.Err == OK {
			if myConfigNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData) {
				return
			} else {
				op := Op{
					Operation:           MIGRATESHARDOp,
					MigrateData_MIGRATE: args.MigrateData,
					ConfigNum_MIGRATE:   args.ConfigNum,
				}
				kv.rf.Start(op)
				return
			}
		}
	}
}
