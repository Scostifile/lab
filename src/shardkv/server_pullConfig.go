package shardkv

import (
	"fmt"
	"sync"
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

// 监视器，每隔一定时间由 leader 进行定时任务的执行
func (kv *ShardKV) monitor(do func(), timeout time.Duration) {
	for !kv.Killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			do()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) watchShardAndMigrate() {
	ifNeedSend, sendData := kv.ifHaveSendData()
	if !ifNeedSend {
		return
	}
	// 有分片需要推送给其他 server
	var wg sync.WaitGroup
	for gid, shardComponent := range sendData {
		wg.Add(1)
		go func(otherGid int, shardData []ShardComponent) {
			defer wg.Done()
			// 执行分片迁移
			kv.sendShardComponentToOther(otherGid, shardData)
		}(gid, shardComponent)
	}
	wg.Wait()
}

func (kv *ShardKV) SendShardToOtherGroupLoop() {
	for !kv.Killed() {
		time.Sleep(SENDSHARDS_TIMEOUT * time.Millisecond)
		_, ifLeader := kv.rf.GetState()
		if !ifLeader {
			continue
		}

		noMigrating := true
		kv.mu.Lock()
		if kv.config.Num == 0 {
			fmt.Printf("SendShardToOtherGroupLoop configNum=%v\n", kv.config.Num)
			kv.mu.Unlock()
			continue
		}
		for shard := 0; shard < NShards; shard++ {
			if kv.migratingShard[shard] == BePulling {
				noMigrating = false
				break
			}
		}
		kv.mu.Unlock()

		if noMigrating {
			continue
		}

		ifNeedSend, sendData := kv.ifHaveSendData()
		if !ifNeedSend {
			continue
		}

		kv.sendShardComponent(sendData)
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
	// kv.config already be updated
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendData := make(map[int][]ShardComponent)
	for shardIndex := 0; shardIndex < NShards; shardIndex++ {
		if kv.migratingShard[shardIndex] == BePulling {
			otherOwnerGid := kv.config.Shards[shardIndex]
			if otherOwnerGid == 0 {
				fmt.Printf("nowOwer = 0!!!!!!!!!!!!!!!!!! in configNum=%v\n", kv.config.Num)
			}
			tempComponent := ShardComponent{
				ShardIndex:      shardIndex,
				KVDBOfShard:     make(map[string]string),
				ClientRequestId: make(map[int64]int),
			}
			CloneShardComponent(&tempComponent, kv.kvDB[shardIndex])
			sendData[otherOwnerGid] = append(sendData[otherOwnerGid], tempComponent)
		}
	}
	return sendData
}

func (kv *ShardKV) sendShardComponent(sendData map[int][]ShardComponent) {
	for gid, shardComponents := range sendData {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
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
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		peersEnd := kv.make_end(groupMember)
		migrateReply := MigrateShardReply{}
		ok := peersEnd.Call("ShardKV.MigrateShard", args, &migrateReply)

		if ok && migrateReply.Err == OK {
			if _, isLeader := kv.rf.GetState(); !isLeader {
				return
			}
			kv.mu.Lock()
			myConfigNum := kv.config.Num
			kv.mu.Unlock()
			if myConfigNum != args.ConfigNum || kv.CheckMigrateState(args.MigrateData) {
				//fmt.Printf("%v: migrateProcess myConfigNum=%v arg.ConfigNum=%v\n",
				//	kv.me, myConfigNum, args.ConfigNum)
				return
			} else {
				op := Op{
					Operation:           GARBAGECOLLECTIONOp,
					MigrateData_MIGRATE: args.MigrateData,
					ConfigNum_MIGRATE:   args.ConfigNum,
				}
				kv.rf.Start(op)
				return
			}
		}
	}
}

func (kv *ShardKV) sendShardComponentToOther(gid int, shardComponents []ShardComponent) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
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
	kv.migrateProcess(groupServers, args)
}

// "MigrateShard" RPC Handler
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()

	if args.ConfigNum > myConfigNum {
		reply.Err = ErrConfigNum
		reply.ConfigMum = myConfigNum
		return
	}

	if args.ConfigNum < myConfigNum || kv.CheckMigrateState(args.MigrateData) {
		reply.Err = OK
		return
	}

	op := Op{
		Operation:           MIGRATESHARDOp,
		MigrateData_MIGRATE: args.MigrateData,
		ConfigNum_MIGRATE:   args.ConfigNum,
	}
	raftIndex, _, _ := kv.rf.Start(op)
	channelRaftIndex := kv.getWaitChannel(raftIndex)

	select {
	case <-time.After(time.Millisecond * CONSENSUS_TIMEOUT):
		_, ifLeader := kv.rf.GetState()
		kv.mu.Lock()
		tempConfigNum := kv.config.Num
		kv.mu.Unlock()

		if args.ConfigNum <= tempConfigNum &&
			kv.CheckMigrateState(args.MigrateData) &&
			ifLeader {
			reply.ConfigMum = tempConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-channelRaftIndex:
		kv.mu.Lock()
		currentConfigNum := kv.config.Num
		kv.mu.Unlock()

		if raftCommitOp.ConfigNum_MIGRATE == args.ConfigNum &&
			args.ConfigNum <= currentConfigNum &&
			kv.CheckMigrateState(args.MigrateData) {
			reply.ConfigMum = currentConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyChannel, raftIndex)
	kv.mu.Unlock()
}
