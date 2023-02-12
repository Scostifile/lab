package shardkv

import (
	"fmt"
	"time"
)

// only process the task by leader
func (kv *ShardKV) leaderMonitor(do func(), timeout time.Duration) {
	for !kv.Killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			do()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) SendShardToOtherGroupLoop() {
	ifNeedSend, sendData := kv.ifHaveSendData()
	if !ifNeedSend {
		return
	}
	kv.sendShardComponent(sendData)
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
			if otherOwnerGid == 0 || kv.gid == otherOwnerGid {
				fmt.Printf("nowOwer = 0!!!!!!!!!!!!!!!!!! in configNum=%v gid=%v ?= otherGid=%v\n", kv.config.Num, kv.gid, otherOwnerGid)
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
		//if _, isLeader := kv.rf.GetState(); !isLeader {
		//	fmt.Printf("%v: gid=%v sendShardComponent is not leader now!!!!\n", kv.me, kv.gid)
		//	return
		//}
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
		//if _, isLeader := kv.rf.GetState(); !isLeader {
		//	fmt.Printf("%v: gid=%v migrateProcess is not leader now!!!!\n", kv.me, kv.gid)
		//	return
		//}
		server := kv.make_end(groupMember)
		migrateReply := MigrateShardReply{}
		ok := server.Call("ShardKV.MigrateShard", args, &migrateReply)
		kv.mu.Lock()
		myConfigNum := kv.config.Num
		kv.mu.Unlock()
		if ok && migrateReply.Err == OK {
			//if _, isLeader := kv.rf.GetState(); !isLeader {
			//	fmt.Printf("%v: gid=%v migrateProcess is not leader now!!!!\n", kv.me, kv.gid)
			//	return
			//}
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
		//_, ifLeaderState := kv.rf.GetState()
		//kv.mu.Lock()
		//tempConfigNum := kv.config.Num
		//kv.mu.Unlock()
		//
		//if args.ConfigNum <= tempConfigNum &&
		//	kv.CheckMigrateState(args.MigrateData) &&
		//	ifLeaderState {
		if kv.CheckMigrateState(args.MigrateData) {
			//reply.ConfigMum = tempConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case raftCommitOp := <-channelRaftIndex:
		//kv.mu.Lock()
		//currentConfigNum := kv.config.Num
		//kv.mu.Unlock()
		//
		//if raftCommitOp.ConfigNum_MIGRATE == args.ConfigNum &&
		//	args.ConfigNum <= currentConfigNum &&
		//	kv.CheckMigrateState(args.MigrateData) {
		if raftCommitOp.ConfigNum_MIGRATE == args.ConfigNum && kv.CheckMigrateState(args.MigrateData) {
			//reply.ConfigMum = currentConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyChannel, raftIndex)
	kv.mu.Unlock()
}
