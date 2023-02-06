package shardkv

import (
	"6.824/raft"
)

func (kv *ShardKV) ReadRaftApplyCommandLoop() {
	for !kv.Killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.GetCommandFromRaft(message)
			}
			if message.SnapshotValid {
				kv.GetSnapshotFromRaft(message)
			}
		}
	}
}

func (kv *ShardKV) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= kv.lastIncludeIndex {
		return
	}

	if op.Operation == NEWCONFIGOp {
		kv.ExecuteNewConfigOpOnServer(op)
		if kv.maxraftstate != -1 {
			kv.IfNeedToSendSnapshotCommand(message.CommandIndex, 9)
		}
		return
	}
	if op.Operation == MIGRATESHARDOp {
		kv.ExecuteMigrateShardsOnServer(op)
		if kv.maxraftstate != -1 {
			kv.IfNeedToSendSnapshotCommand(message.CommandIndex, 9)
		}
		kv.SendMessageToWaitChannel(op, message.CommandIndex)
		return
	}

	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId, key2shard(op.Key)) {
		if op.Operation == PUTOp {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == APPENDOp {
			kv.ExecuteAppendOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.IfNeedToSendSnapshotCommand(message.CommandIndex, 9)
	}

	kv.SendMessageToWaitChannel(op, message.CommandIndex)
}

func (kv *ShardKV) SendMessageToWaitChannel(op Op, raftIndex int) {
	kv.mu.Lock()
	ch, exist := kv.waitApplyChannel[raftIndex]
	kv.mu.Unlock()

	if exist {
		ch <- op
	}
}

func (kv *ShardKV) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	shardNum := key2shard(op.Key)
	kv.mu.Lock()
	value, exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	kv.mu.Unlock()

	return value, exist
}

func (kv *ShardKV) ExecutePutOpOnKVDB(op Op) {
	shardNum := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
}

func (kv *ShardKV) ExecuteAppendOnKVDB(op Op) {
	shardNum := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.kvDB[shardNum].KVDBOfShard[op.Key]
	if exist {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = value + op.Value
	} else {
		kv.kvDB[shardNum].KVDBOfShard[op.Key] = op.Value
	}
	kv.kvDB[shardNum].ClientRequestId[op.ClientId] = op.RequestId
}

func (kv *ShardKV) ExecuteNewConfigOpOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newConfig := op.Config_NEWCONFIG
	if newConfig.Num != kv.config.Num+1 {
		return
	}
	// all migrate shard should be finished
	for shard := 0; shard < NShards; shard++ {
		if kv.migratingShard[shard] {
			return
		}
	}

	kv.lockMigratingShardsL(newConfig.Shards)
	kv.config = newConfig
}

func (kv *ShardKV) lockMigratingShardsL(newShards [NShards]int) {
	oldShards := kv.config.Shards
	for shard := 0; shard < NShards; shard++ {
		// old Shards not ever belong to myself
		if oldShards[shard] == kv.gid && newShards[shard] != kv.gid {
			if newShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
		// new Shards owm to myself
		if oldShards[shard] != kv.gid && newShards[shard] == kv.gid {
			if oldShards[shard] != 0 {
				kv.migratingShard[shard] = true
			}
		}
	}
}

func (kv *ShardKV) ExecuteMigrateShardsOnServer(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	myConfig := kv.config
	if op.ConfigNum_MIGRATE != myConfig.Num {
		return
	}
	for _, shardComponent := range op.MigrateData_MIGRATE {
		if !kv.migratingShard[shardComponent.ShardIndex] {
			continue
		}
		kv.migratingShard[shardComponent.ShardIndex] = false
		kv.kvDB[shardComponent.ShardIndex] = ShardComponent{
			shardComponent.ShardIndex,
			make(map[string]string),
			make(map[int64]int),
		}

		if myConfig.Shards[shardComponent.ShardIndex] == kv.gid {
			CloneShardComponent(
				&kv.kvDB[shardComponent.ShardIndex],
				shardComponent)
		}
	}
}

func CloneShardComponent(component *ShardComponent, sourceComponent ShardComponent) {
	for key, value := range sourceComponent.KVDBOfShard {
		component.KVDBOfShard[key] = value
	}
	for clientId, requestId := range sourceComponent.ClientRequestId {
		component.ClientRequestId[clientId] = requestId
	}
}

func (kv *ShardKV) ifRequestDuplicate(newClientId int64, newRequestId int, shardNum int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastRequestId, ifClientInRecord := kv.kvDB[shardNum].ClientRequestId[newClientId]
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}
