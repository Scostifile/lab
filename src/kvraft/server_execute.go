package kvraft

import (
	"6.824/raft"
)

func (kv *KVServer) ReadRaftApplyCommandLoop() {
	//for !kv.killed() {
	//	message := <-kv.applyCh
	//	if message.CommandValid {
	//		kv.GetCommandFromRaft(message)
	//	}
	//	if message.SnapshotValid {
	//		kv.GetSnapshotFromRaft(message)
	//	}
	//}

	for !kv.killed() {
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

func (kv *KVServer) GetCommandFromRaft(message raft.ApplyMsg) {
	op := message.Command.(Op)

	if message.CommandIndex <= kv.lastIncludeIndex {
		return
	}

	if !kv.ifRequestDuplicate(op.ClientId, op.RequestId) {
		if op.Operation == "Put" {
			kv.ExecutePutOpOnKVDB(op)
		}
		if op.Operation == "Append" {
			kv.ExecuteAppendOpOnKVDB(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.ifNeedToSendSnapshotCommand(message.CommandIndex, 9)
	}

	kv.SendMessageToWaitChannel(op, message.CommandIndex)
}

func (kv *KVServer) SendMessageToWaitChannel(op Op, raftIndex int) bool {
	kv.mu.Lock()
	//defer kv.mu.Unlock()

	ch, exist := kv.waitApplyCh[raftIndex]
	kv.mu.Unlock()
	if exist {
		//fmt.Printf("KVServer:%v SendMessageToWaitChannel key=%v value=%v\n", kv.me, op.Key, op.Value)
		ch <- op
	}
	return exist
}

func (kv *KVServer) ExecuteGetOpOnKVDB(op Op) (string, bool) {
	kv.mu.Lock()
	value, exist := kv.kvDB[op.Key]
	kv.lastRequestId[op.ClientId] = op.RequestId
	kv.mu.Unlock()

	return value, exist
}

func (kv *KVServer) ExecutePutOpOnKVDB(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvDB[op.Key] = op.Value
	//fmt.Printf("KVServer:%v ExecutePutOpOnKVDB data{key=%v value=%v} will be wrietten in DB\n", kv.me, op.Key, op.Value)
	kv.lastRequestId[op.ClientId] = op.RequestId
}

func (kv *KVServer) ExecuteAppendOpOnKVDB(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//if op.Key == "0" {
	//	fmt.Printf("KVServer:%v ExecuteAppendOpOnKVDB data{key=%v value=%v} will be wrietten in DB:%v\n", kv.me, op.Key, op.Value, kv.kvDB["0"])
	//}
	value, exist := kv.kvDB[op.Key]
	if exist {
		kv.kvDB[op.Key] = value + op.Value
	} else {
		kv.kvDB[op.Key] = op.Value
	}
	//if op.Key == "0" {
	//	fmt.Printf("KVServer:%v ExecuteAppendOpOnKVDB data{key=%v value=%v} has been wrietten in DB:%v\n", kv.me, op.Key, op.Value, kv.kvDB["0"])
	//}

	kv.lastRequestId[op.ClientId] = op.RequestId
}

func (kv *KVServer) ifRequestDuplicate(newClientId int64, newRequestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastRequestId, ifClientInRecord := kv.lastRequestId[newClientId]
	if !ifClientInRecord {
		return false
	}
	return newRequestId <= lastRequestId
}
