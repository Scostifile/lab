package shardkv3_2

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GeneralPollFrequency  = 5 * time.Millisecond   // 轮询时间
	TimeoutThreshold      = 200 * time.Millisecond // 超时阈值
	SnapshotPollFrequency = 100 * time.Millisecond // 快照轮询时间
	ConfigListenFrequency = 100 * time.Millisecond // 查询当前配置频率
	GeneralInterval       = 100 * time.Millisecond // 通用间隔
)

type ShardStatus int

//const ( // 分区状态
//	IrresponsibleAndNotOwn ShardStatus = iota // belongToOtherGroup
//	IrresponsibleButOwn                       // pulling
//	ResponsibleAndOwn                         // serving
//	ResponsibleButNotOwn                      // bePulling
//)

const ( // 分区状态
	BelongToOtherGroup ShardStatus = iota
	Pulling
	Serving
	BePulling
)

type AddShardCmd struct {
	ConfigNum   int                 // 配置号
	Shard       int                 // 分区号
	KVData      map[string]string   // 分区数据
	ClientCache map[int64]ReqResult // 客户端最新结果
}

type DeleteShardCmd struct {
	ConfigNum int   // 配置号
	Shard     []int // 分区号
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	done              chan struct{}
	persister         *raft.Persister
	mck               *shardctrler.Clerk                     // 配置客户端
	lastConfig        shardctrler.Config                     // 上次的配置
	config            shardctrler.Config                     // 当前配置
	kvDatas           [shardctrler.NShards]map[string]string // 各分区kv数据
	clientCache       map[int64]ReqResult                    // 存放各个客户端最新结果
	commitIndex       int                                    // server当前commit index
	lastSnapshotIndex int                                    // 上次快照的commit index
	shardStatus       [shardctrler.NShards]ShardStatus       // 各分区状态
	allReplicaNode    map[int][]string                       // 见到的所有节点
}

func copyMapKV(data map[string]string) map[string]string { // 复制kv数据
	newData := make(map[string]string)
	for key, value := range data {
		newData[key] = value
	}
	return newData
}

func copyMapCache(data map[int64]ReqResult) map[int64]ReqResult { // 复制客户端最新结果
	newData := make(map[int64]ReqResult)
	for id, res := range data {
		newData[id] = res
	}
	return newData
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	exitFunc := true
	var index int
	var isLeader bool
	kv.mu.Lock()
	switch kv.shardStatus[shard] {
	case BelongToOtherGroup: // 错误的组
		reply.Status = ErrWrongGroup
	case BePulling: // 错误的组
		reply.Status = ErrWrongGroup
	case Pulling: // 等待分区数据到达
		reply.Status = ErrWaitShardData
	case Serving:
		index, _, isLeader = kv.rf.Start(*args) // 开始协商用户请求
		if !isLeader {
			reply.Status = ErrWrongLeader
		} else {
			exitFunc = false
		}
	}
	kv.mu.Unlock()
	if exitFunc {
		return
	}

	requestBegin := time.Now()
	for {
		kv.mu.Lock()
		outCycle := false
		cache := kv.clientCache[args.ClientId]
		if cache.SequenceNum < args.SequenceNum { // 仍未执行相应请求
			if kv.commitIndex >= index { // 相应位置出现了其他的命令
				reply.Status = ErrWrongLeader
				outCycle = true
			} else if time.Since(requestBegin) > TimeoutThreshold { // 等待超时
				reply.Status = ErrTimeout
				outCycle = true
			}
		} else if cache.SequenceNum == args.SequenceNum { // 请求执行成功
			reply.Status = cache.Status
			reply.Value = cache.Value
			outCycle = true
		} else if cache.SequenceNum > args.SequenceNum {
			reply.Status = ErrOldRequest
			outCycle = true
		}
		kv.mu.Unlock()
		if outCycle {
			return
		}
		time.Sleep(GeneralPollFrequency)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	exitFunc := true
	var index int
	var isLeader bool
	kv.mu.Lock()
	switch kv.shardStatus[shard] {
	case BelongToOtherGroup: // 错误的组
		reply.Status = ErrWrongGroup
	case BePulling: // 错误的组
		reply.Status = ErrWrongGroup
	case Pulling: // 等待分区数据到达
		reply.Status = ErrWaitShardData
	case Serving:
		index, _, isLeader = kv.rf.Start(*args) // 开始协商用户请求
		if !isLeader {
			reply.Status = ErrWrongLeader
		} else {
			exitFunc = false
		}
	}
	kv.mu.Unlock()
	if exitFunc {
		return
	}

	requestBegin := time.Now()
	for {
		kv.mu.Lock()
		outCycle := false
		cache := kv.clientCache[args.ClientId]
		if cache.SequenceNum < args.SequenceNum { // 仍未执行相应请求
			if kv.commitIndex >= index { // 相应位置出现了其他的命令
				reply.Status = ErrWrongLeader
				outCycle = true
			} else if time.Since(requestBegin) > TimeoutThreshold { // 等待超时
				reply.Status = ErrTimeout
				outCycle = true
			}
		} else if cache.SequenceNum == args.SequenceNum { // 请求执行成功
			reply.Status = cache.Status
			outCycle = true
		} else if cache.SequenceNum > args.SequenceNum {
			reply.Status = ErrOldRequest
			outCycle = true
		}
		kv.mu.Unlock()
		if outCycle {
			return
		}
		time.Sleep(GeneralPollFrequency)
	}
}

// pull
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Status = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 与预期的分区数据请求者信息一致，将数据移交给该节点
	if kv.config.Num == args.ConfigNum &&
		kv.config.Shards[args.Shard] == args.GID &&
		kv.shardStatus[args.Shard] == BePulling {
		DPrintf("%d shard migrate from %d-%d to %d success. config num is %d\n", args.Shard, kv.gid, kv.me, args.GID, kv.config.Num)
		reply.KVData = copyMapKV(kv.kvDatas[args.Shard])
		reply.ClintCache = copyMapCache(kv.clientCache)
		reply.Status = OK
		return
	}
	DPrintf("%d shard migrate from %d-%d to %d failed. config num is %d  arg config num is %d  shard status is %v\n", args.Shard, kv.gid, kv.me, args.GID, kv.config.Num, args.ConfigNum, kv.shardStatus)
	reply.Status = ErrShardStatus
	return
}

func (kv *ShardKV) Delete(args *DeleteArgs, reply *DeleteReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()

	if myConfigNum <= args.ConfigNum { // 请求者配置号比当前配置号更新或一致
		needToDeleteShards := []int{}
		for _, shard := range args.MigrateSuccessShard {
			kv.mu.Lock()
			if kv.shardStatus[shard] == BePulling { // 表示该数据已成功转移至其他节点
				DPrintf("%d-%d send delete %d shard cmd. config num is %d", kv.gid, kv.me, shard, kv.config.Num)
				needToDeleteShards = append(needToDeleteShards, shard)
			}
			kv.mu.Unlock()
		}
		if len(needToDeleteShards) == 0 {
			return
		}
		kv.rf.Start(DeleteShardCmd{ConfigNum: myConfigNum, Shard: needToDeleteShards}) // 删除分区数据命令
	}
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.done)
}

func (kv *ShardKV) completeMigrate() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, status := range kv.shardStatus {
		if status == Pulling || status == BePulling {
			return false
		}
	}
	return true
}
func (kv *ShardKV) configListenTask() { // 配置监听线程
	for {
		select {
		case <-kv.done:
			return
		default:
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			lastConfigNum := kv.config.Num
			kv.mu.Unlock()
			if isLeader && kv.completeMigrate() {
				newConfig := kv.mck.Query(lastConfigNum + 1) // 只查看下一任配置
				if newConfig.Num == lastConfigNum+1 {
					DPrintf("%d %d add new config cmd %v ", kv.gid, kv.me, newConfig)
					kv.rf.Start(newConfig) // 最多更新至下一任配置
				}
			}

			time.Sleep(GeneralInterval)
		}
	}
}

func (kv *ShardKV) requestShardTask() { // 请求分区线程
	for {
		select {
		case <-kv.done:
			return
		default:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				for shard, status := range kv.shardStatus {
					if status == Pulling {
						go func(shard int, lastConfig shardctrler.Config) {
							args := MigrateArgs{ConfigNum: lastConfig.Num + 1, GID: kv.gid, Shard: shard}
							oldOwner := lastConfig.Shards[shard] // 向上一任拥有者请求分区数据
							for _, server := range lastConfig.Groups[oldOwner] {
								reply := MigrateReply{}
								ok := kv.make_end(server).Call("ShardKV.Migrate", &args, &reply)
								if ok && reply.Status == OK {
									DPrintf("%d %d send add %d shard cmd.", kv.gid, kv.me, shard)
									// 增加分区数据命令
									kv.rf.Start(AddShardCmd{ConfigNum: lastConfig.Num + 1, Shard: shard, KVData: reply.KVData, ClientCache: reply.ClintCache})
									return
								}
							}
						}(shard, kv.lastConfig)
					}
				}
				kv.mu.Unlock()
			}
			time.Sleep(GeneralInterval)
		}
	}
}
func (kv *ShardKV) notifyDeleteTask() { // 提醒其他分组删除相应分区
	for {
		select {
		case <-kv.done:
			return
		default:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				ownShards := []int{}
				for shard, status := range kv.shardStatus {
					if status == Serving || status == BePulling { // 当前拥有这些分区数据
						ownShards = append(ownShards, shard)
					}
				}
				args := DeleteArgs{ConfigNum: kv.config.Num, MigrateSuccessShard: ownShards}
				for gid, servers := range kv.allReplicaNode {
					if gid != kv.gid {
						for _, server := range servers {
							reply := DeleteReply{}
							go kv.make_end(server).Call("ShardKV.Delete", &args, &reply)
						}
					}
				}
				kv.mu.Unlock()
			}

		}
		time.Sleep(GeneralInterval)
	}
}

// 接受raft消息
func (kv *ShardKV) receiveTask() {
	for {
		select {
		case <-kv.done:
			return
		case msg := <-kv.applyCh:
			{
				kv.mu.Lock()
				if msg.CommandValid { // 普通的日志消息
					kv.commitIndex = msg.CommandIndex
					switch data := msg.Command.(type) {
					case GetArgs:
						shard := key2shard(data.Key)
						cache := kv.clientCache[data.ClientId] // 查看相应客户端缓存
						if cache.SequenceNum < data.SequenceNum && kv.shardStatus[shard] == Serving {
							value, exist := kv.kvDatas[shard][data.Key]
							if !exist {
								kv.clientCache[data.ClientId] = ReqResult{SequenceNum: data.SequenceNum, Status: ErrNoKey}
							} else {
								kv.clientCache[data.ClientId] = ReqResult{SequenceNum: data.SequenceNum, Status: OK, Value: value}
							}

						}
					case PutAppendArgs:
						shard := key2shard(data.Key)
						cache := kv.clientCache[data.ClientId] // 查看相应客户端缓存
						if cache.SequenceNum < data.SequenceNum && kv.shardStatus[shard] == Serving {
							if data.Op == "Put" {
								kv.kvDatas[shard][data.Key] = data.Value
								kv.clientCache[data.ClientId] = ReqResult{SequenceNum: data.SequenceNum, Status: OK}
							} else {
								kv.kvDatas[shard][data.Key] += data.Value
								kv.clientCache[data.ClientId] = ReqResult{SequenceNum: data.SequenceNum, Status: OK}
							}
						}
					case shardctrler.Config:
						if kv.config.Num < data.Num {
							DPrintf("%d %d update config.old config:%v new config:%v", kv.gid, kv.me, kv.config, data)
							for i := 0; i < shardctrler.NShards; i++ {
								oldOwner := kv.config.Shards[i]
								newOwner := data.Shards[i]
								if newOwner == kv.gid && oldOwner != kv.gid {
									if oldOwner == 0 { // 第一任拥有者
										kv.shardStatus[i] = Serving
										kv.kvDatas[i] = make(map[string]string) // 初始化map
									} else { // 等待上一任拥有者的分区数据
										kv.shardStatus[i] = Pulling
									}
								}
								if newOwner != kv.gid && oldOwner == kv.gid {
									if kv.shardStatus[i] == Serving {
										kv.shardStatus[i] = BePulling
									} else { // 不可能发生
										kv.shardStatus[i] = BelongToOtherGroup
									}
								}
							}
							kv.lastConfig = kv.config
							kv.config = data
							for gid, servers := range data.Groups { // 更新见到的所有节点
								kv.allReplicaNode[gid] = servers
							}
						}
					case AddShardCmd:
						if kv.config.Num == data.ConfigNum && kv.shardStatus[data.Shard] == Pulling { // 预期的分区数据到达
							DPrintf("%d %d add new %d shard data. config num is %d", kv.gid, kv.me, data.Shard, kv.config.Num)
							kv.kvDatas[data.Shard] = copyMapKV(data.KVData)
							kv.shardStatus[data.Shard] = Serving
							for id, res := range data.ClientCache { // 更新最新客户端结果
								if kv.clientCache[id].SequenceNum < res.SequenceNum {
									kv.clientCache[id] = res
								}
							}
						}
					case DeleteShardCmd:
						DPrintf("before:%d %d delete %d shard data. config num is %d  shard status:%v", kv.gid, kv.me, data.Shard, kv.config.Num, kv.shardStatus)
						for _, shard := range data.Shard {
							if kv.config.Num == data.ConfigNum && kv.shardStatus[shard] == BePulling {
								kv.shardStatus[shard] = BelongToOtherGroup
								kv.kvDatas[shard] = nil
								DPrintf("after: %d %d delete %d shard data. config num is %d  shard status:%v", kv.gid, kv.me, data.Shard, kv.config.Num, kv.shardStatus)
							}
						}
					}

				} else if msg.SnapshotValid { // 快照消息
					r := bytes.NewBuffer(msg.Snapshot)
					d := labgob.NewDecoder(r)
					var commitIndex int
					var clientCache map[int64]ReqResult
					var kvDatas [shardctrler.NShards]map[string]string
					var config shardctrler.Config
					var shardStatus [shardctrler.NShards]ShardStatus
					var allNode map[int][]string
					if d.Decode(&commitIndex) != nil || d.Decode(&clientCache) != nil || d.Decode(&kvDatas) != nil || d.Decode(&config) != nil || d.Decode(&shardStatus) != nil || d.Decode(&allNode) != nil {
						log.Panicf("snapshot decode error")
					} else {
						DPrintf("%d %d recv snapshot. shard status:%v", kv.gid, kv.me, shardStatus)
						kv.commitIndex = commitIndex
						kv.clientCache = clientCache
						kv.kvDatas = kvDatas
						kv.config = config
						kv.lastConfig = kv.mck.Query(config.Num - 1)
						kv.shardStatus = shardStatus
						kv.allReplicaNode = allNode
						// 更新lastSnapshotIndex
						kv.lastSnapshotIndex = commitIndex
					}
				}
				// 检查raft state大小，生成快照
				if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.commitIndex > kv.lastSnapshotIndex {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.commitIndex)
					e.Encode(kv.clientCache)
					e.Encode(kv.kvDatas)
					// 将以下数据编码至快照
					e.Encode(kv.config)
					e.Encode(kv.shardStatus)
					e.Encode(kv.allReplicaNode)
					snapshot := w.Bytes()
					kv.rf.Snapshot(kv.commitIndex, snapshot)
					kv.lastSnapshotIndex = kv.commitIndex
					if kv.gid == 101 {
						fmt.Printf("%v: gid=%v IfNeedToSendSnapshotCommand need to snap in configNum=%v------raftSize=%v lenOfLog=%v\n",
							kv.me, kv.gid, kv.config.Num, kv.persister.RaftStateSize(), kv.rf.LenOfLog())
						fmt.Printf("----kvdb=%v\n", kv.kvDatas)
						fmt.Printf("-----len(kvDB[1])=%v\n", len(kv.kvDatas))
					}
				}
				kv.mu.Unlock()
			}
		}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(AddShardCmd{})
	labgob.Register(DeleteShardCmd{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.done = make(chan struct{})
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config.Num = 0
	kv.config.Groups = map[int][]string{}
	kv.clientCache = make(map[int64]ReqResult)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardStatus[i] = BelongToOtherGroup
	}
	kv.allReplicaNode = make(map[int][]string)
	// 开启一系列线程
	go kv.receiveTask()
	go kv.configListenTask()
	go kv.requestShardTask()
	go kv.notifyDeleteTask()
	return kv
}
