package shardkv3_2

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                        = "OK"
	ErrNoKey                  = "ErrNoKey"
	ErrWrongGroup             = "ErrWrongGroup"
	ErrWrongLeader            = "ErrWrongLeader"
	ErrTimeout                = "ErrTimeout"
	ErrWaitShardData          = "ErrWaitShardData "         // 等待分区数据
	ErrWaitTargetUpdateConfig = "ErrWaitTargetUpdateConfig" // 等待对方更新配置
	ErrWaitDelete             = "ErrWaitDelete"
	ErrShardStatus            = "ErrShardStatus"
	ErrUnexpectedNode         = "ErrUnexpectedNode"
	ErrOldRequest             = "ErrOldRequest" // 未出现
	ErrLogFull                = "ErrLogFull"    // 未用到

)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// 附带client id与序列号
	ClientId    int64
	SequenceNum int64
}

type PutAppendReply struct {
	Status Err
}

type GetArgs struct {
	Key string
	// 附带client id与序列号
	ClientId    int64
	SequenceNum int64
}

type GetReply struct {
	Status Err
	Value  string
}

type ReqResult struct { // 命令执行返回
	SequenceNum int64  // 命令序列号
	Status      Err    // 命令执行状态
	Value       string // 命令执行结果
}

type MigrateArgs struct { // 分区数据迁移参数
	ConfigNum int // 请求者配置号
	GID       int // 请求者ID
	Shard     int // 请求分区号
}

type MigrateReply struct { // 分区数据迁移返回
	Status     Err                 // 返回状态
	KVData     map[string]string   // 分区数据
	ClintCache map[int64]ReqResult // 客户端最新结果
}

type DeleteArgs struct { // 删除分区参数
	ConfigNum           int   // 请求者配置号
	MigrateSuccessShard []int // 请求者拥有的分区号
}

type DeleteReply struct { // 删除分区返回
}
