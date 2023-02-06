package shardkv3_0

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"crypto/rand"
	"math/big"
	"time"
)

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm        *shardctrler.Clerk
	config    shardctrler.Config
	makeEnd   func(string) *labrpc.ClientEnd
	leaderIds map[int]int
	clientId  int64 // generated by nrand(), it would be better to use some distributed ID generation algorithm that guarantees no conflicts
	commandId int64 // (clientId, commandId) defines a operation uniquely
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		makeEnd:   makeEnd,
		leaderIds: make(map[int]int),
		clientId:  Nrand(),
		commandId: 0,
	}
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandRequest{Key: key, Op: GETOp})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: PUTOp})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: APPENDOp})
}

func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		shard := key2shard(request.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			for {
				var response CommandResponse
				ok := ck.makeEnd(servers[newLeaderId]).Call("ShardKV.Command", request, &response)
				if ok && (response.Err == OK || response.Err == ErrNoKey) {
					ck.commandId++
					return response.Value
				} else if ok && response.Err == ErrWrongGroup {
					break
				} else {
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
