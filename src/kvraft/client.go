package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId       int64
	requestId      int
	recentLeaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.recentLeaderId = GetRandomServer(len(ck.servers))
	return ck
}

// -----------------
func GetRandomServer(length int) int {
	return mathrand.Intn(length)
}

// -----------------

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.requestId++
	server := ck.recentLeaderId
	args := GetArgs{
		key,
		ck.clientId,
		ck.requestId,
	}

	for {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			//fmt.Printf("Get: rechose server=%v\n", server)
			server = (server + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrNoKey {
			//fmt.Printf("Get: value=%v", reply.Value)
			return ""
		}
		if reply.Err == OK {
			ck.recentLeaderId = server
			//fmt.Printf("Get: value=%v", reply.Value)
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestId++
	server := ck.recentLeaderId
	args := PutAppendArgs{
		key,
		value,
		op,
		ck.clientId,
		ck.requestId,
	}
	for {
		//if key == "0" {
		//	fmt.Printf("Client:%v putappend key=%v value=%v to server:%v\n", ck.clientId, args.Key, args.Value, server)
		//}

		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			//fmt.Printf("Client rechoose server:%v\n", server)
			continue
		}
		if reply.Err == OK {
			ck.recentLeaderId = server
			//fmt.Printf("Clinet:%v PutAppend to server=%v is finish key=%v, value=%v goroutine=%v\n", ck.clientId, server, key, value, runtime.NumGoroutine())
			return
		}
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
