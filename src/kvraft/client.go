package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	clientId int64
	seq      int
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
	ck.seq = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.seq++
	args := GetArgs{key, ck.clientId, ck.seq}
	reply := GetReply{"", ""}
	k := ck.leaderId
	for {
		k = k % len(ck.servers)
		//call
		DPrintf("CLNT", "Get call begin Key:%v at id:%d,seq:%d", args.Key, args.ClientId%10, args.Seq)
		ok := ck.servers[k].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == E_noLeader || reply.Err == E_seqNoLinear {
				DPrintf("SVRO", "Get failed no LEADER id:%d seq:%d", ck.clientId%10, args.Seq)
				k++
			} else if reply.Err == E_noError {
				ck.leaderId = k
				DPrintf("CLNT", "Get call success id:%d seq:%d key:%v val:%v", ck.clientId%10, args.Seq, key, reply.Value)
				break
			} else {
				DPrintf("SVRO", "Get other error id:%d  seq:%d", ck.clientId%10, args.Seq)
			}
		} else {
			DPrintf("SVRO", "Get timeout id:%d seq:%d", ck.clientId%10, args.Seq)
			k++
		}
		if k%len(ck.servers) == ck.leaderId {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq++
	args := PutAppendArgs{key, value, op, ck.clientId, ck.seq}
	reply := PutAppendReply{""}
	k := ck.leaderId
	for {
		k = k % len(ck.servers)
		//call
		DPrintf("CLNT", "PutAppend call begin Key:%v Val:%v at id:%d seq:%d", args.Key, args.Value, args.ClientId%10, args.Seq)
		ok := ck.servers[k].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == E_noLeader || reply.Err == E_seqNoLinear {
				DPrintf("SVRO", "PutAppend failed no LEADER id:%d  seq:%d", ck.clientId%10, args.Seq)
				k++
			} else if reply.Err == E_noError {
				DPrintf("CLNT", "PutAppend call success id:%d  seq:%d", ck.clientId%10, args.Seq)
				ck.leaderId = k
				break
			} else {
				DPrintf("SVRO", "PutAppend other error id:%d  seq:%d", ck.clientId%10, args.Seq)
			}
		} else {
			DPrintf("SVRO", "PutAppend timeout id:%d seq:%d", ck.clientId%10, args.Seq)
			k++
		}
		if k%len(ck.servers) == ck.leaderId {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
