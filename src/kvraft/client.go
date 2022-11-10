package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import (
	mathrand "math/rand"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//leaderId其实是为了下次能够直接发给正确leader
	leaderId	int
	//如果这个leader在commit log后crash了，但是还没响应给client，client就会重发这条command给新的leader，这样就会导致这个op执行两次。
	//而这种解决办法就是每次发送操作时附加一个唯一的序列号去为了标识操作,避免op被执行两次
	sequenceNum	int
	clientId	int64
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
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

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
	var args GetArgs 
	args = GetArgs{
		Key : key ,
		SequenceNum : ck.sequenceNum ,
		ClientId : ck.clientId	,
	}
	var reply GetReply
	serverId := ck.leaderId
	for{
		reply = GetReply{}
		ck.sequenceNum++
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		serverId = (serverId + 1) % len(ck.servers)
		if !ok {
			continue
		}
		if reply.Err == ErrNoKey{
			ck.leaderId = serverId
			return ""
		}else if reply.Err == ErrWrongLeader{
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}else if reply.Err == OK{
			ck.leaderId = serverId
			return reply.Value
		}
	}
	// You will have to modify this function.
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
	ck.sequenceNum++
	serverId := ck.leaderId
	args := PutAppendArgs{
		Key: key, 
		Value: value, 
		Op: op, 
		ClientId: ck.clientId, 
		SequenceNum: ck.sequenceNum,
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		serverId = (serverId + 1) % len(ck.servers)
		if !ok {
			continue
		}
		if reply.Err == OK {
			ck.leaderId = serverId
			return
		} else if reply.Err == ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}	
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
