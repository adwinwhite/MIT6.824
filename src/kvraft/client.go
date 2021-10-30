package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	prevLeaderId int
	// You will have to modify this struct.
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
	ck.prevLeaderId = 0
	// You'll have to add code here.
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
	args := GetArgs{Key: key}
	var reply GetReply

	// assume previous leader is still in position
	ok := ck.servers[ck.prevLeaderId].Call("KVServer.Get", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
		}
	}
	for i, v := range ck.servers {
		if i == ck.prevLeaderId {
			continue
		}
		ok := v.Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.prevLeaderId = i
				return reply.Value
			case ErrNoKey:
				ck.prevLeaderId = i
				return ""
			case ErrWrongLeader:
			}
		}
	}


	// You will have to modify this function.
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
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	var reply PutAppendReply
	ok := ck.servers[ck.prevLeaderId].Call("KVServer.PutAppend", &args, &reply)
	if ok && reply.Err == OK {
		return 
	}

	initialSkip := true
	for {
		for i, v := range ck.servers {
			if initialSkip && i == ck.prevLeaderId {
				initialSkip = false
				continue
			}
			ok := v.Call("KVServer.PutAppend", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					ck.prevLeaderId = i
					return
				case ErrWrongLeader:
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
