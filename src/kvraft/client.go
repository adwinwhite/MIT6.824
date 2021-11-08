package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import log "github.com/sirupsen/logrus"
import "sync/atomic"
import "strconv"
import "time"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	prevLeaderId int
	// You will have to modify this struct.
	serialNo     int64
	uid          string 
	mu           sync.Mutex
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
	ck.serialNo = 0
	ck.uid = strconv.Itoa((int)(time.Now().UnixNano()))
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{Key: key, SerialNo: strconv.Itoa((int)(atomic.AddInt64(&ck.serialNo, 1))), ClerkId: ck.uid}
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
		case ErrTimeout:
		}
	}
	initialSkip := true
	for {
		for i, v := range ck.servers {
			if initialSkip && i == ck.prevLeaderId {
				initialSkip = false
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
				case ErrTimeout:
				}
			}
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{Key: key, Value: value, Op: op, SerialNo: strconv.Itoa((int)(atomic.AddInt64(&ck.serialNo, 1))), ClerkId: ck.uid}
	log.Info("Clerk putappends: ", args)
	var reply PutAppendReply
	ok := ck.servers[ck.prevLeaderId].Call("KVServer.PutAppend", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			return
		case ErrTimeout:
		}
	} else {
		log.Info(ck.prevLeaderId, " rpc call is not ok")
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
				case ErrTimeout:
				}
			} else {
				log.Info(i, " rpc call is not ok")
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
