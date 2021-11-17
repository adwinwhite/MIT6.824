package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"
import "sync/atomic"
import (
	log "github.com/sirupsen/logrus"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	prevLeaderId int
	serialNo     int64
	uid          int64
	mu           sync.RWMutex
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
	// Your code here.
	ck.prevLeaderId = 0
	ck.serialNo = 0
	ck.uid = time.Now().UnixNano()



	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := QueryArgs{Header: ClerkHeader{SerialNo: atomic.AddInt64(&ck.serialNo, 1), ClerkId: ck.uid}, Body: QueryArgsBody{Num: num}}
	var reply QueryReply

	// assume previous leader is still in position
	ok := ck.servers[ck.prevLeaderId].Call("ShardCtrler.Query", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			return reply.Config
		case ErrWrongLeader:
		}
	}
	initialSkip := true
	for {
		for i, v := range ck.servers {
			if initialSkip && i == ck.prevLeaderId {
				initialSkip = false
				continue
			}
			ok := v.Call("ShardCtrler.Query", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					ck.prevLeaderId = i
					return reply.Config
				case ErrWrongLeader:
				}
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := JoinArgs{Header: ClerkHeader{SerialNo: atomic.AddInt64(&ck.serialNo, 1), ClerkId: ck.uid}, Body: JoinArgsBody{Servers: servers}}
	var reply JoinReply
	ok := ck.servers[ck.prevLeaderId].Call("ShardCtrler.Join", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			return
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
			ok := v.Call("ShardCtrler.Join", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					ck.prevLeaderId = i
					return
				case ErrWrongLeader:
				}
			} else {
				log.Info(i, " rpc call is not ok")
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := LeaveArgs{Header: ClerkHeader{SerialNo: atomic.AddInt64(&ck.serialNo, 1), ClerkId: ck.uid}, Body: LeaveArgsBody{GIDs: gids}}
	var reply LeaveReply
	ok := ck.servers[ck.prevLeaderId].Call("ShardCtrler.Leave", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			return
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
			ok := v.Call("ShardCtrler.Leave", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					ck.prevLeaderId = i
					return
				case ErrWrongLeader:
				}
			} else {
				log.Info(i, " rpc call is not ok")
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := MoveArgs{Header: ClerkHeader{SerialNo: atomic.AddInt64(&ck.serialNo, 1), ClerkId: ck.uid}, Body: MoveArgsBody{Shard: shard, GID: gid}}
	var reply MoveReply
	ok := ck.servers[ck.prevLeaderId].Call("ShardCtrler.Move", &args, &reply)
	if ok {
		switch reply.Err {
		case OK:
			return
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
			ok := v.Call("ShardCtrler.Move", &args, &reply)
			if ok {
				switch reply.Err {
				case OK:
					ck.prevLeaderId = i
					return
				case ErrWrongLeader:
				}
			} else {
				log.Info(i, " rpc call is not ok")
			}
		}
	}
}
