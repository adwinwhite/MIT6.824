package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	log "github.com/sirupsen/logrus"
	"sync"
	"sync/atomic"
	// "bytes"
	"fmt"
	"encoding/json"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type BroadcastChan struct {
	indexCh chan int64
	exitCh chan struct{}
}

func removeBroadcastCh(s []BroadcastChan, i int) []BroadcastChan {
    s[i] = s[len(s)-1]
    return s[:len(s)-1]
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applyListeners []BroadcastChan
	applyListenersMutex sync.RWMutex
	data    map[string]string
}

func (kv *KVServer) get(key string) (value string, err Err) {
	value, ok := kv.data[key]
	if ok {
		err = OK
	} else {
		err = ErrNoKey
	}
	return value, err
}

func (kv *KVServer) putAppend(key string, value string, op string) {
	log.WithFields(log.Fields{
		"key": key,
		"value": value,
		"op": op,
	}).Info(kv.me, " putAppend")
	switch op {
	case "Put":
		kv.data[key] = value
	case "Append":
		val, ok := kv.data[key]
		if ok {
			kv.data[key] = val + value
		} else {
			kv.data[key] = value
		}
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Check whether I am leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	log.Info(kv.me, " Get ", fmt.Sprintf("%+v", args))

	// Submit operation
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode("Get")
	// e.Encode(args.Key)
	// data := w.Bytes()
	data, _ := json.Marshal(args)
	index, _, isLeader := kv.rf.Start(data)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	var ls BroadcastChan
	ls.indexCh = make(chan int64)
	ls.exitCh = make(chan struct{})
	kv.applyListenersMutex.Lock()
	kv.applyListeners = append(kv.applyListeners, ls)
	log.Info(kv.me, " append a listener for Get")
	kv.applyListenersMutex.Unlock()
	for {
		log.Info(kv.me, " before receiving index from Ch")
		receivedIndex := <- ls.indexCh
		log.Info("I received index ", receivedIndex)
		if receivedIndex >= (int64)(index) {
			reply.Value, reply.Err = kv.get(args.Key)
			close(ls.exitCh)
			return
		}
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Check whether I am leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	log.Info(kv.me, " PutAppend ", fmt.Sprintf("%+v", args))

	// Submit operation
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode("PutAppend")
	// e.Encode(args.Key)
	// e.Encode(args.Value)
	// e.Encode(args.Op)
	// data := w.Bytes()
	data, _ := json.Marshal(args)

	index, _, isLeader := kv.rf.Start(data)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	var ls BroadcastChan
	ls.indexCh = make(chan int64)
	ls.exitCh = make(chan struct{})
	kv.applyListenersMutex.Lock()
	kv.applyListeners = append(kv.applyListeners, ls)
	log.Info(kv.me, " append a listener for PutAppend")
	kv.applyListenersMutex.Unlock()
	for {
		log.Info(kv.me, " before receiving index from Ch")
		receivedIndex := <- ls.indexCh
		log.Info(kv.me, " I received index ", receivedIndex)
		if receivedIndex >= (int64)(index) {
			close(ls.exitCh)
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		var args map[string]string
		if err := json.Unmarshal(msg.Command.([]byte), &args); err != nil {
			panic("failed to decode msg operation type")
		}
		log.Info(kv.me, args)

		_, ok := args["Value"]
		if ok {
			kv.putAppend(args["Key"], args["Value"], args["Op"])
		} 



		// r := bytes.NewBuffer(msg.Command.([]byte))
		// d := labgob.NewDecoder(r)
		// var opea string
		// if d.Decode(&opea) != nil {
			// panic("failed to decode msg operation type")
		// } else {
			// log.Info(kv.me, " is applying operation ", opea)
			// switch opea {
			// case "Get":
			// case "PutAppend":
				// var key string
				// var value string
				// var op string
				// if d.Decode(&key) != nil ||
				// d.Decode(&value) != nil ||
				// d.Decode(&op) != nil {
					// panic("failed to decode msg args")
				// } else {
					// kv.putAppend(key, value, op)
				// }
			// }
		// }
		waitToRemove := make([]int, 0, 1)
		// log.Info(kv.me, " applier before rlock")
		kv.applyListenersMutex.RLock()
		// log.Info("I have ", len(kv.applyListeners), " listeners")
		// log.Info(kv.me, " applier after rlock")
		for i, v := range kv.applyListeners {
			// log.Info(kv.me, " before notifying one listener about index update")
			select {
			case v.indexCh <- (int64)(msg.CommandIndex):
			case <- v.exitCh:
				waitToRemove = append(waitToRemove, i)
			}
			// log.Info(kv.me, " notified one listener about index update")
		}
		kv.applyListenersMutex.RUnlock()
		// log.Info(kv.me, " runlocked listeners")
		kv.applyListenersMutex.Lock()
		log.Info(kv.me, " I have ", len(kv.applyListeners), " listeners")
		log.Info(waitToRemove)
		for _, v := range waitToRemove {
			kv.applyListeners = removeBroadcastCh(kv.applyListeners, v)
		}
		// log.Info(kv.me, " I have ", len(kv.applyListeners), " listeners now")
		kv.applyListenersMutex.Unlock()
		// log.Info(kv.me, " unlocked listeners")
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
