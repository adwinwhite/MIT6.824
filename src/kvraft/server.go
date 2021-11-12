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
	// "context"
	"time"
	"strconv"
	"bytes"
)

const Debug = false

const (
	ServiceRPCTimeout = 5 * time.Second
)

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

// I wish there is tuple
type IndexAndTerm struct {
	index int64
	term int64
}


type BroadcastChan struct {
	indexCh chan IndexAndTerm
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
	persister *raft.Persister
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applyListeners []BroadcastChan
	applyListenersMutex sync.RWMutex
	data    map[string]string
	dataMutex sync.RWMutex

	serialNos map[string]int64
}

func (kv *KVServer) get(key string) (value string, err Err) {
	kv.dataMutex.RLock()
	value, ok := kv.data[key]
	kv.dataMutex.RUnlock()
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
	kv.dataMutex.Lock()
	defer kv.dataMutex.Unlock()
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
	val, _ := kv.data[key]
	log.WithFields(log.Fields{
		"key": key,
		"value": value,
		"op": op,
	}).Info(kv.me, " putAppend ", key, ": ", val)
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Check whether I am leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Submit operation
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode("Get")
	// e.Encode(args.Key)
	// data := w.Bytes()
	data, _ := json.Marshal(args)
	index, term, isLeader := kv.rf.Start(data)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	var ls BroadcastChan
	ls.indexCh = make(chan IndexAndTerm)
	ls.exitCh = make(chan struct{})
	defer close(ls.exitCh)
	kv.applyListenersMutex.Lock()
	kv.applyListeners = append(kv.applyListeners, ls)
	log.WithFields(log.Fields{
		"index": index,
		"term": term,
	}).Info(kv.me, " Get ", fmt.Sprintf("%+v", args))
	kv.applyListenersMutex.Unlock()
	// Determine whether applyMsg matches command.
	// Case 1: entry at index is what we submitted.
	// Case 2: entry at index is not what we submitted due to the leader died or lost leadership before propagating this entry.
	// Just check term of entry at index.
	// What if it's a snapshot? Leader only sends snapshot at restart. Index of snapshot is guaranteed to be smaller than what we need.
	for {
		indexWithTerm := <- ls.indexCh
		// Is there a chance that indexWithTerm.index is greater than index?
		if indexWithTerm.index == (int64)(index) {
			if indexWithTerm.term == (int64)(term) {
				reply.Value, reply.Err = kv.get(args.Key)
				return
			} else {
				reply.Err = ErrWrongLeader
				return
			}
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

	index, term, isLeader := kv.rf.Start(data)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	log.WithFields(log.Fields{
		"index": index,
		"term": term,
	}).
	Info(kv.me, " PutAppend: start returns")
	var ls BroadcastChan
	ls.indexCh = make(chan IndexAndTerm)
	ls.exitCh = make(chan struct{})
	defer close(ls.exitCh)
	kv.applyListenersMutex.Lock()
	kv.applyListeners = append(kv.applyListeners, ls)
	log.Info(kv.me, " append a listener for PutAppend")
	kv.applyListenersMutex.Unlock()
	for {
		indexWithTerm := <- ls.indexCh
		if indexWithTerm.index == (int64)(index) {
			if indexWithTerm.term == (int64)(term) {
				return
			} else {
				reply.Err = ErrWrongLeader
				return
			}
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
		if msg.CommandValid {
			c, ok := msg.Command.([]byte)
			if ok {
				// Check whether command is no-op
				if len(c) == 0 {
					log.WithFields(log.Fields{
						"index": msg.CommandIndex,
						"term": msg.CommandTerm,
					}).Info(kv.me, " received no-op")
				} else {
					var args map[string]string
					if err := json.Unmarshal(c, &args); err != nil {
						panic("failed to decode msg operation type")
					}
					clerkId, ok := args["ClerkId"]
					if !ok {
						panic("failed to access clerk id")
					}

					_, ok = kv.serialNos[clerkId]
					if !ok {
						kv.serialNos[clerkId] = 0
					}


					log.WithFields(log.Fields{
						"clerkId": clerkId,
						"serialNo": kv.serialNos[clerkId],
						"index": msg.CommandIndex,
						"term": msg.CommandTerm,
					}).Info(kv.me, args)

					serialStr, ok := args["SerialNo"]
					if ok {
						serialNo, err := strconv.Atoi(serialStr)
						if err != nil {
							panic("failed to convert serial string to number")
						}
						if (int64)(serialNo) > kv.serialNos[clerkId] {
							_, ok := args["Value"]
							if ok {
								kv.putAppend(args["Key"], args["Value"], args["Op"])
							} 
							// atomic.StoreInt64(&kv.serialNo, (int64)(serialNo))
							kv.serialNos[clerkId] = (int64)(serialNo)
							log.Info(kv.me, args, " applied")
						}
					}
				}
			} else {
				panic("Command is not no-op nor bytes")
			}

			// Check raft persister size.
			if kv.persister.RaftStateSize() > kv.maxraftstate {
				snapshotData := kv.createSnapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshotData)
			}
		} else if msg.SnapshotValid {
			// Receive snapshot
			kv.applySnapshot(msg.Snapshot)
		} else {
			continue
		}

		// Notify all listeners about index&term of msg
		var indexToNotify IndexAndTerm
		if msg.CommandValid {
			indexToNotify.index = (int64)(msg.CommandIndex)
			indexToNotify.term = msg.CommandTerm
		} else if msg.SnapshotValid {
			indexToNotify.index = (int64)(msg.SnapshotIndex)
			indexToNotify.term = (int64)(msg.SnapshotTerm)
		} 
		waitToRemove := make([]int, 0, 1)
		kv.applyListenersMutex.RLock()
		for i, v := range kv.applyListeners {
			// log.Info(kv.me, " before notifying one listener about index update")
			select {
			case v.indexCh <- indexToNotify:
			case <- v.exitCh:
				waitToRemove = append(waitToRemove, i)
			}
			// log.Info(kv.me, " notified one listener about index update")
		}
		kv.applyListenersMutex.RUnlock()
		kv.applyListenersMutex.Lock()
		for i := len(waitToRemove) - 1; i >= 0; i-- {
			kv.applyListeners = removeBroadcastCh(kv.applyListeners, waitToRemove[i])
		}
		kv.applyListenersMutex.Unlock()
	}
}


func (kv *KVServer) createSnapshot() []byte {
	kv.dataMutex.RLock()
	defer kv.dataMutex.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	data := w.Bytes()
	return data
}

func (kv *KVServer) applySnapshot(snapshotData []byte) {
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	kv.dataMutex.Lock()
	defer kv.dataMutex.Unlock()
	newData := make(map[string]string)
	if d.Decode(&newData) != nil {
		panic("failed to decode kv data")
	} else {
		kv.data = newData
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
	kv.serialNos = make(map[string]int64)
	kv.persister = persister

	// You may need initialization code here.
	go kv.applier()

	return kv
}
