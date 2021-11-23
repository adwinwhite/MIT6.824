package shardkv

import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
	"time"
	"github.com/sasha-s/go-deadlock"

	"6.824/shardctrler"
)

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	// ctrlers      []*labrpc.ClientEnd
	ctrler       *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applyListeners      []BroadcastChan
	applyListenersMutex deadlock.RWMutex

	data      map[string]string
	dataMutex deadlock.RWMutex
	// snapshotMutex deadlock.RWMutex

	serialNos map[int64]int64

	config        shardctrler.Config
	configMutex   deadlock.RWMutex
	clerkInfo     ClerkHeader
}

// I wish there is tuple
type IndexAndTerm struct {
	index int64
	term  int64
}

type BroadcastChan struct {
	indexCh chan IndexAndTerm
	exitCh  chan struct{}
}

func removeBroadcastCh(s []BroadcastChan, i int) []BroadcastChan {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

type Op struct {
	// Your data here.
	Name      string
	Args      interface{}
	ClerkInfo ClerkHeader
}

func (kv *ShardKV) get(key string) (value string, err Err) {
	// kv.snapshotMutex.RLock()
	// defer kv.snapshotMutex.RUnlock()
	kv.dataMutex.RLock()
	defer kv.dataMutex.RUnlock()
	value, ok := kv.data[key]
	if ok {
		err = OK
	} else {
		err = ErrNoKey
	}
	log.Info(kv.gid, "-", kv.me, " get ", key, ": ", kv.data[key])
	return value, err
}

func (kv *ShardKV) putAppend(key string, value string, op string) {
	// kv.snapshotMutex.RLock()
	// defer kv.snapshotMutex.RUnlock()
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
	log.Info(kv.gid, "-", kv.me, " putAppend ", key, ": ", kv.data[key])
}

func (kv *ShardKV) reconfigure(config shardctrler.Config, shardData map[string]string) {
	kv.dataMutex.Lock()
	defer kv.dataMutex.Unlock()
	for k, v := range shardData {
		kv.data[k] = v
	}
	kv.configMutex.Lock()
	defer kv.configMutex.Unlock()
	kv.config = config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Check clerk config num first
	// If my config is not up-to-date, wait.
	// But for simplicity, just return ErrWrongGroup and let clerk retry.
	kv.configMutex.RLock()
	// merely for debugging
	confNum := kv.config.Num
	if args.Header.ConfigNo > kv.config.Num {
		reply.Err = ErrWrongGroup
		kv.configMutex.RUnlock()
		return
	} else {
		kv.configMutex.RUnlock()
	}

	// Check shard first
	if !kv.isMyShard(key2shard(args.Body.Key)) {
		reply.Err = ErrWrongGroup
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Name: "Get", Args: args.Body, ClerkInfo: args.Header}
	index, term, isLeader := kv.rf.Start(op)
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
		"term":  term,
		"configNo": confNum,
	}).Info(kv.gid, "-", kv.me, " Get ", fmt.Sprintf("%+v", args))
	kv.applyListenersMutex.Unlock()
	// Determine whether applyMsg matches command.
	// Case 1: entry at index is what we submitted.
	// Case 2: entry at index is not what we submitted due to the leader died or lost leadership before propagating this entry.
	// Just check term of entry at index.
	// What if it's a snapshot? Leader only sends snapshot at restart. Index of snapshot is guaranteed to be smaller than what we need.
	for {
		indexWithTerm := <-ls.indexCh
		// Is there a chance that indexWithTerm.index is greater than index?
		if indexWithTerm.index == (int64)(index) {
			if indexWithTerm.term == (int64)(term) {
				reply.Value, reply.Err = kv.get(args.Body.Key)
				return
			} else {
				reply.Err = ErrWrongLeader
				return
			}
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Check clerk config num first
	// If my config is not up-to-date, wait.
	// But for simplicity, just return ErrWrongGroup and let clerk retry.
	kv.configMutex.RLock()
	if args.Header.ConfigNo > kv.config.Num {
		reply.Err = ErrWrongGroup
		kv.configMutex.RUnlock()
		return
	} else {
		kv.configMutex.RUnlock()
	}

	// Check shard first
	if !kv.isMyShard(key2shard(args.Body.Key)) {
		reply.Err = ErrWrongGroup
		return
	}

	// Reply Ok by default.
	reply.Err = OK

	// Check whether I am leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{Name: "PutAppend", Args: args.Body, ClerkInfo: args.Header}
	index, term, isLeader := kv.rf.Start(op)
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
		"term":  term,
	}).Info(kv.gid, "-", kv.me, " PutAppend ", fmt.Sprintf("%+v", args))
	kv.applyListenersMutex.Unlock()

	for {
		indexWithTerm := <-ls.indexCh
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


type ReconfigureArgs struct {
	Config shardctrler.Config
	ShardData map[string]string
}

// Log reconfiguration and requested shard data
func (kv *ShardKV) Reconfigure(config shardctrler.Config, shardData map[string]string) bool {
	// This function only returns false when I am no longer leader
	// Check whether I am leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return false
	}

	atomic.AddInt64(&kv.clerkInfo.SerialNo, 1)
	op := Op{Name: "Reconfigure", Args: ReconfigureArgs{Config: config, ShardData: shardData}, ClerkInfo: kv.clerkInfo}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}

	var ls BroadcastChan
	ls.indexCh = make(chan IndexAndTerm)
	ls.exitCh = make(chan struct{})
	defer close(ls.exitCh)
	kv.applyListenersMutex.Lock()
	kv.applyListeners = append(kv.applyListeners, ls)
	log.WithFields(log.Fields{
		"index": index,
		"term":  term,
	}).Info(kv.gid, "-", kv.me, " Reconfigure ", fmt.Sprintf("%+v", op))
	kv.applyListenersMutex.Unlock()

	for {
		indexWithTerm := <-ls.indexCh
		if indexWithTerm.index == (int64)(index) {
			if indexWithTerm.term == (int64)(term) {
				return true
			} else {
				return false
			}
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) isMyShard(s int) bool {
	kv.configMutex.RLock()
	defer kv.configMutex.RUnlock()
	if kv.config.Shards[s] == kv.gid {
		return true
	}
	return false
}

type RequestShardsArgs struct {
	Shards []int
}

type RequestShardsReply struct {
	ShardsData map[string]string
	Err        Err
}

func (kv *ShardKV) RequestShards(args *RequestShardsArgs, reply *RequestShardsReply) {
	reply.Err = OK
	// Check whether I am leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.dataMutex.RLock()
	defer kv.dataMutex.RUnlock()
	// Copy key-value pairs that are in requested shards
	reply.ShardsData = make(map[string]string)
	for k, v := range kv.data {
		s := key2shard(k)
		isRequested := false
		for _, rs := range args.Shards {
			if s == rs {
				isRequested = true
				break
			}
		}
		if isRequested {
			reply.ShardsData[k] = v
		}
	}
}

func (kv *ShardKV) getShards(oldConf shardctrler.Config, newConf shardctrler.Config) map[string]string {

	// If old config is the first one, return empty shard data
	if oldConf.Num == 0 {
		return make(map[string]string)
	}

	absentShards := func(oldShards []int, newShards []int) map[int][]int {
		myOld := make([]int, 0)
		for s, g := range oldShards {
			if g == kv.gid {
				myOld = append(myOld, s)
			}
		}

		// map old gid to shards
		myAbsent := make(map[int][]int)
		for s, g := range newShards {
			if g == kv.gid {
				exists := false
				for _, ns := range myOld {
					if s == ns {
						exists = true
						break
					}
				}
				if !exists {
					if myAbsent[oldShards[s]] == nil {
						myAbsent[oldShards[s]] = append([]int(nil), s)
					}
					myAbsent[oldShards[s]] = append(myAbsent[oldShards[s]], s)
				}
			}
		}
		return myAbsent
	}(oldConf.Shards[:], newConf.Shards[:])

	// log.Info(kv.me, " absent shards: ", absentShards)

	resultCh := make(chan map[string]string)

	getShards := func(gid int, shards []int, groups map[int][]string, resCh chan map[string]string) {
		servers, ok := groups[gid]
		if !ok {
			panic("No such gid")
		}
		args := RequestShardsArgs{Shards: shards}
		for {
			for _, srv := range servers {
				peer := kv.make_end(srv)
				var reply RequestShardsReply
				ok = peer.Call("ShardKV.RequestShards", &args, &reply)
				if ok && reply.Err == OK {
					resCh <- reply.ShardsData
					return
				}
			}
		}
	}

	for g, ss := range absentShards {
		go getShards(g, ss, oldConf.Groups, resultCh)
	}

	requestedShardData := make(map[string]string)
	for i := 0; i < len(absentShards); i++ {
		shardsData := <-resultCh
		for k, v := range shardsData {
			requestedShardData[k] = v
		}
	}
	return requestedShardData
}

func (kv *ShardKV) configDetector() {
	for {
		// Query only if I am leader
		_, isLeader := kv.rf.GetState()
		if isLeader {
			// Query ctrler about latest config
			latestConfig := kv.ctrler.Query(-1)
			// log.Info(kv.gid, "-", kv.me, " LatestConfig: ", latestConfig)

			// Config changed
			kv.configMutex.RLock()
			if latestConfig.Num > kv.config.Num {
				// Request shards' data from other groups
				// log.Info(kv.gid, "-", kv.me, " MyConfigNum: ", kv.config)
				shardData := kv.getShards(kv.config, latestConfig)
				kv.configMutex.RUnlock()
				// log.Info(kv.gid, "-", kv.me, " ShardData: ", shardData)
				kv.Reconfigure(latestConfig, shardData)
			} else {
				kv.configMutex.RUnlock()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			switch c := msg.Command.(type) {
			case Op:
				clerkId := c.ClerkInfo.ClerkId

				// Check whether serialNo for this clerk exists
				_, ok := kv.serialNos[clerkId]
				if !ok {
					kv.serialNos[clerkId] = 0
				}

				log.WithFields(log.Fields{
					"index":    msg.CommandIndex,
					"serialNo": kv.serialNos[clerkId],
				}).Info(kv.gid, "-", kv.me, " Command : ", fmt.Sprintf("%+v", c))
				// Only apply command if msg's serialNo is larger than recorded one so that duplicate command won't be applied
				if c.ClerkInfo.SerialNo > kv.serialNos[clerkId] {
					switch c.Name {
					case "Get":
					case "PutAppend":
						args, ok := c.Args.(PutAppendArgsBody)
						if !ok {
							panic("failed to assert args as JoinArgsBody")
						}
						kv.putAppend(args.Key, args.Value, args.Op)
					case "Reconfigure":
						args, ok := c.Args.(ReconfigureArgs)
						if !ok {
							panic("failed to assert args as ReconfigureArgs")
						}
						kv.reconfigure(args.Config, args.ShardData)
					}
					kv.serialNos[clerkId] = c.ClerkInfo.SerialNo
					// kv.configMutex.RLock()
					// log.WithFields(log.Fields{
					// "id": kv.me,
					// "index": msg.CommandIndex,
					// }).Info(c)
					// kv.configMutex.RUnlock()
				}
			case bool:
				log.WithFields(log.Fields{
					"index": msg.CommandIndex,
					"term":  msg.CommandTerm,
				}).Info(kv.me, " received no-op")
			default:
				panic("Command is not no-op nor bytes")
			}

			if kv.maxraftstate > 0 && kv.rf.Persister().RaftStateSize() > kv.maxraftstate {
				snapshotData := kv.createSnapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshotData)
			}

			// Check raft persister size. Concurrent version.
			// if atomic.LoadInt32(&kv.isSnapshoting) == 0 && kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
			// kv.snapshotStateMutex.Lock()
			// if atomic.LoadInt32(&kv.isSnapshoting) == 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
			// atomic.StoreInt32(&kv.isSnapshoting, 1)
			// snapshotData := kv.createSnapshot()
			// kv.rf.Snapshot(msg.CommandIndex, snapshotData)
			// atomic.StoreInt32(&kv.isSnapshoting, 0)
			// }
			// kv.snapshotStateMutex.Unlock()
			// }
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
			select {
			case v.indexCh <- indexToNotify:
			case <-v.exitCh:
				waitToRemove = append(waitToRemove, i)
			}
		}
		kv.applyListenersMutex.RUnlock()
		kv.applyListenersMutex.Lock()
		for i := len(waitToRemove) - 1; i >= 0; i-- {
			kv.applyListeners = removeBroadcastCh(kv.applyListeners, waitToRemove[i])
		}
		kv.applyListenersMutex.Unlock()
	}
}

func (kv *ShardKV) createSnapshot() []byte {
	kv.dataMutex.RLock()
	defer kv.dataMutex.RUnlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.serialNos)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) applySnapshot(snapshotData []byte) {
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	kv.dataMutex.RLock()
	defer kv.dataMutex.RUnlock()
	newData := make(map[string]string, 0)
	newSerialNos := make(map[int64]int64)
	if d.Decode(&newData) != nil ||
		d.Decode(&newSerialNos) != nil {
		panic("failed to decode kv data")
	} else {
		kv.data = newData
		kv.serialNos = newSerialNos
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	// kv.ctrlers = ctrlers

	// Your initialization code here.
	labgob.Register(Op{})
	labgob.Register(ClerkHeader{})
	labgob.Register(GetArgsBody{})
	labgob.Register(PutAppendArgsBody{})
	labgob.Register(ReconfigureArgs{})

	// Use something like this to talk to the shardctrler:
	kv.ctrler = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.serialNos = make(map[int64]int64)
	kv.config = kv.ctrler.Query(-1)
	kv.clerkInfo = ClerkHeader{ClerkId: time.Now().UnixNano(), SerialNo: 0, ConfigNo: -1}
	log.Info(kv.gid, "-", kv.me, " config:", kv.config)
	kv.data = make(map[string]string)
	go kv.configDetector()
	go kv.applier()

	return kv
}
