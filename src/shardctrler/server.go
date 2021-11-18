package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import (
	log "github.com/sirupsen/logrus"
	"fmt"
	// "math"
	"sort"
	// "bytes"
	// "encoding/gob"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	applyListeners      []BroadcastChan
	applyListenersMutex sync.RWMutex

	configs []Config // indexed by config num
	configMutex         sync.RWMutex

	serialNos map[int64]int64


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
	Name string
	Args interface{}
	ClerkInfo ClerkHeader
}

// func (sc *ShardCtrler) encodeToBytes(data interface{}) []byte {
	// buf := bytes.Buffer{}
	// enc := gob.NewEncoder(&buf)
	// err := enc.Encode(data)
	// if err != nil {
		// log.WithFields(log.Fields{
			// "ctrlerId": sc.me,
		// }).Fatal("encodeToBytes: ", err)
	// }
	// return buf.Bytes()
// }

// func (sc *ShardCtrler) decodeToOp(b []byte) Op {
	// op := Op{}
	// dec := gob.NewDecoder(bytes.NewReader(b))
	// err := dec.Decode(&op)
	// if err != nil {
		// log.WithFields(log.Fields{
			// "ctrlerId": sc.me,
		// }).Fatal("decodeToOp: ", err)
	// }
	// return op
// }

func balanceShards(shards *[NShards]int, servers map[int][]string) {
	if len(shards) == 0 || len(servers) == 0{
		return
	}
	// log.Info("shards: ", shards)
	// log.Info("servers: ", servers)



	// How many shards should a group serve?
	// aveShards := float64(len(shards)) / float64(len(servers))

	// Count how many shards a group actually serve now.
	counts := make(map[int][]int)
	leavingGroups := make([]int, 0)
	for k := range servers {
		counts[k] = make([]int, 0)
	}
	for s, g := range shards {
		_, ok := counts[g]

		// gid may not exists if it left
		if ok {
			counts[g] = append(counts[g], s)
		} else {
			counts[g] = append([]int(nil), s)
			leavingGroups = append(leavingGroups, g)
		}
	}
	// Flatten shards belonging to leavingGroups and remove them from counts
	unservedShards := make([]int, 0)
	for _, g := range leavingGroups {
		unservedShards = append(unservedShards, counts[g]...)
		delete(counts, g)
	}
	// Add shards that point to invalid gid 0
	unservedShards = append(unservedShards, counts[0]...)
	delete(counts, 0)
	// log.Info("initial counts: ", counts)

	// Sort groups by length
	gids := make([]int, 0)
	for g := range counts {
		gids = append(gids, g)
	}
	// In case of two groups having the same length but having different ordering in servers map. This scenrio exists though I have no idea why.
	sort.Slice(gids, func(i, j int) bool { 
		if len(counts[gids[i]]) > len(counts[gids[j]]) {
			return true
		} else if len(counts[gids[i]]) == len(counts[gids[j]]) {
			if gids[i] > gids[j] {
				return true
			}
		}
		return false
	})
	// log.Info("sorted gids: ", gids)

	takeOneShardFromAbundant := func() int {
		// First take from unserved shards
		if len(unservedShards) > 0 {
			oneShard := unservedShards[0]
			unservedShards = unservedShards[1:]
			return oneShard
		} 
		largestLen := len(counts[gids[0]])
		rightestGID := gids[0]
		for i, g := range gids {
			if len(counts[g]) != largestLen {
				rightestGID = gids[i - 1]
				break
			}
		}
		oneShard := counts[rightestGID][0]
		counts[rightestGID] = counts[rightestGID][1:]
		return oneShard
	}

	checkBalance := func() bool {
		if len(unservedShards) != 0 {
			return false
		}
		// log.Info(gids)
		// log.Info(counts[gids[0]], counts[gids[len(gids) - 1]])
		if len(counts[gids[0]]) - len(counts[gids[len(gids) - 1]]) <= 1 {
			return true
		}
		return false
	}

	// Fill from groups with least shards
	for checkBalance() == false {
		oneShard := takeOneShardFromAbundant()
		smallestLen := len(counts[gids[len(gids) - 1]])
		leftestGID := gids[len(gids) - 1]
		for _, g := range gids {
			if len(counts[g]) == smallestLen {
				leftestGID = g
				break
			}
		}
		counts[leftestGID] = append(counts[leftestGID], oneShard)
		// log.Info("current gids: ", gids)
		// log.Info("current counts: ", counts)
	}


	for g, v := range counts {
		for _, s := range v {
			shards[s] = g
		}
	}
}

// Requires re-balancing
func (sc *ShardCtrler) join(servers map[int][]string) {
	sc.configMutex.Lock()
	defer sc.configMutex.Unlock()
	newConfig := sc.configs[len(sc.configs) - 1].deepcopy()
	newConfig.Num = len(sc.configs)

	for k := range servers {
		_, ok := newConfig.Groups[k]
		// If gid exists then append, otherwise put.
		if ok {
			newConfig.Groups[k] = append(newConfig.Groups[k], servers[k]...)
		} else {
			newConfig.Groups[k] = append([]string(nil), servers[k]...)
		}
	}

	balanceShards(&newConfig.Shards, newConfig.Groups)


	sc.configs = append(sc.configs, newConfig)
}

// Requires re-balancing
func (sc *ShardCtrler) leave(gids []int) {
	sc.configMutex.Lock()
	defer sc.configMutex.Unlock()
	newConfig := sc.configs[len(sc.configs) - 1].deepcopy()
	newConfig.Num = len(sc.configs)

	for _, id := range gids {
		delete(newConfig.Groups, id)
	}

	balanceShards(&newConfig.Shards, newConfig.Groups)

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) move(shard int, gid int) {
	sc.configMutex.Lock()
	defer sc.configMutex.Unlock()
	newConfig := sc.configs[len(sc.configs) - 1].deepcopy()
	newConfig.Num = len(sc.configs)
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) query(num int) Config {
	sc.configMutex.RLock()
	defer sc.configMutex.RUnlock()
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs) - 1]
	}
	return sc.configs[num]
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// Reply Ok by default.
	reply.Err = OK
	// Check whether I am leader
	_, isLeader := sc.rf.GetState()
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
	// data, _ := json.Marshal(args)
	op := Op{Name: "Join", Args: args.Body, ClerkInfo: args.Header}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var ls BroadcastChan
	ls.indexCh = make(chan IndexAndTerm)
	ls.exitCh = make(chan struct{})
	defer close(ls.exitCh)
	sc.applyListenersMutex.Lock()
	sc.applyListeners = append(sc.applyListeners, ls)
	log.WithFields(log.Fields{
		"index": index,
		"term":  term,
	}).Info(sc.me, " Join ", fmt.Sprintf("%+v", args))
	sc.applyListenersMutex.Unlock()

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


func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// Reply Ok by default.
	reply.Err = OK
	// Check whether I am leader
	_, isLeader := sc.rf.GetState()
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
	// data, _ := json.Marshal(args)
	op := Op{Name: "Leave", Args: args.Body, ClerkInfo: args.Header}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var ls BroadcastChan
	ls.indexCh = make(chan IndexAndTerm)
	ls.exitCh = make(chan struct{})
	defer close(ls.exitCh)
	sc.applyListenersMutex.Lock()
	sc.applyListeners = append(sc.applyListeners, ls)
	log.WithFields(log.Fields{
		"index": index,
		"term":  term,
	}).Info(sc.me, " Leave ", fmt.Sprintf("%+v", args))
	sc.applyListenersMutex.Unlock()

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


func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// Reply Ok by default.
	reply.Err = OK
	// Check whether I am leader
	_, isLeader := sc.rf.GetState()
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
	// data, _ := json.Marshal(args)
	op := Op{Name: "Move", Args: args.Body, ClerkInfo: args.Header}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	var ls BroadcastChan
	ls.indexCh = make(chan IndexAndTerm)
	ls.exitCh = make(chan struct{})
	defer close(ls.exitCh)
	sc.applyListenersMutex.Lock()
	sc.applyListeners = append(sc.applyListeners, ls)
	log.WithFields(log.Fields{
		"index": index,
		"term":  term,
	}).Info(sc.me, " Move ", fmt.Sprintf("%+v", args))
	sc.applyListenersMutex.Unlock()

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



func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
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
	// data, _ := json.Marshal(args)
	op := Op{Name: "Query", Args: args.Body, ClerkInfo: args.Header}
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	var ls BroadcastChan
	ls.indexCh = make(chan IndexAndTerm)
	ls.exitCh = make(chan struct{})
	defer close(ls.exitCh)
	sc.applyListenersMutex.Lock()
	sc.applyListeners = append(sc.applyListeners, ls)
	log.WithFields(log.Fields{
		"index": index,
		"term":  term,
	}).Info(sc.me, " Query ", fmt.Sprintf("%+v", args))
	sc.applyListenersMutex.Unlock()
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
				reply.Err = OK
				reply.Config = sc.query(args.Body.Num)
				return
			} else {
				reply.Err = ErrWrongLeader
				return
			}
		}
	}
}



//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			switch c := msg.Command.(type) {
			case Op:
				clerkId := c.ClerkInfo.ClerkId
				
				// Check whether serialNo for this clerk exists
				_, ok := sc.serialNos[clerkId]
				if !ok {
					sc.serialNos[clerkId] = 0
				}

				// Only apply command if msg's serialNo is larger than recorded one so that duplicate command won't be applied
				if c.ClerkInfo.SerialNo > sc.serialNos[clerkId] {
					switch c.Name {
					case "Join":
						args, ok := c.Args.(JoinArgsBody)
						if !ok {
							panic("failed to assert args as JoinArgsBody")
						}
						sc.join(args.Servers)
					case "Leave":
						args, ok := c.Args.(LeaveArgsBody)
						if !ok {
							panic("failed to assert args as JoinArgsBody")
						}
						sc.leave(args.GIDs)
					case "Move":
						args, ok := c.Args.(MoveArgsBody)
						if !ok {
							panic("failed to assert args as JoinArgsBody")
						}
						sc.move(args.Shard, args.GID)
					case "Query":
					}
					// sc.configMutex.RLock()
					// log.WithFields(log.Fields{
						// "id": sc.me,
						// "index": msg.CommandIndex,
					// }).Info(sc.configs[len(sc.configs) - 1])
					// sc.configMutex.RUnlock()
				}
			case bool:
				log.WithFields(log.Fields{
					"index": msg.CommandIndex,
					"term":  msg.CommandTerm,
				}).Info(sc.me, " received no-op")
			default:
				panic("Command is not no-op nor bytes")
			}

			// Check raft persister size. Concurrent version.
			// if atomic.LoadInt32(&sc.isSnapshoting) == 0 && sc.maxraftstate > 0 && sc.persister.RaftStateSize() > sc.maxraftstate {
				// sc.snapshotStateMutex.Lock()
				// if atomic.LoadInt32(&sc.isSnapshoting) == 0 && sc.persister.RaftStateSize() > sc.maxraftstate {
				    // atomic.StoreInt32(&sc.isSnapshoting, 1)
				    // snapshotData := sc.createSnapshot()
				    // sc.rf.Snapshot(msg.CommandIndex, snapshotData)
				    // atomic.StoreInt32(&sc.isSnapshoting, 0)
				// }
				// sc.snapshotStateMutex.Unlock()
			// }
		} else if msg.SnapshotValid {
			// Receive snapshot
			// sc.applySnapshot(msg.Snapshot)
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
		sc.applyListenersMutex.RLock()
		for i, v := range sc.applyListeners {
			// log.Info(sc.me, " before notifying one listener about index update")
			select {
			case v.indexCh <- indexToNotify:
			case <-v.exitCh:
				waitToRemove = append(waitToRemove, i)
			}
			// log.Info(sc.me, " notified one listener about index update")
		}
		sc.applyListenersMutex.RUnlock()
		sc.applyListenersMutex.Lock()
		for i := len(waitToRemove) - 1; i >= 0; i-- {
			sc.applyListeners = removeBroadcastCh(sc.applyListeners, waitToRemove[i])
		}
		sc.applyListenersMutex.Unlock()
	}
}

// func (sc *ShardCtrler) createSnapshot() []byte {
	// sc.snapshotMutex.RLock()
	// defer sc.snapshotMutex.RUnlock()
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(sc.data)
	// e.Encode(sc.serialNos)
	// data := w.Bytes()
	// return data
// }

// func (sc *ShardCtrler) applySnapshot(snapshotData []byte) {
	// r := bytes.NewBuffer(snapshotData)
	// d := labgob.NewDecoder(r)
	// sc.snapshotMutex.Lock()
	// defer sc.snapshotMutex.Unlock()
	// newData := make([]map[string]string, 0)
	// newSerialNos := make(map[string]int64)
	// if d.Decode(&newData) != nil ||
		// d.Decode(&newSerialNos) != nil {
		// panic("failed to decode sc data")
	// } else {
		// sc.data = newData
		// sc.serialNos = newSerialNos
	// }
// }
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(ClerkHeader{})
	labgob.Register(JoinArgsBody{})
	labgob.Register(LeaveArgsBody{})
	labgob.Register(MoveArgsBody{})
	labgob.Register(QueryArgsBody{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.serialNos = make(map[int64]int64)
	go sc.applier()

	return sc
}
