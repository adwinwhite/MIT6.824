package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"fmt"
	"bytes"
	"encoding/gob"
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
	// SerialNo int64
	// ClerkId  int64
}

func (sc *ShardCtrler) encodeToBytes(data interface{}) []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		log.WithFields(log.Fields{
			"ctrlerId": sc.me,
		}).Fatal("encodeToBytes: ", err)
	}
	return buf.Bytes()
}

func (sc *ShardCtrler) decodeToOp(b []byte) Op {
	op := Op{}
	dec := gob.NewDecoder(bytes.NewReader(b))
	err := dec.Decode(&op)
	if err != nil {
		log.WithFields(log.Fields{
			"ctrlerId": sc.me,
		}).Fatal("decodeToOp: ", err)
	}
	return op
}

func (sc *ShardCtrler) join(servers map[int][]string) {
	sc.configMutex.Lock()
	defer sc.configMutex.Unlock()
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
	op := Op{Name: "Join", Args: args}
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
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
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
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
