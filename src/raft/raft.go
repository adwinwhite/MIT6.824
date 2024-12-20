package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"github.com/sasha-s/go-deadlock"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
	// "encoding/json"
	// "strconv"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int64

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

type LogEntry struct {
	Command interface{}
	Term    int64
}

type RaftRole int32

const (
	Follower RaftRole = iota
	Candidate
	Leader
)

const (
	CollectVotesTimeout time.Duration = 300 * time.Millisecond //1s
	ElectionTimerBase   time.Duration = 200 * time.Millisecond
	ElectionTimerRandom time.Duration = 400 * time.Millisecond
	RPCTimeout          time.Duration = CollectVotesTimeout
	AppendEntriesRPCTimeout time.Duration = 100 * time.Millisecond
	AppendEntriesFailWaitTime time.Duration = 10 * time.Millisecond
	AppendEntriesIdleWaitTime time.Duration = 50 * time.Millisecond
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64               // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm   int64
	votedFor      int64
	log           []LogEntry
	logMutex      deadlock.RWMutex

	// Volatile state
	commitIndex      int64
	commitIndexMutex deadlock.RWMutex
	lastApplied      int64
	applyMsgMutex    deadlock.Mutex

	// Volatile state for leader
	nextIndex          []int64
	matchIndex         []int64
	followerIndexMutex deadlock.RWMutex

	//
	role           RaftRole
	cancelElection context.CancelFunc
	cancelMutex    deadlock.Mutex

	// Snapshot and last included index&term
	// Protected by logMutex
	snapshotData     []byte
	lastIncludedTerm int64
	logFirstIndex    int64
}

func (rf *Raft) Persister() *Persister {
	return rf.persister
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return (int)(atomic.LoadInt64(&rf.currentTerm)), atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Leader)
}

func (rf *Raft) becomeFollower() {
	// if atomic.CompareAndSwapInt32((*int32)(&rf.role), (int32)(Leader), (int32)(Follower)) || atomic.CompareAndSwapInt32((*int32)(&rf.role), (int32)(Candidate), (int32)(Follower)) {}
	atomic.StoreInt32((*int32)(&rf.role), (int32)(Follower))
	atomic.StoreInt64(&rf.votedFor, -1)
	// log.WithFields(log.Fields{
	// "term": atomic.LoadInt64(&rf.currentTerm),
	// }).Info(rf.me, " became a follower")
}

func (rf *Raft) becomeLeader() bool {
	rf.logMutex.RLock()
	logLength := (int64)(len(rf.log)) + rf.logFirstIndex
	rf.logMutex.RUnlock()
	rf.followerIndexMutex.Lock()
	rf.nextIndex = make([]int64, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = logLength
	}
	rf.matchIndex = make([]int64, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.followerIndexMutex.Unlock()

	if !atomic.CompareAndSwapInt32((*int32)(&rf.role), (int32)(Candidate), (int32)(Leader)) {
		return false
	}

	log.WithFields(log.Fields{
		"term": atomic.LoadInt64(&rf.currentTerm),
	}).Info(rf.me, " became the leader")
	// Notify the service that there is leadership change.
	rf.Start(false)
	return true
}

func (rf *Raft) becomeCandidate() {
	atomic.StoreInt32((*int32)(&rf.role), (int32)(Candidate))
	// rf.termMutex.Lock()
	// defer rf.termMutex.Unlock()
	term := atomic.LoadInt64(&rf.currentTerm)
	atomic.StoreInt64(&rf.currentTerm, term+1)
	atomic.StoreInt64(&rf.votedFor, (int64)(rf.me))
	// log.Info(rf.me, " prepares to lock log mutex when becoming candiate")
	rf.logMutex.RLock()
	// log.Info(rf.me, " locks log mutex when becoming candiate")
	rf.persist(false)
	rf.logMutex.RUnlock()
	// log.Info(rf.me, " unlocks log mutex when becoming candiate")
	log.WithFields(log.Fields{
		"term": atomic.LoadInt64(&rf.currentTerm),
	}).Info(rf.me, " became a candidate")
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Need logMutex
func (rf *Raft) persist(withSnapshot bool) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(atomic.LoadInt64(&rf.currentTerm))
	e.Encode(atomic.LoadInt64(&rf.votedFor))
	e.Encode(atomic.LoadInt64(&rf.commitIndex))
	// e.Encode(atomic.LoadInt64(&rf.lastApplied))
	e.Encode(rf.logFirstIndex)
	e.Encode(rf.lastIncludedTerm)
	// e.Encode(rf.snapshotData)
	e.Encode(rf.log)
	data := w.Bytes()
	if withSnapshot {
		rf.persister.SaveStateAndSnapshot(data, rf.snapshotData)
	} else {
		rf.persister.SaveRaftState(data)
	}
	// log.WithFields(log.Fields{
		// "term": atomic.LoadInt64(&rf.currentTerm),
	// }).Info(rf.me, " persists successfully")

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int64
	var votedFor int64
	var commitIndex int64
	// var lastApplied int64
	var logFirstIndex int64
	var lastIncludedTerm int64
	// var snapshotData []byte
	var logEntries []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		// d.Decode(&lastApplied) != nil ||
		d.Decode(&logFirstIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		// d.Decode(&snapshotData) != nil ||
		d.Decode(&logEntries) != nil {
		panic("failed to read persistent states")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.commitIndex = commitIndex
		// rf.lastApplied = lastApplied
		rf.logFirstIndex = logFirstIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// rf.snapshotData = snapshotData
		rf.log = logEntries
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	currentCommitIndex := atomic.LoadInt64(&rf.commitIndex)
	// Is this possible? Since sendApplyMsg only sends committed entries.
	if (int64)(index) > currentCommitIndex {
		log.WithFields(log.Fields{
			"LastIncludedIndex": index,
			"commitIndex": currentCommitIndex,
		}).Error(rf.me, " the service snapshots too much")
		return
		// panic("the service snapshots too much")
	}

	rf.logMutex.Lock()
	if (int64)(index) < rf.logFirstIndex {
		log.Info(rf.me, " Snapshot: index is smaller than logFirstIndex then no need to snapshot")
		rf.logMutex.Unlock()
		return
	}


	log.WithFields(log.Fields{
		"LastIncludedIndex": index,
		"logFirstIndex": rf.logFirstIndex,
	}).Info(rf.me, " Snapshot: ")
	rf.lastIncludedTerm = rf.log[index - (int)(rf.logFirstIndex)].Term
	rf.snapshotData = snapshot
	if index+1 >= len(rf.log)+(int)(rf.logFirstIndex) {
		rf.log = make([]LogEntry, 0)
	} else {
		tmp := make([]LogEntry, len(rf.log)+(int)(rf.logFirstIndex)-index-1)
		copy(tmp, rf.log[index+1-(int)(rf.logFirstIndex):])
		rf.log = tmp
	}
	rf.logFirstIndex = (int64)(index) + 1
	rf.persist(true)
	log.WithFields(log.Fields{
	"term": atomic.LoadInt64(&rf.currentTerm),
	}).Info(rf.me, " has lastIncludedTerm ", rf.lastIncludedTerm)
	rf.logMutex.Unlock()

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int64
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

func (rf *Raft) cancelElectionOrNoNeed() {
	rf.cancelMutex.Lock()
	defer rf.cancelMutex.Unlock()
	if rf.cancelElection != nil {
		rf.cancelElection()
		rf.cancelElection = nil
		// atomic.StorePointer(&unsafe.Pointer(&rf.cancelElection), nil)
		// log.WithFields(log.Fields{
		// "term": atomic.LoadInt64(&rf.currentTerm),
		// }).Info(rf.me, " canceled election")
	}
}

func (rf *Raft) lastTerm() int64 {
	term := rf.lastIncludedTerm
	if len(rf.log) > 0 {
		term = rf.log[len(rf.log) - 1].Term
	}
	return term
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	prevTerm := atomic.LoadInt64(&rf.currentTerm)
	reply.Term = prevTerm

	log.WithFields(log.Fields{
		"term": prevTerm,
	}).Info(rf.me, " received vote request from ", args.CandidateId)

	if args.Term < prevTerm {
		log.WithFields(log.Fields{
			"term": prevTerm,
		}).Info(rf.me, " refused to vote for ", args.CandidateId, " due to outdated term")
		reply.VoteGranted = false
		return
	}

	// When candidate's term is greater than mine, set my term equal to it
	if args.Term > prevTerm {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		log.WithFields(log.Fields{
			"term": prevTerm,
		}).Info(rf.me, " RequestVote: update term to ", args.Term)
		rf.becomeFollower()
	}


	rf.logMutex.RLock()
	defer rf.logMutex.RUnlock()
	whoIVotedFor := atomic.LoadInt64(&rf.votedFor)
	if whoIVotedFor == -1 || whoIVotedFor == args.CandidateId {
		if (args.LastLogTerm == rf.lastTerm() && args.LastLogIndex+1 >= (int64)(len(rf.log))+rf.logFirstIndex) || (args.LastLogTerm > rf.lastTerm()) {
			log.WithFields(log.Fields{
				"term": prevTerm,
			}).Info(rf.me, " the candidate has lastLogTerm: ", args.LastLogTerm)

			if args.CandidateId != rf.me {
				rf.becomeFollower()
			}
			reply.VoteGranted = true
			atomic.StoreInt64(&rf.votedFor, args.CandidateId)
			rf.persist(false)

			rf.cancelElectionOrNoNeed()
			log.WithFields(log.Fields{
				"term": prevTerm,
			}).Info(rf.me, " voted for ", args.CandidateId)
			return
		} else {
			log.WithFields(log.Fields{
				"term": atomic.LoadInt64(&rf.currentTerm),
			}).Info(rf.me, " refused to vote for ", args.CandidateId, " since log does not match")
		}
	} else {
		log.WithFields(log.Fields{
			"term":        atomic.LoadInt64(&rf.currentTerm),
			// "commitIndex": atomic.LoadInt64(&rf.commitIndex),
		}).Info(rf.me, " refused to vote for ", args.CandidateId, " due to having voted for ", whoIVotedFor)
	}
	reply.VoteGranted = false
	// if args.Term > prevTerm {
		// rf.persist()
	// }
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) collectVotes() (err error) {
	cv := make(chan RequestVoteReply)
	stopCh := make(chan struct{})
	var term int64 = atomic.LoadInt64(&rf.currentTerm)
	rf.logMutex.RLock()
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	// log.WithFields(log.Fields{
	// "term": term,
	// }).Info(rf.me, " tried to lock log")
	args := RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: (int64)(len(rf.log)) - 1 + rf.logFirstIndex, LastLogTerm: lastLogTerm}
	// log.WithFields(log.Fields{
		// "term": term,
	// }).Info(rf.me, " tried to win election with log: ", rf.log)
	rf.logMutex.RUnlock()
	// log.WithFields(log.Fields{
	// "term": term,
	// }).Info(rf.me, " unlocked log")
	for i := 0; i < len(rf.peers); i++ {
		if (int64)(i) == rf.me {
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				select {
				case cv <- reply:
				case <-stopCh:
				}
			}
			// log.Info(i, " requestvote goroutine finished")
		}(i)
	}
	log.WithFields(log.Fields{
		"term": term,
	}).Info(rf.me, " requested votes")
	var reply RequestVoteReply
	var votesCount int
	ctx, cancel := context.WithTimeout(context.Background(), CollectVotesTimeout)
	defer cancel()
	defer close(stopCh)
	for {
		select {
		case reply = <-cv:
			// rf.termMutex.Lock()
			if reply.Term > term {
				rf.cancelElectionOrNoNeed()
				atomic.StoreInt64(&rf.currentTerm, reply.Term)
				log.WithFields(log.Fields{
					"term": term,
				}).Info(rf.me, " CollectVotes: update term to ", reply.Term)
				// log.WithFields(log.Fields{
				// "term": term,
				// }).Info(rf.me, " prepares to lock log mutex when updating term")
				// rf.logMutex.RLock()
				// log.WithFields(log.Fields{
				// "term": term,
				// }).Info(rf.me, " locks log mutex when updating term")
				rf.becomeFollower()
				// rf.persist()
				// rf.logMutex.RUnlock()
				// log.WithFields(log.Fields{
				// "term": term,
				// }).Info(rf.me, " unlocks log mutex when updating term")
				return errors.New("larger term")
			}
			// rf.termMutex.Unlock()
			if reply.VoteGranted {
				votesCount++
				log.WithFields(log.Fields{
					"term": atomic.LoadInt64(&rf.currentTerm),
				}).Info(rf.me, " has ", votesCount, " votes granted now")
			}
			if (votesCount+1)*2 > len(rf.peers) {
				if rf.becomeLeader() {
					for i := range rf.peers {
						if (int64)(i) == rf.me {
							continue
						}
						go rf.sendEntries(i)
					}
				}
				return nil
			}
		case <-ctx.Done():
			log.WithFields(log.Fields{
				"term": atomic.LoadInt64(&rf.currentTerm),
			}).Info(rf.me, " have been collecting votes for too long")
			return errors.New("timeout")
		}
	}
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term         int64
	Success      bool
	NeedPrevLog  bool
	NeedLogIndex int64
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// log.WithFields(log.Fields{
		// "term":        atomic.LoadInt64(&rf.currentTerm),
		// "commitIndex": atomic.LoadInt64(&rf.commitIndex),
	// }).Info(rf.me, " receives append entries from ", args.LeaderId)
	// log.Info(rf.me, args)

	needPersist := false
	defer func() {
		if needPersist {
			rf.logMutex.RLock()
			rf.persist(false)
			rf.logMutex.RUnlock()
		}
	}()
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	reply.Success = true
	reply.NeedPrevLog = false
	if args.Term < reply.Term {
		reply.Success = false
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " appends nothing since the leader has outdated term: ", args.Term)
		return
	} else {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		if args.Term > reply.Term {
			log.WithFields(log.Fields{
				"term": reply.Term,
			}).Info(rf.me, " AppendEntries: update term to ", args.Term)
		}

		needPersist = true
	}

	prevCommitIndex := atomic.LoadInt64(&rf.commitIndex)
	// if two leaders met
	if atomic.LoadInt32((*int32)(&rf.role)) != (int32)(Follower) && prevCommitIndex <= args.LeaderCommit {
		rf.becomeFollower()
		atomic.StoreInt64(&rf.votedFor, args.LeaderId)
		needPersist = true
	}

	if args.LeaderId == atomic.LoadInt64(&rf.votedFor) {
		rf.cancelElectionOrNoNeed()
	}

	rf.logMutex.RLock()
	defer rf.logMutex.RUnlock()

	if (int64)(len(rf.log)-1)+rf.logFirstIndex < args.PrevLogIndex {
		reply.Success = false
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " appends nothing due to intermediate entries absent")
		reply.NeedPrevLog = true
		reply.NeedLogIndex = prevCommitIndex + 1
		return
	}

	myPrevLogTerm := rf.lastIncludedTerm
	if args.PrevLogIndex - rf.logFirstIndex >= 0 {
		myPrevLogTerm = rf.log[args.PrevLogIndex-rf.logFirstIndex].Term
	}
	if myPrevLogTerm != args.PrevLogTerm {
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " has different prevLogTerm from leader's")
		reply.Success = false
		reply.NeedPrevLog = true
		reply.NeedLogIndex = prevCommitIndex + 1
		return
	}
	rf.logMutex.RUnlock()


	rf.logMutex.Lock()


	appendStartIndex := -1
	for i, v := range args.Entries {
		if (int64)(i) + args.PrevLogIndex + 1 < rf.logFirstIndex {
			continue
		}
		if (int64)(i)+args.PrevLogIndex+1 > (int64)(len(rf.log))-1+rf.logFirstIndex {
			appendStartIndex = i
			break
		}
		// What if len(rf.log) == 0?
		if rf.log[i+(int)(args.PrevLogIndex)+1-(int)(rf.logFirstIndex)].Term != v.Term {
			rf.log = rf.log[:i+(int)(args.PrevLogIndex)+1-(int)(rf.logFirstIndex)]
			appendStartIndex = i
			break
		}
	}

	if appendStartIndex >= 0 {
		// I don't understand why slicing args.Entries involes a write and causes data race.
		// So it's the slice expanding causing the problem.
		// Well making a copy of entries on the sender side works.
		rf.log = append(rf.log, args.Entries[appendStartIndex:]...)
		needPersist = true
	}
	rf.logMutex.Unlock()
	rf.logMutex.RLock()


	if args.LeaderCommit > prevCommitIndex {
		rf.commitIndexMutex.Lock()
		if args.LeaderCommit <= atomic.LoadInt64(&rf.commitIndex) {
			rf.commitIndexMutex.Unlock()
			return
		}

		if args.LeaderCommit < (int64)(len(rf.log)-1)+rf.logFirstIndex {
			atomic.StoreInt64(&rf.commitIndex, args.LeaderCommit)
		} else {
			atomic.StoreInt64(&rf.commitIndex, (int64)(len(rf.log)-1)+rf.logFirstIndex)
		}
		needPersist = true
		// currentCommitIndex := atomic.LoadInt64(&rf.commitIndex)
		rf.commitIndexMutex.Unlock()
		rf.logMutex.RUnlock()
		go rf.sendApplyMsg()
		rf.logMutex.RLock()
		// log.WithFields(log.Fields{
			// "term":   atomic.LoadInt64(&rf.currentTerm),
		// }).Info(rf.me, " updates commitIndex to ", currentCommitIndex)
	}


	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


type InstallSnapshotArgs struct {
	Term int64
	LeaderId int64
	LastIncludedTerm int64
	LastIncludedIndex int64
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int64
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	log.WithFields(log.Fields{
		"term":        atomic.LoadInt64(&rf.currentTerm),
	}).Info(rf.me, " receives InstallSnapshot rpc from ", args.LeaderId)

	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	reply.Success = true
	if args.Term < reply.Term {
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " installs nothing since the leader has outdated term")
		reply.Success = false
		return
	} else {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		if args.Term > reply.Term {
			log.WithFields(log.Fields{
				"term": reply.Term,
			}).Info(rf.me, " update term to ", args.Term)
		}
	}

	rf.logMutex.Lock()
	rf.commitIndexMutex.Lock()
	if args.LastIncludedIndex <= atomic.LoadInt64(&rf.commitIndex) {
		log.WithFields(log.Fields{
			"term":        atomic.LoadInt64(&rf.currentTerm),
			"lastIncludedIndex": args.LastIncludedIndex,
			"commitIndex": atomic.LoadInt64(&rf.commitIndex),
		}).Info(rf.me, " InstallSnapshot: lastIncludedIndex is smaller than current commit index")
		rf.commitIndexMutex.Unlock()
		rf.logMutex.Unlock()
		return
	}
	atomic.StoreInt64(&rf.commitIndex, args.LastIncludedIndex)
	rf.commitIndexMutex.Unlock()
	// CommitIndex may be larger than total length of log by now.
	// Use log mutex to protect log.




	logLength := (int64)(len(rf.log)) + rf.logFirstIndex

	// update raft
	rf.logFirstIndex = args.LastIncludedIndex + 1
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshotData = args.Data
	// log.WithFields(log.Fields{
		// "term":   atomic.LoadInt64(&rf.currentTerm),
		// // "commit": atomic.LoadInt64(&rf.commitIndex),
	// }).Info(rf.me, " updates commitIndex to ", currentCommitIndex)
	// How can this happen? NextIndex may not be up-to-date.
	if logLength >= args.LastIncludedIndex + 1 && args.LastIncludedIndex >= rf.logFirstIndex {
		if rf.log[args.LastIncludedIndex - rf.logFirstIndex].Term == args.LastIncludedTerm {
			rf.log = append(make([]LogEntry, logLength - args.LastIncludedIndex - 1), rf.log[args.LastIncludedIndex + 1 - rf.logFirstIndex:]...)
			rf.logMutex.Unlock()

			rf.logMutex.RLock()
			rf.persist(true)
			rf.logMutex.RUnlock()
			go rf.sendApplyMsg()
			return
		} 
	}
	rf.log = make([]LogEntry, 0)
	rf.logMutex.Unlock()

	rf.logMutex.RLock()

	rf.persist(true)
	rf.logMutex.RUnlock()
	go rf.sendApplyMsg()
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}



//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	isLeader := atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Leader)
	if isLeader {
		rf.logMutex.Lock()
		rf.log = append(rf.log, LogEntry{Command: command, Term: atomic.LoadInt64(&rf.currentTerm)})
		// currentCommitIndex := atomic.LoadInt64(&rf.commitIndex)
		rf.persist(false)
		index = len(rf.log) - 1 + (int)(rf.logFirstIndex)
		// logLength := (int64)(len(rf.log)) + rf.logFirstIndex
		rf.logMutex.Unlock()
		for i := range rf.peers {
			if (int64)(i) == rf.me {
				continue
			}
			go rf.sendEntriesToPeer(i, nil)
		}
	}

	// Your code here (2B).

	return index, (int)(atomic.LoadInt64(&rf.currentTerm)), isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		sleeptime := ElectionTimerBase + (time.Duration)(rand.Int63n(ElectionTimerRandom.Milliseconds())) * time.Millisecond
		// log.WithFields(log.Fields{
		// "term": atomic.LoadInt64(&rf.currentTerm),
		// }).Info(rf.me, " will start election in ", sleeptime, " milliseconds")
		ctx, cancel := context.WithCancel(context.Background())
		rf.cancelMutex.Lock()
		rf.cancelElection = cancel
		rf.cancelMutex.Unlock()
		select {
		case <-time.After(sleeptime):
			if atomic.LoadInt32((*int32)(&rf.role)) != (int32)(Leader) {
				rf.becomeCandidate()
				go rf.collectVotes()
			}
		case <-ctx.Done():
			continue
		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// idemponent.
func (rf *Raft) sendEntriesToPeer(peer int, replyCh chan bool) {
	// send entries
	rf.followerIndexMutex.RLock()
	nextIndex := atomic.LoadInt64(&rf.nextIndex[peer])
	rf.followerIndexMutex.RUnlock()

	rf.logMutex.RLock()

	logLength := (int64)(len(rf.log)) + rf.logFirstIndex
	term := atomic.LoadInt64(&rf.currentTerm)
	leaderCommit := atomic.LoadInt64(&rf.commitIndex)

	if nextIndex < rf.logFirstIndex {
		var args InstallSnapshotArgs
		var reply InstallSnapshotReply
		args = InstallSnapshotArgs{Term: term, LeaderId: rf.me, LastIncludedIndex: rf.logFirstIndex - 1, LastIncludedTerm: rf.lastIncludedTerm, Data: rf.snapshotData, Done: true}
		rf.logMutex.RUnlock()
		ok := rf.sendInstallSnapshot(peer, &args, &reply)
		if replyCh != nil {
			replyCh <- ok
		}
		rf.logMutex.RLock()
		if ok {
			if reply.Term > term {
				atomic.StoreInt64(&rf.currentTerm, reply.Term)
				log.WithFields(log.Fields{
					"term": term,
				}).Info(rf.me, " BroadcastEntries: update term to ", reply.Term)
				rf.becomeFollower()
			} else {
				rf.followerIndexMutex.Lock()
				if reply.Success {
					// log.Info(args)
					atomic.StoreInt64(&rf.nextIndex[peer], rf.logFirstIndex)
					atomic.StoreInt64(&rf.matchIndex[peer], rf.logFirstIndex - 1)
					// break
				} 
				rf.followerIndexMutex.Unlock()
				if reply.Success {
					rf.updateLeaderCommit()
				}

			}
		}
		rf.logMutex.RUnlock()
		if ok && reply.Success {
			go rf.sendApplyMsg()
		}
	} else {
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		// I become follower and log is sliced
		if nextIndex > (int64)(len(rf.log))+rf.logFirstIndex {
			rf.logMutex.RUnlock()
			return
		}
		prevLogTerm := rf.lastIncludedTerm
		if nextIndex - 1 - rf.logFirstIndex >= 0 {
			prevLogTerm = rf.log[nextIndex-1-rf.logFirstIndex].Term
		}
		if logLength < nextIndex+1 {
			args = AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: nextIndex - 1, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: leaderCommit}
		} else {
			// making a copy to avoid data race. Though it is probably unnecessary if peers are not on the same machine.
			args = AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: nextIndex - 1, PrevLogTerm: prevLogTerm, Entries: append([]LogEntry(nil), rf.log[nextIndex-rf.logFirstIndex : logLength-rf.logFirstIndex]...), LeaderCommit: leaderCommit}
		}
		rf.logMutex.RUnlock()
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if replyCh != nil {
			replyCh <- ok
		}
		if ok {
			if reply.Term > term {
				atomic.StoreInt64(&rf.currentTerm, reply.Term)
				rf.becomeFollower()
			} else {
				rf.followerIndexMutex.Lock()
				if reply.Success {
					atomic.StoreInt64(&rf.nextIndex[peer], logLength)
					atomic.StoreInt64(&rf.matchIndex[peer], logLength-1)
				} else {
					if reply.NeedPrevLog && reply.NeedLogIndex > 0 {
						atomic.StoreInt64(&rf.nextIndex[peer], reply.NeedLogIndex)
					}
				}
				rf.followerIndexMutex.Unlock()
				if reply.Success {
					rf.logMutex.RLock()
					rf.updateLeaderCommit()
					rf.logMutex.RUnlock()
					go rf.sendApplyMsg()
				}
			}
		}
	}
}


func (rf *Raft) sendEntries(peer int) {
	for atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Leader) {
		rf.logMutex.RLock()
		logLength := (int64)(len(rf.log)) + rf.logFirstIndex
		rf.logMutex.RUnlock()
		rf.cancelElectionOrNoNeed()

		replyCh := make(chan bool)
		ctx, cancel := context.WithTimeout(context.Background(), AppendEntriesRPCTimeout)
		go rf.sendEntriesToPeer(peer, replyCh)
		select {
		case ok := <-replyCh:
			// Connect is not good. Wait for a while
			if !ok {
				time.Sleep(AppendEntriesFailWaitTime)
			}
		case <-ctx.Done():
		}
		cancel()

		// If there is nothing to send, slow down a bit
		rf.followerIndexMutex.RLock()
		if rf.nextIndex[peer] >= logLength {
			rf.followerIndexMutex.RUnlock()
			time.Sleep(AppendEntriesIdleWaitTime)
			// ctx, cancel := context.WithTimeout(context.Background(), AppendEntriesRPCTimeout)
			// select {
			// case <-rf.senderNotifier[peer]:
			// case <-ctx.Done():
			// }
			// cancel()
			continue
		}
		rf.followerIndexMutex.RUnlock()
	}
}


// Need logMutex Rlock
func (rf *Raft) updateLeaderCommit() {
	logLength := (int64)(len(rf.log)) + rf.logFirstIndex
	prevCommitIndex := atomic.LoadInt64(&rf.commitIndex)
	for i := int64(logLength - 1); i > prevCommitIndex; i-- {
		countMatch := 0
		rf.followerIndexMutex.RLock()
		for j := 0; j < len(rf.peers); j++ {
			if (int64)(j) == rf.me {
				continue
			}
			if atomic.LoadInt64(&rf.matchIndex[j]) >= i {
				countMatch++
			}
		}
		rf.followerIndexMutex.RUnlock()
		if atomic.LoadInt32((*int32)(&rf.role)) != (int32)(Leader) {
			return
		}
		// Add leader itself
		if (countMatch + 1) * 2 > len(rf.peers) {
			// rf.logMutex.RLock()
			if atomic.LoadInt64(&rf.currentTerm) == rf.log[i-rf.logFirstIndex].Term {
				rf.commitIndexMutex.Lock()
				atomic.StoreInt64(&rf.commitIndex, i)
				rf.commitIndexMutex.Unlock()
				rf.persist(false)
				// log.WithFields(log.Fields{
					// "term": atomic.LoadInt64(&rf.currentTerm),
				// }).Info(rf.me, " commits up to ", i, " as the leader")
			}
			// rf.logMutex.RUnlock()
			break
		}
	}
}



func (rf *Raft) sendApplyMsg() {
	rf.applyMsgMutex.Lock()
	defer rf.applyMsgMutex.Unlock()

	// Send msg only when lastApplied < commitIndex
	currentCommitIndex := atomic.LoadInt64(&rf.commitIndex)
	if rf.lastApplied >= currentCommitIndex {
		return
	}

	// Send snapshot or log entries? Determined by lastApplied and logFirstIndex
	// logFirstIndex is protected by logMutex
	// Should release logMutex before using applyCh. Otherwise it will block when kvserver calls snapshot.
	rf.logMutex.RLock()
	if rf.lastApplied + 1 < rf.logFirstIndex {
		// Send snapshot
		msg := ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: rf.snapshotData, SnapshotIndex: (int)(rf.logFirstIndex - 1), SnapshotTerm: (int)(rf.lastIncludedTerm)}
		rf.lastApplied = rf.logFirstIndex - 1
		rf.logMutex.RUnlock()
		rf.applyCh <- msg
	} else {
		// Send log entries from lastApplied + 1 to commitIndex
		msgs := make([]ApplyMsg, currentCommitIndex - rf.lastApplied)
		for i := range msgs {
			msgs[i] = ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied + (int64)(i) + 1 - rf.logFirstIndex].Command, CommandIndex: (int)(rf.lastApplied + (int64)(i) + 1), CommandTerm: rf.log[rf.lastApplied + (int64)(i) + 1 - rf.logFirstIndex].Term}
		}
		rf.logMutex.RUnlock()
		for i := range msgs {
			rf.applyCh <- msgs[i]
			rf.lastApplied++
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = (int64)(me)
	rf.votedFor = -1
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.lastApplied = 0
	rf.role = Follower
	rf.commitIndex = 0
	rf.log = append(rf.log, LogEntry{Command: 0, Term: 0})
	rf.logFirstIndex = 0
	rf.lastIncludedTerm = 0
	// rf.snapshotData = make([]byte, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshotData = persister.ReadSnapshot()

	

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
