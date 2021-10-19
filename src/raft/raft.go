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
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"errors"
	"math/rand"
	"time"
	"context"
	log "github.com/sirupsen/logrus"
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
	Content string
	Term    int64
}

type RaftRole int32

const (
	Follower RaftRole = iota
	Candidate
	Leader
)

const (
	CollectVotesTimeout time.Duration = 1000 * time.Millisecond //1s
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int64
	termMutex   sync.Mutex
	votedFor    int64
	log         []LogEntry
	logMutex    sync.Mutex

	// Volatile state
	commitIndex int
	lastApplied int

	// Volatile state for leader
	nextIndex  []int
	matchIndex []int

	//
	role          RaftRole
	cancelElection context.CancelFunc
	cancelMutex    sync.Mutex

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
	log.WithFields(log.Fields{
		"term": atomic.LoadInt64(&rf.currentTerm),
	}).Info(rf.me, " became a follower")
}

func (rf *Raft) becomeLeader() bool {
	if !atomic.CompareAndSwapInt32((*int32)(&rf.role), (int32)(Candidate), (int32)(Leader)) {
		return false
	}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.logMutex.Lock()
	logLength := len(rf.log)
	rf.logMutex.Unlock()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = logLength
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	log.WithFields(log.Fields{
		"term": atomic.LoadInt64(&rf.currentTerm),
	}).Info(rf.me, " became the leader")
	return true
}

func (rf *Raft) becomeCandidate() {
	atomic.StoreInt32((*int32)(&rf.role), (int32)(Candidate))
	// rf.termMutex.Lock()
	// defer rf.termMutex.Unlock()
	term := atomic.LoadInt64(&rf.currentTerm)
	atomic.StoreInt64(&rf.currentTerm, term + 1) 
	atomic.StoreInt64(&rf.votedFor, (int64)(rf.me))
	log.WithFields(log.Fields{
		"term": atomic.LoadInt64(&rf.currentTerm),
	}).Info(rf.me, " became a candidate")
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int64
	LastLogIndex int
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
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " canceled election")
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// rf.termMutex.Lock()
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	log.WithFields(log.Fields{
		"term": reply.Term,
	}).Info(rf.me, " received vote request from ", args.CandidateId)
	if args.Term < reply.Term {
		log.WithFields(log.Fields{
			"term": reply.Term,
		}).Info(rf.me, " refused to vote for ", args.CandidateId, " due to outdated term")
		reply.VoteGranted = false
		return
	} else if args.Term > reply.Term {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		if atomic.LoadInt32((*int32)(&rf.role)) != (int32)(Leader) {
			rf.becomeFollower()
		} 

	}
	// rf.termMutex.Unlock()


	whoIVotedFor := atomic.LoadInt64(&rf.votedFor)
	if whoIVotedFor == -1 || whoIVotedFor == args.CandidateId {
		rf.logMutex.Lock()
		defer rf.logMutex.Unlock()
		if (len(rf.log) == 0 && ((args.LastLogTerm == 0 && args.LastLogIndex == -1) || (args.LastLogTerm > 0))) || (len(rf.log) > 0 && ((args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)) || (args.LastLogTerm > rf.log[len(rf.log)-1].Term))) {
			if args.CandidateId != rf.me {
				rf.becomeFollower()
			}
			reply.VoteGranted = true
			atomic.StoreInt64(&rf.votedFor, args.CandidateId)

			rf.cancelElectionOrNoNeed()
			log.WithFields(log.Fields{
				"term": reply.Term,
			}).Info(rf.me, " voted for ", args.CandidateId)
			return
		} else {
			log.WithFields(log.Fields{
				"term": atomic.LoadInt64(&rf.currentTerm),
			}).Info(rf.me, " refused to vote for ", args.CandidateId, " since log does not match")
		}
	} else {
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " refused to vote for ", args.CandidateId, " due to having voted for ", whoIVotedFor)
	}
	reply.VoteGranted = false
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
	var lastLogTerm int64 = 0
	var term int64 = atomic.LoadInt64(&rf.currentTerm)
	// log.WithFields(log.Fields{
		// "term": term,
	// }).Info(rf.me, " tried to lock log")
	rf.logMutex.Lock()
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args := RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: lastLogTerm}
	rf.logMutex.Unlock()
	// log.WithFields(log.Fields{
		// "term": term,
	// }).Info(rf.me, " unlocked log")
	for i := 0; i < len(rf.peers); i++ {
		go func(i int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				cv <- reply
			}
		}(i)
	}
	log.WithFields(log.Fields{
		"term": term,
	}).Info(rf.me, " requested votes")
	var reply RequestVoteReply
	var votesCount int
	ctx, cancel := context.WithTimeout(context.Background(), CollectVotesTimeout)
	defer cancel()
	for {
		select {
		case reply = <-cv:
			// rf.termMutex.Lock()
			if reply.Term > term {
				rf.becomeFollower()
				atomic.StoreInt64(&rf.currentTerm, reply.Term)
				return errors.New("larger term")
			}
			// rf.termMutex.Unlock()
			if reply.VoteGranted {
				votesCount++
				log.WithFields(log.Fields{
					"term": atomic.LoadInt64(&rf.currentTerm),
				}).Info(rf.me, " has ", votesCount, " votes granted now")
			}
			if votesCount * 2 > len(rf.peers) {
				if rf.becomeLeader() {
					go rf.talkPeriodically()
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
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Check term
	// rf.termMutex.Lock()
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	if args.Term < reply.Term {
		reply.Success = false
		return
	} else {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		if args.LeaderId != rf.me {
			rf.becomeFollower()
		}
		atomic.StoreInt64(&rf.votedFor, args.LeaderId)
	}
	// rf.termMutex.Unlock()

	if args.LeaderId == atomic.LoadInt64(&rf.votedFor) {
		rf.cancelElectionOrNoNeed()
	}

	// initial empty heartbeat
	if args.PrevLogIndex < 0 {
		reply.Success = true
		return
	}

	rf.logMutex.Lock()
	defer rf.logMutex.Unlock()
	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	for i := args.PrevLogIndex + 1; i < len(rf.log); i++ {
		if rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:i]
			break
		}
	}

	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
		sleeptime := 100+rand.Int63n(500)
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " will start election in ", sleeptime, " milliseconds")
		ctx, cancel := context.WithCancel(context.Background())
		rf.cancelMutex.Lock()
		rf.cancelElection = cancel
		rf.cancelMutex.Unlock()
		select {
		case <-time.After((time.Duration)(sleeptime) * time.Millisecond):
			// if atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Follower) {
			rf.becomeCandidate()
			// }
			go rf.collectVotes()
		case <-ctx.Done():
			continue
		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

func (rf *Raft) talkPeriodically() {
	for atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Leader) {
		rf.logMutex.Lock()
		logLength := len(rf.log)
		rf.logMutex.Unlock()
		term := atomic.LoadInt64(&rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			go func(i int, l int) {
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				if l < rf.nextIndex[i]+1 {
					args = AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: -1, PrevLogTerm: 0, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
					rf.sendAppendEntries(i, &args, &reply)
					return
				}

				ok := false
				for !ok {
					args = AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1, PrevLogTerm: rf.log[rf.nextIndex[i]-1].Term, Entries: rf.log[rf.nextIndex[i]:l], LeaderCommit: rf.commitIndex}
					ok := rf.sendAppendEntries(i, &args, &reply)
					if ok {
						rf.nextIndex[i] = l
						rf.matchIndex[i] = rf.nextIndex[i] - 1
						break
					} else {
						rf.nextIndex[i]--
					}
				}
			}(i, logLength)
		}
		for i := logLength - 1; i > rf.commitIndex; i++ {
			countMatch := 0
			for _, v := range rf.matchIndex {
				if v >= i {
					countMatch++
				}
			}
			if countMatch * 2 > len(rf.peers) && rf.log[i].Term == term {
				rf.commitIndex = i
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.lastApplied = -1
	rf.role = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
