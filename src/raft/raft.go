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
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
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
	CollectVotesTimeout time.Duration = 1000 * time.Millisecond //1s
	ElectionTimerBase time.Duration = 200 * time.Millisecond
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
	currentTerm int64
	votedFor    int64
	log         []LogEntry
	logMutex    sync.Mutex

	// Volatile state
	commitIndex int64
	commitIndexMutex sync.Mutex
	lastApplied int64

	// Volatile state for leader
	nextIndex  []int64
	matchIndex []int64
	followerIndexMutex sync.Mutex

	//
	role           RaftRole
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
	// log.WithFields(log.Fields{
	// "term": atomic.LoadInt64(&rf.currentTerm),
	// }).Info(rf.me, " became a follower")
}

func (rf *Raft) becomeLeader() bool {
	if !atomic.CompareAndSwapInt32((*int32)(&rf.role), (int32)(Candidate), (int32)(Leader)) {
		return false
	}
	rf.logMutex.Lock()
	logLength := (int64)(len(rf.log))
	rf.logMutex.Unlock()
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
	atomic.StoreInt64(&rf.currentTerm, term+1)
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
	CommitIndex  int64
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


		// If I don't become follower then everyone will just vote for itself and nobody can get elected.
		// Nonono, whoever has higher term will get elected since the candidate of lower term becomes follower after receiving response
		// if atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Candidate) {
			// rf.becomeFollower()
		// }

		// When increasing term, reset votedFor to null
		if atomic.LoadInt32((*int32)(&rf.role)) != (int32)(Leader) {
			rf.becomeFollower()
		} //else {
			// reply.VoteGranted = false
			// return
		// }


	}

	// If there are only two peers remaining online, one established leader with lower commitIndex, the other candidate with higher commitIndex, then I'd like to pick the one with higher commitIndex as leader
	// Suppose now the candidate requests vote from the leader
	// if atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Leader) && args.CommitIndex > atomic.LoadInt64(&rf.commitIndex) {
		// rf.becomeFollower()
	// }


	// rf.termMutex.Unlock()
	// if atomic.LoadInt64(&rf.commitIndex) < args.CommitIndex {
		// rf.becomeFollower()
	// }

	whoIVotedFor := atomic.LoadInt64(&rf.votedFor)
	if whoIVotedFor == -1 || whoIVotedFor == args.CandidateId {
		rf.logMutex.Lock()
		defer rf.logMutex.Unlock()
		if (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex + 1 >= len(rf.log)) || (args.LastLogTerm > rf.log[len(rf.log)-1].Term) {
			if args.CandidateId != rf.me {
				rf.becomeFollower()
			}
			reply.VoteGranted = true
			atomic.StoreInt64(&rf.votedFor, args.CandidateId)

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
			"term": atomic.LoadInt64(&rf.currentTerm),
			"commitIndex": atomic.LoadInt64(&rf.commitIndex),
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
	var term int64 = atomic.LoadInt64(&rf.currentTerm)
	rf.logMutex.Lock()
	lastLogTerm := rf.log[len(rf.log)-1].Term
	// log.WithFields(log.Fields{
	// "term": term,
	// }).Info(rf.me, " tried to lock log")
	args := RequestVoteArgs{Term: term, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: lastLogTerm, CommitIndex: atomic.LoadInt64(&rf.commitIndex)}
	rf.logMutex.Unlock()
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
				rf.cancelElectionOrNoNeed()
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
			if (votesCount + 1) * 2 > len(rf.peers) {
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
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
	NeedPrevLog bool
	NeedLogIndex int64
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.WithFields(log.Fields{
		"term": atomic.LoadInt64(&rf.currentTerm),
		"commitIndex": atomic.LoadInt64(&rf.commitIndex),
	}).Info(rf.me, " receives append entries from ", args.LeaderId)
	log.Info(args)

	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	reply.Success = true
	reply.NeedPrevLog = false
	if args.Term < reply.Term {
		reply.Success = false
		log.WithFields(log.Fields{
		"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " appends nothing since the leader has outdated term")
		return
	} else {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
	}


	rf.commitIndexMutex.Lock()
	// log.WithFields(log.Fields{
	// "term": atomic.LoadInt64(&rf.currentTerm),
	// }).Info(rf.me, " after commitIndexMutex locked")
	defer rf.commitIndexMutex.Unlock()
	prevCommitIndex := atomic.LoadInt64(&rf.commitIndex)
	// if two leaders met
	if atomic.LoadInt32((*int32)(&rf.role)) != (int32)(Follower) && atomic.LoadInt64(&rf.commitIndex) <= args.LeaderCommit {
		rf.becomeFollower()
		atomic.StoreInt64(&rf.votedFor, args.LeaderId)
	}

	if args.LeaderId == atomic.LoadInt64(&rf.votedFor) {
		rf.cancelElectionOrNoNeed()
	}

	// log.WithFields(log.Fields{
		// "term": atomic.LoadInt64(&rf.currentTerm),
		// "commitIndex": atomic.LoadInt64(&rf.commitIndex),
	// }).Info(rf.me, " receives append entries from ", args.LeaderId)
	// log.Info(args)





	rf.logMutex.Lock()
	defer rf.logMutex.Unlock()
	// log.WithFields(log.Fields{
	// "term": atomic.LoadInt64(&rf.currentTerm),
	// }).Info(rf.me, " after logMutex locked")


	if (int64)(len(rf.log)-1) < args.PrevLogIndex {
		reply.Success = false
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " appends nothing due to intermediate entries absent")
		reply.NeedPrevLog = true
		reply.NeedLogIndex = prevCommitIndex + 1
		return
	}


	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
		}).Info(rf.me, " has different prevLogTerm from leader's")
		reply.Success = false
		reply.NeedPrevLog = true
		reply.NeedLogIndex = prevCommitIndex + 1
		return
	}


	appendStartIndex := -1
	for i, v := range args.Entries {
		if i + (int)(args.PrevLogIndex) + 1 > len(rf.log) - 1 {
			appendStartIndex = i
			break
		}
		if rf.log[i + (int)(args.PrevLogIndex) + 1].Term != v.Term {
			rf.log = rf.log[:i + (int)(args.PrevLogIndex) + 1]
			appendStartIndex = i
			break
		}
	}



	if appendStartIndex >= 0 {
		rf.log = append(rf.log, args.Entries[appendStartIndex:]...)
	}

	if args.LeaderCommit > prevCommitIndex {
		if args.LeaderCommit < (int64)(len(rf.log)-1) {
			atomic.StoreInt64(&rf.commitIndex, args.LeaderCommit)
		} else {
			atomic.StoreInt64(&rf.commitIndex, (int64)(len(rf.log) - 1))
		}
		rf.sendApplyMsg((int)(prevCommitIndex) + 1, (int)(atomic.LoadInt64(&rf.commitIndex) + 1))
		log.WithFields(log.Fields{
			"term": atomic.LoadInt64(&rf.currentTerm),
			"commit": atomic.LoadInt64(&rf.commitIndex),
		}).Info(rf.me, " updates commitIndex to ", atomic.LoadInt64(&rf.commitIndex))
	}

	// log.WithFields(log.Fields{
		// "term": atomic.LoadInt64(&rf.currentTerm),
	// }).Info(rf.me, " perhaps added some entries, not sure ")


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
	isLeader := atomic.LoadInt32((*int32)(&rf.role)) == (int32)(Leader)
	if isLeader {
		rf.logMutex.Lock()
		rf.log = append(rf.log, LogEntry{Command: command, Term: atomic.LoadInt64(&rf.currentTerm)})
		index = len(rf.log) - 1
		rf.logMutex.Unlock()
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
		sleeptime := 100 + rand.Int63n(500)
		// log.WithFields(log.Fields{
		// "term": atomic.LoadInt64(&rf.currentTerm),
		// }).Info(rf.me, " will start election in ", sleeptime, " milliseconds")
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
		logLength := (int64)(len(rf.log))
		// log.WithFields(log.Fields{
			// "term": atomic.LoadInt64(&rf.currentTerm),
		// }).Info(rf.me, " has log of length ", logLength, " as the leader")
		rf.logMutex.Unlock()
		term := atomic.LoadInt64(&rf.currentTerm)
		leaderCommit := atomic.LoadInt64(&rf.commitIndex)

		// imitate sending appendEntries to leader itself
		rf.cancelElectionOrNoNeed()
		rf.followerIndexMutex.Lock()
		rf.nextIndex[rf.me] = logLength
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		rf.followerIndexMutex.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if (int64)(i) == rf.me {
				continue
			}
			go func(i int, logLength int64, leaderCommit int64) {
				// log.WithFields(log.Fields{
					// "term": atomic.LoadInt64(&rf.currentTerm),
				// }).Info(rf.me, " sends AppendEntries to ", i)

				var args AppendEntriesArgs
				var reply AppendEntriesReply
				rf.followerIndexMutex.Lock()
				nextIndex := atomic.LoadInt64(&rf.nextIndex[i])
				rf.followerIndexMutex.Unlock()
				rf.logMutex.Lock()
				prevLogTerm := rf.log[nextIndex-1].Term
				if logLength < nextIndex+1 {
					args = AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: nextIndex - 1, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: leaderCommit}
				} else {
					args = AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: nextIndex - 1, PrevLogTerm: prevLogTerm, Entries: rf.log[nextIndex:logLength], LeaderCommit: leaderCommit}
				}
				rf.logMutex.Unlock()

				// log.WithFields(log.Fields{
					// "term": atomic.LoadInt64(&rf.currentTerm),
				// }).Info(rf.me, " sends non-initial AppendEntries ", i, " as the leader")



				// if len(rf.log[nextIndex:logLength]) > 0 {
					// log.Info("nonzero entries sent", nextIndex, logLength)
				// }


				// time reply. Thus invalidate old leader's requests
				// t1 := time.Now()
				ok := rf.sendAppendEntries(i, &args, &reply)
				// if time.Since(t1) >= ElectionTimerBase / 4 {
					// ok = false
				// }
				// log.WithFields(log.Fields{
					// "term": atomic.LoadInt64(&rf.currentTerm),
					// "nextIndex": logLength,
				// }).Info(rf.me, " succeeds to append ", i, " : ", ok)
				if ok {
					if reply.Term > term {
						atomic.StoreInt64(&rf.currentTerm, reply.Term)
						rf.becomeFollower()
						return
					}
					rf.followerIndexMutex.Lock()
					if reply.Success {
						// log.Info(args)
						atomic.StoreInt64(&rf.nextIndex[i], logLength)
						atomic.StoreInt64(&rf.matchIndex[i], logLength - 1)
						// break
					} else {
						if reply.NeedPrevLog && reply.NeedLogIndex > 0 {
							atomic.StoreInt64(&rf.nextIndex[i], reply.NeedLogIndex)
						}
					}
					rf.followerIndexMutex.Unlock()
				}
			}(i, logLength, leaderCommit)
		}
		prevCommitIndex := atomic.LoadInt64(&rf.commitIndex)
		for i := int64(logLength - 1); i > prevCommitIndex; i-- {
			countMatch := 0
			rf.followerIndexMutex.Lock()
			for j := 0; j < len(rf.peers); j++ {
				if atomic.LoadInt64(&rf.matchIndex[j]) >= i {
					countMatch++
				}
			}
			rf.followerIndexMutex.Unlock()
			if atomic.LoadInt32((*int32)(&rf.role)) != (int32)(Leader) {
				return
			}
			if countMatch*2 > len(rf.peers) {
				atomic.StoreInt64(&rf.commitIndex, i)
				// rf.logMutex.Lock()
				rf.sendApplyMsg((int)(prevCommitIndex) + 1, (int)(i) + 1)
				// rf.logMutex.Unlock()
				log.WithFields(log.Fields{
					"term": atomic.LoadInt64(&rf.currentTerm),
				}).Info(rf.me, " commits up to ", i, " as the leader")
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendApplyMsg(start int, end int) {
	for i := start; i < end; i++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
