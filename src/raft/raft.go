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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/src/labrpc"
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
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int64
	votedFor    int64
	commitIndex int64
	lastApplied int64

	role                   string
	nextVoteTimestamp      int64
	nextHeartbeatTimestamp int64
}

func (rf *Raft) getCurrentTerm() int64 {
	return atomic.LoadInt64(&rf.currentTerm)
}
func (rf *Raft) setTerm(newTerm int64) {
	rf.setVotedFor(-1)
	atomic.StoreInt64(&rf.currentTerm, newTerm)
}
func (rf *Raft) incTerm() {
	rf.setVotedFor(-1)
	atomic.AddInt64(&rf.currentTerm, 1)
}

func (rf *Raft) getVotedFor() int64 {
	return atomic.LoadInt64(&rf.votedFor)
}
func (rf *Raft) setVotedFor(newVote int64) {
	atomic.StoreInt64(&rf.votedFor, newVote)
}

func (rf *Raft) getRole() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}
func (rf *Raft) setRole(newVote string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = newVote
}

const (
	RoleLeader    = "leader"
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
)

func getCurrentTimestampMs() int64 {
	return time.Now().UnixNano() / 1000000
}

func getNextVoteTimeoutTimestamp() int64 {
	return getCurrentTimestampMs() + 100 + rand.Int63n(100)
}

func getNextHeartbeatTimestamp() int64 {
	return getCurrentTimestampMs() + 30
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = int(rf.getCurrentTerm())
	isleader = rf.getRole() == RoleLeader

	return term, isleader
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

type LogEntry struct{}

type AppendEntriesArgs struct {
	Term         int64
	LeaderID     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Your code here (2A, 2B).
	reply.VoteGranted = false
	reply.Term = rf.getCurrentTerm()
	if args.Term < rf.getCurrentTerm() {
		return
	}

	if args.Term > rf.getCurrentTerm() {
		rf.setTerm(args.Term)
		reply.Term = rf.getCurrentTerm()
		rf.tryBecomeFollower(args.Term, int64(args.CandidateId))
	}

	if rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateId {
		if rf.checkIfIncomeLogNotOld(args.LastLogTerm, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.setVotedFor(args.CandidateId)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	reply.Term = rf.getCurrentTerm()
	if args.Term < rf.getCurrentTerm() {
		return
	}

	if args.Term > rf.getCurrentTerm() {
		rf.setTerm(args.Term)
		reply.Term = rf.getCurrentTerm()
		rf.tryBecomeFollower(args.Term, int64(args.LeaderID))
	}

	atomic.StoreInt64(&rf.nextVoteTimestamp, getNextVoteTimeoutTimestamp())

	if rf.getRole() == RoleFollower || rf.getRole() == RoleCandidate {
		reply.Success = true
	}

	// TODO Receiver implementation 2~5 行为逻辑

}

func (rf *Raft) checkIfIncomeLogNotOld(term int64, index int64) bool {
	myLastLogIdx := rf.getLastLogIndex()
	myLastTerm := rf.getLogTermByIndex(myLastLogIdx)
	if term > myLastTerm {
		return true
	} else if term == myLastTerm && index >= myLastLogIdx {
		return true
	}

	return false
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
	DPrintf("%d Send sendRequestVote to %d At term %d\n", rf.me, server, rf.getCurrentTerm())
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("%d Send sendAppendEntries to %d At term %d\n", rf.me, server, rf.getCurrentTerm())
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

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(5 * time.Millisecond)

		if getCurrentTimestampMs() < atomic.LoadInt64(&rf.nextVoteTimestamp) {
			continue
		} else {
			atomic.StoreInt64(&rf.nextVoteTimestamp, getNextVoteTimeoutTimestamp())
		}
		if rf.getRole() == RoleLeader { // Candidate模式下，第一次发出去的选票可能没有结果，所以Candidate超时了也要重新发起选举
			continue
		}

		// 走到这里，说明Follower要开始竞选了
		go rf.doElection()
	}
}

func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		time.Sleep(5 * time.Millisecond)

		if getCurrentTimestampMs() < atomic.LoadInt64(&rf.nextHeartbeatTimestamp) {
			continue
		} else {
			atomic.StoreInt64(&rf.nextHeartbeatTimestamp, getNextHeartbeatTimestamp())
		}
		if rf.getRole() == RoleFollower || rf.getRole() == RoleCandidate {
			continue
		}

		go rf.broadcastHeartbeat()
	}
}

func (rf *Raft) doElection() {
	rf.setRole(RoleCandidate)
	rf.incTerm()
	rf.setVotedFor(int64(rf.me))

	recvVoteCnt := int64(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			args := RequestVoteArgs{
				Term:         rf.getCurrentTerm(),
				CandidateId:  int64(rf.me),
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLogTermByIndex(rf.getLastLogIndex()),
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(idx, &args, &reply)
			if reply.VoteGranted {
				newCnt := atomic.AddInt64(&recvVoteCnt, 1)
				DPrintf("\033[33m%d Recv Vote From %d At term %d\033[0m\n", rf.me, idx, rf.getCurrentTerm())
				if newCnt > int64(len(rf.peers))/2 {
					DPrintf("\033[31m%d Win Election At term %d\033[0m\n", rf.me, rf.getCurrentTerm())
					rf.tryBecomeLeader(reply.Term)
					go rf.broadcastHeartbeat()
				}
			}
		}(i)
	}
}

func (rf *Raft) tryBecomeLeader(term int64) {
	if term < rf.getCurrentTerm() || term > rf.getCurrentTerm() {
		return
	}
	if rf.getRole() == RoleLeader {
		return
	}
	rf.setRole(RoleLeader)
}

func (rf *Raft) tryBecomeFollower(term int64, triggerSrorce int64) {
	if term < rf.getCurrentTerm() || term > rf.getCurrentTerm() {
		return
	}
	if rf.getRole() == RoleFollower {
		return
	}
	if rf.getRole() != RoleFollower {
		DPrintf("\033[31m[***] %d Turn To Follower From State %s At term %d, Because Server %d\033[0m\n", rf.me, rf.getRole(), rf.getCurrentTerm(), triggerSrorce)
		rf.setRole(RoleFollower)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			args := AppendEntriesArgs{
				Term:         rf.getCurrentTerm(),
				LeaderID:     rf.me,
				PrevLogIndex: rf.getLastLogIndex(),
				PrevLogTerm:  rf.getLogTermByIndex(rf.getLastLogIndex()),
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(idx, &args, &reply)
			if reply.Term > rf.getCurrentTerm() {
				rf.setTerm(reply.Term)
				rf.tryBecomeFollower(reply.Term, int64(idx))
			}
		}(i)
	}
}

func (rf *Raft) getLastLogIndex() int64 {
	return 0
}

func (rf *Raft) getLogTermByIndex(idx int64) int64 {
	return 0
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
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	atomic.StoreInt64(&rf.nextVoteTimestamp,  getNextVoteTimeoutTimestamp())
	atomic.StoreInt64(&rf.nextHeartbeatTimestamp, getNextHeartbeatTimestamp())
	rf.setRole(RoleFollower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()

	return rf
}
