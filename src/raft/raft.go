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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"example.com/raft/src/labrpc"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	votedForTerm int   // 自己加的，应该如何去掉？
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextInedx   []int
	matchIndex  []int

	role RaftRole // 当前角色

	resetRequestForVoteTimerCh chan struct{}

	
	// ==================
	nextElectionTime time.Time
	nextHeartBeatTime time.Time
	snapshotEndIndex int
	snapshotEndTerm int
}

type LogEntry struct{
	Term int
	Action int
	Key string
	Value string
	PrevValue string
}

type RaftRole int

const RaftRoleLeader RaftRole = 1
const RaftRoleFollower RaftRole = 2
const RaftRoleCandidate RaftRole = 3


func getNewElectionTime() time.Time {
	return time.Now().Add(time.Millisecond * time.Duration(150 + rand.Intn(20)))
}

func getNewHeartBeatTime() time.Time {
	return time.Now().Add(time.Millisecond * time.Duration(50 + rand.Intn(20)))
}


func (r *Raft) getLastLogIndex() int {
	return r.snapshotEndIndex + len(r.log)
}

func (r *Raft) getLastLogTerm() int {
	if len(r.log) > 0 {
		return int(r.log[len(r.log)-1].Term)
	}
	return r.snapshotEndTerm
}


func (r *Raft) getLogByIdx(idx int) *LogEntry {
	t := idx - r.snapshotEndIndex
	if t > 0 && t < len(r.log){
		return &r.log[t]
	}
	return nil
}



func (r *Raft) resetNextElectionTime () {
	r.nextElectionTime = getNewElectionTime()
}

func (r *Raft) resetNextHeartBeatTime () {
	r.nextHeartBeatTime = getNewHeartBeatTime()
}


func (r *Raft) requestVoteWorker() {
	r.resetNextElectionTime()
	r.resetNextHeartBeatTime()
	for {
		time.Sleep(5 * time.Millisecond)
		if r.role != RaftRoleLeader {
			if time.Now().After(r.nextElectionTime) {
				go r.doElect()
			}
		} else {
			if time.Now().After(r.nextHeartBeatTime) {
				go r.doHeartBeat()
			}
		}
	}
}


func (r *Raft) doHeartBeat() {
	r.resetNextHeartBeatTime()
	for peerIdx := range r.peers{
		if peerIdx != r.me {
			args := AppendEntriesArgs{
				Term: r.currentTerm,
				LeaderID: r.me,
				PrevLogIndex: r.getLastLogIndex(),
				PrevLogTerm: r.getLastLogTerm(),
			}
			go func(peerIdx int) {
				resp := AppendEntriesReply{}
				r.sendAppendEntries(peerIdx, &args, &resp)
				if resp.Term > r.currentTerm {
					r.role = RaftRoleFollower
				}
			}(peerIdx)
		}
	}
}

func (r *Raft) doElect() {
	r.mu.Lock()
	

	r.resetNextElectionTime()

	r.currentTerm++
	

	r.role = RaftRoleCandidate
	wg := sync.WaitGroup{}
	winCnt := int64(1)
	r.votedFor = r.me
	r.votedForTerm = r.currentTerm
	r.mu.Unlock()

	for peerIdx := range r.peers{
		if peerIdx != r.me {

			wg.Add(1)
			go func(peerIdx int){
				defer wg.Done()
				args := RequestVoteArgs{
					Term: int(r.currentTerm),
					CandidateID: r.me,
					LastLogIndex: len(r.log),
				}
				resp := RequestVoteReply{}
				r.sendRequestVote(peerIdx, &args, &resp)
				if resp.VoteGranted {
					atomic.AddInt64(&winCnt, 1)
				}
				
			}(peerIdx)
		}
	}
	fmt.Println("-====")
	wg.Wait()
	fmt.Println("-----")
	

	if winCnt > int64(len(r.peers) / 2) {
		fmt.Println("[***]", r.me, "Win Election At term", r.currentTerm)
		r.role = RaftRoleLeader
		go r.doHeartBeat() // 赢得选举后，立即发送广播，通知别人自己已经胜出
	}


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here (2A).

	if rf.role == RaftRoleLeader {
		isleader = true
	}

	return rf.currentTerm, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term int // leader’s term
	LeaderID int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm int // term of prevLogIndex entry
	Entries []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < int(rf.currentTerm){
		return
	} 

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm==rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.votedForTerm = args.Term
			rf.role = RaftRoleFollower
			fmt.Println(rf.me, "Voted For A", args.CandidateID, "term", args.Term)
			return
		}
	}

	if rf.votedFor != args.CandidateID {
		if rf.votedForTerm < args.Term {
			if  args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm==rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				rf.votedForTerm = args.Term
				rf.role = RaftRoleFollower
				fmt.Println(rf.me, "Voted For B", args.CandidateID, "term", args.Term)
			}
		}
		return
	}
	panic("Should not reach here")
	
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { 
	rf.resetNextElectionTime()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return 
	}
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		return
	}




	// TODO Apply Log

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	
	reply.Success = true
	
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
	fmt.Println(rf.me, "sendRequestVote to ", server, "term", args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Println(rf.me, "sendAppendEntries to ", server, "term", args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	go rf.requestVoteWorker()
	return rf
}
