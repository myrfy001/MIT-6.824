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

	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/src/labgob"
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
	id        time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	logs       []*LogEntry
	nextIndex  []int64
	matchIndex []int64

	currentTerm int64
	votedFor    int64
	commitIndex int64
	lastApplied int64

	role                   string
	nextVoteTimestamp      int64
	nextHeartbeatTimestamp int64
	applyCh                chan ApplyMsg
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

func (rf *Raft) getRole(lock bool) string {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	return rf.role
}
func (rf *Raft) setRole(newVote string, lock bool) {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.setNextIndex(int64(i), rf.getLastLogIndex(false)+1)
		rf.setMatchIndex(int64(i), 0)
	}

	rf.role = newVote
}

func (rf *Raft) getNextIndex(peer int64) int64 {
	return atomic.LoadInt64(&rf.nextIndex[peer])
}

func (rf *Raft) setNextIndex(peer int64, val int64) {
	atomic.StoreInt64(&rf.nextIndex[peer], val)
}

func (rf *Raft) getMatchIndex(peer int64) int64 {
	return atomic.LoadInt64(&rf.matchIndex[peer])
}

func (rf *Raft) setMatchIndex(peer int64, val int64) {
	atomic.StoreInt64(&rf.matchIndex[peer], val)
}

func (rf *Raft) getLastApplyID() int64 {
	return atomic.LoadInt64(&rf.lastApplied)
}

func (rf *Raft) setLastApplyID(val int64) {
	atomic.StoreInt64(&rf.lastApplied, val)
}

func (rf *Raft) getLogSlice(idx, cnt int64, lock bool) []LogEntry {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	ret := make([]LogEntry, 0, cnt)
	n := idx + cnt
	if n > int64(len(rf.logs)) {
		n = int64(len(rf.logs))
	}
	for _, item := range rf.logs[idx:n] {
		ret = append(ret, *item)
	}
	return ret
}

func (rf *Raft) getCommitIndex() int64 {
	return atomic.LoadInt64(&rf.commitIndex)
}

func (rf *Raft) setCommitIndex(val int64) {
	atomic.StoreInt64(&rf.commitIndex, val)
}

func (rf *Raft) getLogByIndex(idx int64, lock bool) *LogEntry {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if idx > rf.getLastLogIndex(false) {
		return nil
	}
	return rf.logs[idx]
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
	return getCurrentTimestampMs() + 120 + rand.Int63n(100)
}

func getNextHeartbeatTimestamp() int64 {
	return getCurrentTimestampMs() + 40
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = int(rf.getCurrentTerm())
	isleader = rf.getRole(true) == RoleLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(lock bool) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// logs, _ := json.Marshal(rf.logs)
	// DPrintf("%d persist at term %d: %s", rf.me, rf.getCurrentTerm(), string(logs))
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
	var curTerm = int64(0)
	var votedFor = int64(0)
	var logs = []*LogEntry{}

	if d.Decode(&curTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		panic("readPersist Error")
	} else {
		rf.currentTerm = curTerm
		rf.votedFor = votedFor
		rf.logs = logs

		logs, _ := json.Marshal(rf.logs)
		DPrintf("%d readpersist at term %d: %s", rf.me, rf.getCurrentTerm(), string(logs))

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

type LogEntry struct {
	Data  interface{}
	Term  int64
	Index int64
}

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

	defer rf.persist(true)

	if args.Term > rf.getCurrentTerm() {
		rf.tryBecomeFollowerAndUpdateTerm(args.Term, int64(args.CandidateId), true)
		reply.Term = rf.getCurrentTerm()
		DPrintf("\033[35m%d Update To Term %d because RV Request From %d\033[0m\n", rf.me, args.Term, args.CandidateId)
	}

	if rf.getVotedFor() == -1 || rf.getVotedFor() == args.CandidateId {
		if rf.checkIfIncomeLogNotOld(args.LastLogTerm, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.setVotedFor(args.CandidateId)

			myLastLogIdx := rf.getLastLogIndex(true)
			myLastTerm := rf.getLogTermByIndex(myLastLogIdx, true)
			DPrintf("\033[31m%d Give Vote To %d, myT: %d, myI: %d, InT: %d, InI: %d\033[0m\n", rf.me, args.CandidateId, myLastTerm, myLastLogIdx, args.LastLogTerm, args.LastLogIndex)
		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	reply.Term = rf.getCurrentTerm()
	if args.Term < rf.getCurrentTerm() {
		return
	}

	defer rf.persist(true)

	if args.Term > rf.getCurrentTerm() {
		rf.tryBecomeFollowerAndUpdateTerm(args.Term, int64(args.LeaderID), true)
		reply.Term = rf.getCurrentTerm()
		DPrintf("\033[35m%d Update To Term %d because AE Request from %d\033[0m\n", rf.me, args.Term, args.LeaderID)
	}

	if rf.getRole(true) == RoleLeader {
		DPrintf("\033[35m%d is leader now but receive AE from %d with term %d, ignore\033[0m\n", rf.me, args.LeaderID, args.Term)
		return
	}

	atomic.StoreInt64(&rf.nextVoteTimestamp, getNextVoteTimeoutTimestamp())

	myPrevLog := rf.getLogByIndex(args.PrevLogIndex, true)
	if myPrevLog == nil {
		return
	}
	if myPrevLog.Term != args.PrevLogTerm {
		return
	}

	// 走到这里，就可以回复true了
	reply.Success = true

	rf.removeLogStartFromIdx(args.PrevLogIndex+1, true)

	for i := range args.Entries {
		rf.appendLogByReceivedLog(&args.Entries[i])
	}

	rf.mu.Lock()
	newCommitIdx := args.LeaderCommit
	maxIndexOfMyselfLog := rf.getLastLogIndex(false)
	if maxIndexOfMyselfLog < newCommitIdx {
		newCommitIdx = maxIndexOfMyselfLog
	}
	rf.setCommitIndex(newCommitIdx)
	rf.mu.Unlock()

}

func (rf *Raft) tryBecomeFollowerAndUpdateTerm(term int64, triggerSrorce int64, lock bool) {
	// 当发现比自己更大的term时，更新自己的term和切换到follower必须是一个事务
	// 同时，Leader在发送AE前，检查自己是不是Leader以及确定自己发送的AE的Term这件事，也必须是一个事务
	// 避免先检查自己是Leader以后，到真正发送AE之间，自己的Term被更新，导致自己的Log是老的，但是自己的Term是新的，自己拿着最新的Term去骗人
	// 可能发生的情况是，Leader断网后，自己一直没有更新，并且一直认为自己还是Leader，网络恢复后，Leader的日志已经落后，但是收到别人的HeartBeat以后，
	// 先更新了自己的Term, 然后设置自己的角色为Follower，在这个间隙中（Term已经更新，但是角色还是Leader）时，就可能以Leader的身份和最新的Term向外发送消息
	// 从而干扰其他peer
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	rf.setTerm(term)
	rf.tryBecomeFollower(term, triggerSrorce, false)
	atomic.StoreInt64(&rf.nextVoteTimestamp, getNextVoteTimeoutTimestamp())
}

func (rf *Raft) checkLeaderAndGetCurrentTerm(lock bool) int64 {
	// 参见tryBecomeFollowerAndUpdateTerm的注释
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if rf.getRole(false) != RoleLeader {
		return -1
	}
	return rf.getCurrentTerm()
}

func (rf *Raft) checkIfIncomeLogNotOld(term int64, index int64) bool {
	myLastLogIdx := rf.getLastLogIndex(true)
	myLastTerm := rf.getLogTermByIndex(myLastLogIdx, true)
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
	if len(args.Entries) > 0 {
		DPrintf("%d Send sendAppendEntries to %d At term %d log cnt %d id %v\n", rf.me, server, rf.getCurrentTerm(), len(args.Entries), rf.id)
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.getRole(false) == RoleLeader

	// Your code here (2B).

	if !isLeader {
		return index, term, isLeader
	}

	index = int(rf.appendLogByData(command, false))

	term = int(rf.getCurrentTerm())

	rf.persist(false)

	DPrintf("\033[32m%d client add log %d term %d value %v\033[0m\n", rf.me, index, term, command)

	return index, term, isLeader
}

func (rf *Raft) appendLogByData(data interface{}, lock bool) int64 {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	newIdx := rf.getLastLogIndex(false) + 1

	log := LogEntry{
		Data:  data,
		Term:  rf.getCurrentTerm(),
		Index: newIdx,
	}

	rf.logs = append(rf.logs, &log)

	return newIdx
}

func (rf *Raft) appendLogByReceivedLog(data *LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logs = append(rf.logs, data)

}

// removeLogStartFromIdx 从idx开始删除，以及后面的日志，idx也删掉
func (rf *Raft) removeLogStartFromIdx(idx int64, lock bool) {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	oldLen := len(rf.logs)
	rf.logs = rf.logs[:idx]
	if len(rf.logs) != oldLen {
		DPrintf("\033[34m%d remove log after log idx %d\033[0m\n", rf.me, len(rf.logs)-1)
	}
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
		if rf.getRole(true) == RoleLeader { // Candidate模式下，第一次发出去的选票可能没有结果，所以Candidate超时了也要重新发起选举
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
		if rf.getRole(true) == RoleFollower || rf.getRole(true) == RoleCandidate {
			continue
		}

		go rf.broadcastHeartbeat()
	}
	DPrintf("%v heartbest exit because killed\n", rf.id)
}

func (rf *Raft) logReplicaWorker(peer int64) {
	for rf.killed() == false {
		time.Sleep(1 * time.Millisecond)
		if rf.getRole(true) != RoleLeader {
			continue
		}

		// nextIdx = 0 是一个dummy项，正经的日志都是从1开始的
		nextIdx := rf.getNextIndex(peer)
		if nextIdx > rf.getLastLogIndex(true) {
			continue
		}
		entries := rf.getLogSlice(nextIdx-1, 101, true)
		if len(entries) == 0 {
			continue
		}

		curTerm := rf.checkLeaderAndGetCurrentTerm(true)
		if curTerm == -1 {
			continue
		}
		args := AppendEntriesArgs{
			Term:         curTerm,
			LeaderID:     rf.me,
			PrevLogIndex: entries[0].Index,
			PrevLogTerm:  entries[0].Term,
			Entries:      entries[1:],
			LeaderCommit: rf.getCommitIndex(),
		}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(int(peer), &args, &reply)

		if reply.Term > rf.getCurrentTerm() {
			rf.tryBecomeFollowerAndUpdateTerm(reply.Term, peer, true)
			rf.persist(true)
			DPrintf("\033[35m%d Update To Term %d because AE Reply from %d when rep log\033[0m\n", rf.me, reply.Term, peer)
			continue
		}

		if reply.Success {
			rf.setNextIndex(peer, nextIdx+int64(len(entries)-1))
			rf.setMatchIndex(peer, nextIdx+int64(len(entries)-1))
		} else {
			if nextIdx > 1 {
				rf.setNextIndex(peer, nextIdx-1)
			} else if nextIdx == 1 {
				// 针对1的特殊处理，否则会导致TestBackup2B失败
				continue
			} else {
				panic(fmt.Sprintf("nextIndex for peer %d reach 0", peer))
			}
		}
	}
}

func (rf *Raft) logCommitWorker() {
	for rf.killed() == false {
		time.Sleep(1 * time.Millisecond)
		if rf.getRole(true) != RoleLeader {
			continue
		}

		nowCommited := rf.getCommitIndex()
		quorum := len(rf.peers) / 2

		n := rf.getLastLogIndex(true)

	brk:
		for n > nowCommited {
			cnt := 1
			for peer := 0; peer < len(rf.peers); peer++ {
				if rf.getMatchIndex(int64(peer)) > n {
					cnt++
				}
				if cnt > quorum {
					log := rf.getLogByIndex(n, true)
					if log == nil {
						// 说明log不存在，可能是已经被压缩了，说明n退的太靠前了， 理论上不应该出现
						panic(fmt.Sprintf("No log at index %d", n))
					}
					if log.Term != rf.getCurrentTerm() {
						// 已经读到了前一任的日志了，n退的太多了，leader只能commit自己的日志
						break brk
					}
					rf.setCommitIndex(n)

					DPrintf("\033[31m%d commit log index %d term %d value %v\033[0m\n", rf.me, log.Index, log.Term, log.Data)

					break brk
				}
			}
			n--
		}
	}
}

func (rf *Raft) applyMsgWorker() {
	for {
		time.Sleep(1 * time.Millisecond)
		newCommitIdx := rf.getCommitIndex()
		// 下面循环的初始条件的+1是为了避免对idx=0的判断
		for i := rf.getLastApplyID() + 1; i <= newCommitIdx; i++ {
			log := rf.getLogByIndex(i, true)
			if log == nil {
				panic("Get log which is nil")
			}
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Data,
				CommandIndex: int(log.Index),
			}
			rf.setLastApplyID(i)
			DPrintf("\033[31m%d apply log index %d term %d value %v\033[0m\n", rf.me, log.Index, log.Term, log.Data)
		}
	}
}

func (rf *Raft) doElection() {
	rf.setRole(RoleCandidate, true)
	rf.incTerm()
	rf.setVotedFor(int64(rf.me))
	rf.persist(true)

	recvVoteCnt := int64(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			args := RequestVoteArgs{
				Term:         rf.getCurrentTerm(),
				CandidateId:  int64(rf.me),
				LastLogIndex: rf.getLastLogIndex(true),
				LastLogTerm:  rf.getLogTermByIndex(rf.getLastLogIndex(true), true),
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(idx, &args, &reply)
			if reply.VoteGranted {
				newCnt := atomic.AddInt64(&recvVoteCnt, 1)
				DPrintf("\033[33m%d Recv Vote From %d For term %d, my cur term %d\033[0m\n", rf.me, idx, reply.Term, rf.getCurrentTerm())
				if newCnt > int64(len(rf.peers))/2 {
					if rf.tryBecomeLeader(reply.Term, true) {
						go rf.broadcastHeartbeat()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) tryBecomeLeader(term int64, lock bool) bool {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if term < rf.getCurrentTerm() || term > rf.getCurrentTerm() {
		return false
	}
	if rf.getRole(false) == RoleLeader {
		return false
	}
	rf.setRole(RoleLeader, false)
	DPrintf("\033[31m%d Win Election At term %d\033[0m\n", rf.me, term)
	return true
}

func (rf *Raft) tryBecomeFollower(term int64, triggerSrorce int64, lock bool) {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	if term < rf.getCurrentTerm() || term > rf.getCurrentTerm() {
		return
	}
	if rf.getRole(false) == RoleFollower {
		return
	}
	if rf.getRole(false) != RoleFollower {
		DPrintf("\033[31m[***] %d Turn To Follower From State %s At term %d, Because Server %d\033[0m\n", rf.me, rf.getRole(false), rf.getCurrentTerm(), triggerSrorce)
		rf.setRole(RoleFollower, false)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			curTerm := rf.checkLeaderAndGetCurrentTerm(true)
			if curTerm == -1 {
				return
			}
			args := AppendEntriesArgs{
				Term:         curTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.getLastLogIndex(true),
				PrevLogTerm:  rf.getLogTermByIndex(rf.getLastLogIndex(true), true),
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(idx, &args, &reply)
			if reply.Term > rf.getCurrentTerm() {
				rf.tryBecomeFollowerAndUpdateTerm(reply.Term, int64(idx), false)
				rf.persist(true)
				DPrintf("\033[35m%d Update To Term %d because Found New One from heartbeat response of %d\033[0m\n", rf.me, reply.Term, idx)
			}
		}(i)
	}
}

func (rf *Raft) getLastLogIndex(lock bool) int64 {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	return int64(len(rf.logs)) - 1
}

func (rf *Raft) getLogTermByIndex(idx int64, lock bool) int64 {
	if lock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	return rf.logs[idx].Term
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
	rf.id = time.Now()

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	atomic.StoreInt64(&rf.nextVoteTimestamp, getNextVoteTimeoutTimestamp())
	atomic.StoreInt64(&rf.nextHeartbeatTimestamp, getNextHeartbeatTimestamp())

	x := time.Now()
	rf.matchIndex = make([]int64, len(peers))
	rf.nextIndex = make([]int64, len(peers))
	// 这里追加一条空日志，占据logs下标为0的位置，作为dummy head 简化后续代码。实际raft的log都是从idx=1开始的
	rf.logs = append(rf.logs, &LogEntry{})
	rf.setRole(RoleFollower, true)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("%d Restart Reading Uss %v", me, time.Since(x))
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.logCommitWorker()
	go rf.applyMsgWorker()
	for i := 0; i < len(peers); i++ {
		if i != me {
			go rf.logReplicaWorker(int64(i))
		}
	}

	return rf
}
