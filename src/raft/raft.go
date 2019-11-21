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
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	scale = 2

	maxTimeOutTime    = 1000 * time.Millisecond * scale
	minTimeOutTime    = 300 * time.Millisecond * scale
	heartBeatInterval = 50 * time.Millisecond * scale
	stepInterval      = 10 * time.Millisecond * scale
)

type Log struct {
	Command interface{}
	Term    int
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
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	status int

	grantedVotedNumber int64
	votedNumber        int64

	lock         sync.Locker
	timeOutReset bool

	ctx    context.Context
	cancel context.CancelFunc
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == Leader

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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.status == Leader || rf.status == Candidate {
			rf.transToFollower()
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.Log("reject request vote from %v because of term %v", args.CandidateId, args.Term)
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.commitIndex {
		rf.Log("agree request vote from %v", args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		rf.Log("reject request vote from %v because of candidate id or commit index", args.CandidateId)
		reply.VoteGranted = false
	}

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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Log("receive append entries rpc from %v", args.LeaderId)
	rf.resetTimeOut()
	if rf.status != Leader {
		rf.votedFor = -1
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.status == Leader || rf.status == Candidate {
			rf.transToFollower()
		}
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// TODO: 3, 4, 5

	switch rf.status {
	case Follower:
	case Candidate:
		if args.Term >= rf.currentTerm {
			rf.transToFollower()
		}
	case Leader:
	}

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

func (rf *Raft) loop() {
	rf.Log("start loop")
	go rf.timer()
	for {
		select {
		case <-rf.ctx.Done():
			rf.Log("end loop")
			return
		default:
		}
		rf.loopStep()
		time.Sleep(time.Millisecond)
	}
}

func (rf *Raft) loopStep() {
	//rf.Log("loop step")
	switch rf.status {
	case Follower:
		rf.stepFollower()
	case Candidate:
		rf.stepCandidate()
	case Leader:
		rf.stepLeader()
	}
	time.Sleep(stepInterval)
}

func (rf *Raft) stepFollower() {

}

func (rf *Raft) stepCandidate() {
	//if int(atomic.LoadInt64(&rf.votedNumber)) == len(rf.peers) {
	if int(atomic.LoadInt64(&rf.grantedVotedNumber)) > len(rf.peers)/2 {
		rf.transToLeader()
	}
	//}

}

func (rf *Raft) requestVotes() {
	rf.votedFor = -1
	rf.currentTerm++
	rf.Log("request votes")
	rf.grantedVotedNumber = 0
	rf.votedNumber = 0
	for i := 0; i < len(rf.peers); i++ {
		//if i == rf.me {
		//	continue
		//}
		go func(i int) {
			reply := RequestVoteReply{}
			success := rf.sendRequestVote(i, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.commitIndex,
				LastLogTerm:  rf.log[rf.commitIndex].Term,
			}, &reply)

			if success {
				if reply.VoteGranted {
					atomic.AddInt64(&rf.grantedVotedNumber, 1)
					rf.Log("get true vote from %v", i)
				} else {
					rf.Log("get false vote from %v", i)
				}
			} else {
				rf.Log("request vote connection to %v failed", i)
			}

			atomic.AddInt64(&rf.votedNumber, 1)
		}(i)
	}
	//rf.Log("request votes end %v/%v", rf.grantedVotedNumber, len(rf.peers))
}

func (rf *Raft) stepLeader() {
	rf.sendHeartbeatToAll()
	time.Sleep(rf.randLeaderSleepTime())
}

func (rf *Raft) sendHeartbeatToAll() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.Log("send heartbeat to %v", i)
			reply := AppendEntriesReply{}
			success := rf.sendAppendEntries(i, &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.log) - 1,
				PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}, &reply)
			if !success {
				rf.Log("heartbeat to %v failed", i)
			}
		}(i)
	}
}

func (rf *Raft) transToFollower() {
	rf.Log("trans to follower")
	rf.votedFor = -1
	rf.status = Follower
}

func (rf *Raft) transToCandidate() {
	rf.Log("trans to candidate")

	rf.requestVotes()
	rf.status = Candidate
}

func (rf *Raft) transToLeader() {
	rf.Log("trans to leader")

	rf.status = Leader
}

func (rf *Raft) resetTimeOut() {
	rf.timeOutReset = true
}

func (rf *Raft) timer() {
	for {
		select {
		case <-rf.ctx.Done():
			return
		default:
		}
		time.Sleep(rf.randTimeOutSleepTime())
		if rf.timeOutReset {
			rf.timeOutReset = false
			continue
		}
		switch rf.status {
		case Follower:
			rf.Log("time out")
			rf.followerTimeOut()
		case Candidate:
			rf.Log("time out")
			rf.candidateTimeOut()
		case Leader:
			//rf.leaderTimeOut()
		}
	}
}

func (rf *Raft) followerTimeOut() {
	rf.transToCandidate()
}

func (rf *Raft) candidateTimeOut() {
	rf.transToCandidate()
}

func (rf *Raft) leaderTimeOut() {

}

func (rf *Raft) randTimeOutSleepTime() time.Duration {
	return time.Duration(int64(minTimeOutTime) + rand.Int63n(int64(maxTimeOutTime-minTimeOutTime)))
}

func (rf *Raft) randLeaderSleepTime() time.Duration {
	return time.Duration(int64(heartBeatInterval))
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.Log("I'm killed")
	rf.cancel()
}

func (rf *Raft) Log(format string, a ...interface{}) {
	var status string
	switch rf.status {
	case Follower:
		status = "Follower "
	case Candidate:
		status = "Candidate"
	case Leader:
		status = "Leader   "
	}
	log.Print(fmt.Sprintf("[Term: %v, Id: %v, %v ] ", rf.currentTerm, rf.me, status), fmt.Sprintf(format, a...))
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, len(rf.peers))

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.status = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.ctx, rf.cancel = context.WithCancel(context.Background())
	rf.Log("created")
	go rf.loop()

	return rf
}
