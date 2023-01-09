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
	//"6.824/labgob"
	//"bytes"
	//"fmt"
	//"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ----------------------
const electionTimeout = 1000 * time.Millisecond //1 * time.Second
const (
	electionTimeoutStart    time.Duration = 1000 * time.Millisecond //400
	electionTimeoutInterval time.Duration = 500 * time.Millisecond
	heartbeatInterval       time.Duration = 200 * time.Millisecond //100
)

//150 * time.Millisecond
// ----------------------

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

// --------
type Tstate int

const (
	Follower Tstate = iota
	Candidate
	Leader
)

// --------
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	applyCh      chan ApplyMsg
	applyCond    *sync.Cond
	state        Tstate
	electionTime time.Time
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int
	votedFor    int
	log         Log

	// Volatile state
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex  []int
	matchIndex []int

	// Snapshot state
	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int

	// Temporary location to give the service snapshot to the apply thread
	waitingSnapshot []byte
	waitingIndex    int // lastIncludedIndex
	waitingTerm     int // lastIncludedTerm

	// may fix lab3 speed
	timer      *time.Timer
	timerLock  sync.Mutex
	appendCond *sync.Cond //only valid in leader state
	//heartbeatTimerTerminateChannel chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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

// ------------------
func (rva *RequestVoteArgs) String() string {
	return fmt.Sprintf("Term %v Id %v LastLogIndex %v LastLogTerm %v",
		rva.Term, rva.CandidateId, rva.LastLogIndex, rva.LastLogTerm)
}

// ------------------

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// ------------------
func (rvr *RequestVoteReply) String() string {
	return fmt.Sprintf("Term %v VoteGranted %v",
		rvr.Term, rvr.VoteGranted)
}

// ------------------

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	myIndex := rf.log.lastindex()
	myTerm := rf.log.entry(myIndex).Term
	uptodate := (args.LastLogTerm == myTerm && args.LastLogIndex >= myIndex) ||
		args.LastLogTerm > myTerm

	DPrintf(
		"%v: RequestVote %v %v uptodate %v (myIndex %v, myTerm %v)\n",
		rf.me, args, reply, uptodate, myIndex, myTerm)

	if args.Term < rf.currentTerm {
		// Rule 1
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		// Rule 2
		rf.resetTimer()
		//rf.setElectionTime()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
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
	DPrintf("%v: sendRequestVote to %v: %v\n", rf.me, server, args)
	//fmt.Printf("%v: sendRequestVote currentTime=%v lastLog=%v-----------------------------------------------------------------------\n", rf.me, rf.currentTerm, rf.log.lastindex())
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	/*
		index := -1
		term := -1
		isLeader := true
	*/

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	e := Entry{rf.currentTerm, command}
	index := rf.log.lastindex() + 1
	rf.log.append(e)
	rf.persist()

	DPrintf("%v: Start %v %v\n", rf.me, index, e)
	//fmt.Printf("%v: Start index=%v entry=%v\n", rf.me, index, e)
	//rf.sendAppendsL(false)

	// fix lab3
	rf.appendCond.Broadcast()

	return index, rf.currentTerm, true
	//return index, term, isLeader
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

// --------------
func (rf *Raft) newTermL(term int) {
	DPrintf("%v: newTerm %v follower\n", rf.me, term)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) leaderProcess(currentTerm int) {
	for server := range rf.peers {
		if server == rf.me { //do not send to myself
			continue
		}
		go func(server int) { //use seperate goroutines to send messages: can set independent timers.
			//initial heartbeat.
			DPrintf("leader %d send heartbeat to %d\n", rf.me, server)
			go rf.sendAppendL(server, true, currentTerm)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for !rf.killed() {
				//fmt.Printf("%v: loop in currentTerm:%v ?= rf.term:%v to peer:%v\n",
				//	rf.me, currentTerm, rf.currentTerm, server)
				if rf.currentTerm != currentTerm || rf.state != Leader {
					return
				}

				go rf.sendAppendL(server, false, currentTerm)
				rf.appendCond.Wait()
			}
		}(server)
	}
}

func (rf *Raft) becomeLeaderL() {
	rf.mu.Lock()
	DPrintf("%v: becomeLeader %v %v\n",
		rf.me, rf.currentTerm, rf.log.lastindex())

	if rf.state == Leader {
		rf.mu.Unlock()
		return
	} else {
		rf.state = Leader
	}

	for i, _ := range rf.nextIndex {
		// nextIndex is just a guess; we may be initializing
		// it too high for some followers, but we'll back up.
		rf.nextIndex[i] = rf.log.lastindex() + 1
	}
	//fmt.Printf("%v: becomeLeaderL currentTerm=%v lastLog=%v----------------------------------------------------------\n", rf.me, rf.currentTerm, rf.log.lastindex())

	// fix lab3
	currentTerm := rf.currentTerm
	heartbeatTimer := time.NewTimer(heartbeatInterval)
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.currentTerm != currentTerm || rf.state != Leader {
				rf.mu.Unlock()
				rf.appendCond.Broadcast()
				return
			}
			rf.mu.Unlock()

			select {
			case <-heartbeatTimer.C:
				rf.resetTimer()
				rf.appendCond.Broadcast()
				//fmt.Printf("%v: heartbeat broadcast in term%v gorountine:%v\n",
				//	rf.me, currentTerm, runtime.NumGoroutine())
				heartbeatTimer.Reset(heartbeatInterval)
			}
		}
	}()
	rf.leaderProcess(currentTerm)
	rf.mu.Unlock()
}

func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(peer, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("%v: RequestVote reply from %v: %v\n",
			rf.me, peer, &reply)
		if reply.Term > rf.currentTerm {
			rf.newTermL(reply.Term)
		}
		if reply.VoteGranted {
			*votes += 1
			if *votes > len(rf.peers)/2 {
				if rf.currentTerm == args.Term { // still current?
					if rf.state == Leader {
						return
					}
					go rf.becomeLeaderL()
					//fmt.Printf("%v: requestVote from peer=%v\n", rf.me, peer)
					//rf.sendAppendsL(true)
				}
			}
		}
	}
}

func (rf *Raft) requestVotesL() {
	args := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.log.lastindex(),
		rf.log.entry(rf.log.lastindex()).Term}
	votes := 1
	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, args, &votes)
		}
	}
}

func (rf *Raft) startElectionL() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1
	rf.state = Candidate

	rf.votedFor = rf.me
	rf.persist()

	DPrintf("%v: start election for term %v\n", rf.me, rf.currentTerm)

	rf.requestVotesL()
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(electionTimeout)
	ms := rand.Int63() % 300
	//ms := rand.Int63() % 150
	t = t.Add(time.Duration(ms) * time.Millisecond)
	//fmt.Printf("ms=%v time.Duration=%v, time=%v electiontime=%v\n", ms, time.Duration(ms), time.Duration(ms)*time.Millisecond, t)
	rf.electionTime = t
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("%v: tick state %v\n", rf.me, rf.state)

	//if rf.state == Leader {
	//	rf.setElectionTime()
	//	rf.sendAppendsL(true)
	//} else if time.Now().After(rf.electionTime) {
	//	rf.setElectionTime()
	//	rf.startElectionL()
	//}

	if rf.state == Leader {
		//rf.resetTimer()
		rf.setElectionTime()
		rf.appendCond.Broadcast()
	} else if time.Now().After(rf.electionTime) {
		rf.setElectionTime()
		rf.startElectionL()
	}
}

//---------------

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		//time.Sleep().
		//rf.tick()
		//ms := 200 //50
		//time.Sleep(time.Duration(ms) * time.Millisecond)

		// may fix lab3 speed
		<-rf.timer.C
		if rf.killed() {
			break
		}

		go rf.startElectionL()

		rf.timerLock.Lock()
		duration := time.Duration(rand.Int63())%electionTimeoutInterval + electionTimeoutStart
		rf.timer.Reset(duration)
		rf.timerLock.Unlock()
	}
}

func (rf *Raft) resetTimer() {
	rf.timerLock.Lock()
	//timer must first be stopped, then reset.
	if !rf.timer.Stop() {
		//this may go wrong, but very unlikely.
		//see here: https://zhuanlan.zhihu.com/p/133309349
		select {
		case <-rf.timer.C: //try to drain from the channel
		default:
		}
	}
	duration := time.Duration(rand.Int63())%electionTimeoutInterval + electionTimeoutStart
	rf.timer.Reset(duration)
	rf.timerLock.Unlock()
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

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	//rf.setElectionTime()

	rf.votedFor = -1
	rf.log = mkLogEmpty()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// fix lab3
	rf.appendCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// fix lab3
	rand.Seed(int64(rf.me))
	rf.timerLock.Lock()
	rf.timer = time.NewTimer(time.Duration(rand.Int())%electionTimeoutInterval + electionTimeoutStart)
	rf.timerLock.Unlock()

	// start ticker goroutine to start elections
	go rf.applier()

	go rf.ticker()

	return rf
}
