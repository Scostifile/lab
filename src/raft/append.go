package raft

import (
	"fmt"
	"log"
	"sort"
)

type AppendEntriesArgs struct {
	Term         int     // Leader's term
	LeaderId     int     // who is the leader
	PrevLogIndex int     // index immediately preceding new ones
	PrevLogTerm  int     // term of PrevLogIndex entry
	Entries      []Entry // new log entries
	LeaderCommit int     // leader's commitIndex
}

func (aea *AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"Term %v Leader %v PrvLogIndex %v PrvLogTerm %v Entries %v LeaderCommit %v",
		aea.Term, aea.LeaderId, aea.PrevLogIndex, aea.PrevLogTerm, aea.Entries, aea.LeaderCommit)
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // follower had entry for prevLogIndex/prevLogTerm
	ConflictTerm  int  // term of conflicting entry
	ConflictFirst int  // first index for that term
	ConflictValid bool
}

func (aer *AppendEntriesReply) String() string {
	return fmt.Sprintf("Term %v Succ %v CfiTerm %v ClfFirst %v ClfValid %v",
		aer.Term, aer.Success, aer.ConflictTerm, aer.ConflictFirst, aer.ConflictValid)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
		rf.votedFor = args.LeaderId
		rf.persist()
		DPrintf("%v: AppendEntries argsTerm=%v > peercurrentTerm=%v\n", rf.me, args.Term, rf.currentTerm)
	} else if args.Term < rf.currentTerm {
		DPrintf("%v: AppendEntries rfTerm=%v\n", rf.me, rf.currentTerm)
		return
	}

	if rf.state == Candidate {
		rf.state = Follower
	}
	rf.setElectionTime()
	reply.Success = false
	reply.ConflictValid = false
	reply.ConflictFirst = -1
	reply.ConflictTerm = -1

	// for 2D
	if args.PrevLogIndex < rf.snapshotIndex {
		//fmt.Printf("%v: AppendEntries from leader:%v Term:%v peersnapshotindex:%v > leaderprevlogindex:%v\n", rf.me, args.LeaderId, rf.currentTerm, rf.snapshotIndex, args.PrevLogIndex)
		reply.ConflictValid = true
		//reply.ConflictFirst = rf.snapshotIndex + 1
		reply.ConflictFirst = rf.log.lastindex() + 1
		return
	}

	// Rule 2
	if rf.log.lastindex() < args.PrevLogIndex {
		//fmt.Printf("%v: AppendEntries rule2 lastindex:%v, previndex:%v\n", rf.me, rf.log.lastindex(), args.PrevLogIndex)
		reply.ConflictValid = true
		reply.ConflictFirst = rf.log.lastindex() + 1
		//reply.ConflictTerm = -1
		return
	}

	// optimization of conflict log entry
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		//fmt.Printf("%v: AppendEntries optimization of conflict peerlogTerm:%v leaderlogTerm:%v previndex:%v logindex0:%v\n",
		//	rf.me, rf.log.entry(args.PrevLogIndex).Term, args.PrevLogTerm, args.PrevLogIndex, rf.log.start())
		reply.ConflictValid = true
		reply.ConflictTerm = rf.log.entry(args.PrevLogIndex).Term
		for index := args.PrevLogIndex; index >= rf.log.start(); index-- {
			if rf.log.entry(index).Term != reply.ConflictTerm {
				reply.ConflictFirst = index + 1
				break
			}
		}
		return
	}

	reply.Success = true
	for i, e := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index <= rf.log.lastindex() {
			// Rule 3
			if rf.log.entry(index).Term != e.Term {
				rf.log.cutend(index)
				rf.log.append(e)
			}
		} else {
			// Rule 4
			rf.log.append(e)
		}
		// assert that entries are equal
		//if reflect.DeepEqual(rf.log.entry(index).Command, e.Command) {
		//	log.Fatalf("Entry Error %v from=%v index=%v old=%v new=%v\n",
		//		rf.me, args.LeaderId, index, rf.log.entry(index), args.Entries[i])
		//}
	}
	rf.persist()

	// Rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		lastNew := args.PrevLogIndex + 1 + len(args.Entries) - 1
		if rf.commitIndex > lastNew {
			DPrintf("%v: Commit to %v\n", rf.me, lastNew)
			rf.commitIndex = lastNew
		}
		rf.signalApplierL()
	}
	//fmt.Printf("%v AppendEntries receive from server=%v goroutine=%v\n", rf.me, args.LeaderId, runtime.NumGoroutine())
	//rf.persist()
}

func (rf *Raft) sendAppendsL(heartbeat bool) {
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.log.lastindex() > rf.nextIndex[i] || heartbeat {
				rf.sendAppendL(i, heartbeat)
			}
		}
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	next := rf.nextIndex[peer]

	if next <= rf.log.start() { // <= because always skip entry "0"
		next = rf.log.start() + 1
	}

	// ?? maybe not execute
	if next-1 > rf.log.lastindex() {
		fmt.Printf("%v: sendAppendL nextIndex[%v]=%v > lastindex=%v, index0=%v\n",
			rf.me, peer, rf.nextIndex[peer], rf.log.lastindex(), rf.log.start())
		next = rf.log.lastindex() //+ 1
	}

	if next <= rf.snapshotIndex && rf.snapshotIndex > 0 {
		//if next <= rf.snapshotIndex {
		rf.sendSnapshot(peer)
		//fmt.Printf("%v: sendAppendL next:%v < lastincludeindex:%v, sendSnapshot to %v\n",
		//	rf.me, next, rf.snapshotIndex, peer)
		return
	}

	args := &AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		next - 1,
		rf.log.entry(next - 1).Term,
		make([]Entry, rf.log.lastindex()-next+1),
		rf.commitIndex}
	copy(args.Entries, rf.log.slice(next))
	go func() {
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendReplyL(peer, args, &reply)
		}
	}()
}

func (rf *Raft) sendAppendEntries(
	peer int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) bool {
	DPrintf("%v: sendAppedn to %v: %v\n", rf.me, peer, args)
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) advanceCommitL() {
	if rf.state != Leader {
		log.Fatalf("advanceCommit: state %v\n", rf.state)
	}

	//start := rf.commitIndex + 1
	//if start < rf.log.start() { // on restart start could be 1
	//	start = rf.log.start()
	//}
	//
	//for index := start; index <= rf.log.lastindex(); index++ {
	//	if rf.log.entry(index).Term != rf.currentTerm {
	//		continue
	//	}
	//	n := 1
	//	for i := 0; i < len(rf.peers); i++ {
	//		if i != rf.me && rf.matchIndex[i] >= index {
	//			n += 1
	//		}
	//	}
	//	if n > len(rf.peers)/2 {
	//		DPrintf("%v: Commit %v\n", rf.me, index)
	//		rf.commitIndex = index
	//		//fmt.Printf("%v: Commit*********************************************************** %v\n", rf.me, index)
	//	}
	//}

	matchLen := len(rf.matchIndex)
	tempMatchIndex := make([]int, matchLen-1)
	j := 0
	for i := 0; i < matchLen; i++ {
		if i != rf.me {
			tempMatchIndex[j] = rf.matchIndex[i]
			j++
		}
	}
	sort.Ints(tempMatchIndex)
	medianIndex := tempMatchIndex[(matchLen-1)/2]
	//fmt.Printf("%v: advanceCommit tempMatchIndex:%v medianIndex:%v\n", rf.me, tempMatchIndex, medianIndex)
	if medianIndex > rf.commitIndex && rf.log.entry(medianIndex).Term == rf.currentTerm {
		rf.commitIndex = medianIndex
	} else {
		return
	}

	rf.signalApplierL()
}

func (rf *Raft) processConflictTermL(
	peer int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) {

	//if reply.ConflictTerm > rf.currentTerm { // follower's log ahead but isn't committed, thus it can be erased
	//	DPrintf("%v: processConflictTermL: reset nextIndex %v to %v\n",
	//		rf.me, peer, reply.ConflictFirst)
	//	rf.nextIndex[peer] = reply.ConflictFirst
	//} else {
	//	if rf.nextIndex[peer] > rf.log.lastindex() {
	//		rf.nextIndex[peer] = rf.log.lastindex()
	//	}
	//	DPrintf("%v: processConflictTermL: reset nextIndex %v to term %v,"+
	//		"starting %v log %v\n",
	//		rf.me, peer, reply.ConflictTerm, rf.nextIndex[peer], rf.log)
	//	for {
	//		if rf.nextIndex[peer] < rf.log.start()+1 || rf.log.entry(rf.nextIndex[peer]).Term < reply.ConflictTerm { // missed everything?
	//			break
	//		}
	//		if rf.log.entry(rf.nextIndex[peer]).Term == reply.ConflictTerm {
	//			break
	//		}
	//		rf.nextIndex[peer] = -1
	//	}
	//	DPrintf("%v: processConflictTermL: back up follower to %v\n", rf.me, rf.nextIndex[peer])
	//}

	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictFirst
	} else {
		lastLogIndex := -1
		for i := rf.log.lastindex(); i > rf.log.start(); i-- {
			term := rf.log.entry(i).Term
			if term == reply.ConflictTerm {
				lastLogIndex = i
				break
			} else if term < reply.ConflictTerm {
				break
			}
		}
		if lastLogIndex > 0 {
			rf.nextIndex[peer] = lastLogIndex
		} else {
			rf.nextIndex[peer] = reply.ConflictFirst
		}
	}

	if rf.nextIndex[peer] < rf.log.start()+1 {
		rf.sendSnapshot(peer)
		//fmt.Printf("%v: processConflictTermL sendSnapshot\n", rf.me)
	}
}

func (rf *Raft) processAppendReplyTermL(
	peer int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) {
	if reply.Success {
		newnext := args.PrevLogIndex + len(args.Entries) + 1
		newmatch := args.PrevLogIndex + len(args.Entries)
		if newnext > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newnext
		}
		if newmatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newmatch
		}
		DPrintf("%v: processAppendReply success: peer %v next=%v match=%v\n",
			rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
	} else if reply.ConflictValid {
		rf.processConflictTermL(peer, args, reply)
		//rf.sendAppendL(peer, false)
	} else if rf.nextIndex[peer] > 1 {
		DPrintf("%v: processAppendReplyL backup by one\n", rf.me)
		//rf.nextIndex[peer] -= 1
		fmt.Printf("%v: processAppendReplyL next:%v index0:%v==%v\n", rf.me, rf.nextIndex[peer], rf.log.start(), rf.log.index0)
		if rf.nextIndex[peer] < rf.log.start()+1 {
			rf.sendSnapshot(peer)
			fmt.Printf("%v: processAppendReplyTermL chocie3\n", rf.me)
		}
	}
	rf.advanceCommitL() // Leader rule
}

func (rf *Raft) processAppendReplyL(
	peer int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) {
	DPrintf("%v: processAppendReply from %v %v\n", rf.me, peer, reply)
	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
	} else if rf.currentTerm == args.Term {
		rf.processAppendReplyTermL(peer, args, reply)
	}
}
