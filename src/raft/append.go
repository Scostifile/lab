package raft

import (
	"fmt"
	"log"
)

type AppendEntriesArgs struct {
	Term         int     // Leader's term
	LeaderId     int     // who is the leader
	PrevLogIndex int     // index immediately preceding new ones
	PrevLogTerm  int     // term of PrevLogIndex entry
	Entries      []Entry // new Log entries
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
	defer func() { reply.Term = rf.currentTerm }()

	reply.Success = false
	reply.ConflictValid = false
	reply.ConflictFirst = -1
	reply.ConflictTerm = -1

	switch {
	case args.Term > rf.currentTerm:
		rf.newTermL(args.Term)
		DPrintf("%v: AppendEntries argsTerm=%v > peercurrentTerm=%v\n", rf.me, args.Term, rf.currentTerm)
	case args.Term < rf.currentTerm:
		DPrintf("%v: AppendEntries rfTerm=%v\n", rf.me, rf.currentTerm)
		return
	case args.Term == rf.currentTerm:
		if rf.state != Follower {
			rf.state = Follower
		}
		if rf.state == Leader {
			fmt.Printf("AppendEntries Error! Another leader:%v in current term:%v! Receive from:%v\n",
				rf.me, rf.currentTerm, args.LeaderId)
		}
	}

	//rf.setElectionTime()

	// for 2D
	if args.PrevLogIndex < rf.snapshotIndex {
		//fmt.Printf("%v: AppendEntries from leader:%v Term:%v peersnapshotindex:%v > leaderprevlogindex:%v\n", rf.me, args.LeaderId, rf.currentTerm, rf.snapshotIndex, args.PrevLogIndex)

		// outdated message because of unreliable network
		reply.ConflictValid = true
		//reply.ConflictFirst = rf.Entries.lastindex() + 1
		return
	}
	rf.resetTimer()

	// Rule 2
	if rf.log.lastindex() < args.PrevLogIndex {
		//fmt.Printf("%v: AppendEntries rule2 lastindex:%v, previndex:%v\n", rf.me, rf.Entries.lastindex(), args.PrevLogIndex)

		reply.ConflictValid = true
		reply.ConflictFirst = rf.log.lastindex() + 1
		return
	}

	// optimization of conflict Entries entry
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		//fmt.Printf("%v: AppendEntries optimization of conflict peerlogTerm:%v leaderlogTerm:%v previndex:%v logindex0:%v\n",
		//	rf.me, rf.Entries.entry(args.PrevLogIndex).Term, args.PrevLogTerm, args.PrevLogIndex, rf.Entries.start())
		reply.ConflictValid = true
		reply.ConflictTerm = rf.log.entry(args.PrevLogIndex).Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex >= rf.log.start() &&
			rf.log.entry(conflictIndex).Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictFirst = conflictIndex + 1
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
		//if reflect.DeepEqual(rf.Entries.entry(index).Command, e.Command) {
		//	Entries.Fatalf("Entry Error %v from=%v index=%v old=%v new=%v\n",
		//		rf.me, args.LeaderId, index, rf.Entries.entry(index), args.Entries[i])
		//}
	}
	rf.persist()

	// Rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		lastNew := args.PrevLogIndex + 1 + len(args.Entries) - 1
		//if rf.commitIndex > lastNew {
		//	DPrintf("%v: Commit to %v\n", rf.me, lastNew)
		//	rf.commitIndex = lastNew
		//}

		if rf.commitIndex > rf.log.lastindex() {
			if lastNew != rf.log.lastindex() {
				log.Fatalf("%v: AppendEntries lastNew != rf.Entries.lastIndex!!!\n", rf.me)
			}
			rf.commitIndex = rf.log.lastindex()
		}

		rf.signalApplierL()
	}
	//fmt.Printf("%v AppendEntries receive from server=%v goroutine=%v\n", rf.me, args.LeaderId, runtime.NumGoroutine())
}

//func (rf *Raft) sendAppendsL(heartbeat bool) {
//	for i, _ := range rf.peers {
//		if i != rf.me {
//			if rf.Entries.lastindex() > rf.nextIndex[i] || heartbeat {
//				go rf.sendAppendL(i, heartbeat, rf.currentTerm)
//			}
//		}
//	}
//}

func (rf *Raft) sendAppendL(
	peer int,
	heartbeat bool,
	currentTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != currentTerm || rf.state != Leader {
		fmt.Printf("%v: sendAppendL to peer=%v now is not a leader in rf.term%v==currentTerm%v, its state=%v\n",
			rf.me, peer, rf.currentTerm, currentTerm, rf.state)
		return
	}
	next := rf.nextIndex[peer]

	if next <= rf.log.start() { // <= because always skip entry "0"
		next = rf.log.start() + 1
	}

	// ?? maybe not execute
	if next > rf.log.lastindex()+1 {
		fmt.Printf("%v: sendAppendL nextIndex[%v]=%v > lastindex=%v, Index0=%v\n",
			rf.me, peer, rf.nextIndex[peer], rf.log.lastindex(), rf.log.start())
		next = rf.log.lastindex() + 1
	}

	if next <= rf.snapshotIndex && rf.snapshotIndex > 0 {
		//if next <= rf.snapshotIndex {
		rf.sendSnapshot(peer, currentTerm)
		//fmt.Printf("%v: sendAppendL next:%v < lastincludeindex:%v, sendSnapshot to %v\n",
		//	rf.me, next, rf.snapshotIndex, peer)
		return
	}

	args := &AppendEntriesArgs{
		//rf.currentTerm,
		currentTerm,
		rf.me,
		next - 1,
		rf.log.entry(next - 1).Term,
		make([]Entry, rf.log.lastindex()-next+1),
		rf.commitIndex,
	}
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
	DPrintf("%v: sendAppend to %v: %v\n", rf.me, peer, args)
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) advanceCommitL() {
	if rf.state != Leader {
		//Entries.Fatalf("advanceCommit: state %v\n", rf.state)
		fmt.Printf("%v: advanceCommit: rf.state%v != leader\n", rf.me, rf.state)
		return
	}

	//start := rf.commitIndex + 1
	//if start < rf.Entries.start() { // on restart start could be 1
	//	start = rf.Entries.start()
	//}
	//
	//for index := start; index <= rf.Entries.lastindex(); index++ {
	//	if rf.Entries.entry(index).Term != rf.currentTerm {
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

	//matchLen := len(rf.peers)
	//tempMatchIndex := make([]int, matchLen-1)
	//j := 0
	//for i := 0; i < matchLen; i++ {
	//	if i != rf.me {
	//		tempMatchIndex[j] = rf.matchIndex[i]
	//		j++
	//	}
	//}
	//sort.Ints(tempMatchIndex)
	//medianIndex := tempMatchIndex[matchLen/2]
	//fmt.Printf("%v: advanceCommit tempMatchIndex:%v medianIndex:%v\n", rf.me, tempMatchIndex, medianIndex)
	//if (medianIndex - rf.Entries.Index0) >= len(rf.Entries.Entries) {
	//	fmt.Printf("%v: advanceCommit tempMatchIndex:%v medianIndex:%v Index0:%v logLen:%v\n",
	//		rf.me, tempMatchIndex, medianIndex, rf.Entries.Index0, len(rf.Entries.Entries))
	//}

	medianIndex := getMidLargeIndexBySort(rf.matchIndex)
	//medianIndex := getMidLargeIndexByHeap(rf.matchIndex, len(rf.peers)/2)

	// may happen in situation of figure8, when the previous leader A had replicated Entries
	// entries(e.g. index: 0~16) in majority servers but didn't commit them in term 7.
	// Then the previous leader A crash, and other server B became leader in term 8(whose Entries
	// index is from 0~9). Leader B update its Entries(0~11) so that A's Entries was cut whose Entries
	// entries index was from 0~11 in term 8. And server A became leader in term 9, when it
	// operated till this code, its rf.matachIndex[peer] may be until index 16, thus it would
	// enter into fellow if() condition.
	if (medianIndex - rf.log.Index0) >= len(rf.log.Entries) {
		fmt.Printf("%v: advanceCommit medianIndex:%v Index0:%v logLen:%v\n",
			rf.me, medianIndex, rf.log.Index0, len(rf.log.Entries))
	}

	//if medianIndex > rf.commitIndex && rf.Entries.entry(medianIndex).Term == rf.currentTerm {
	if medianIndex > rf.commitIndex &&
		medianIndex <= rf.log.lastindex() &&
		rf.log.entry(medianIndex).Term == rf.currentTerm {
		rf.commitIndex = medianIndex
		if rf.commitIndex > rf.lastApplied {
			rf.signalApplierL()
		}
	}
}

func (rf *Raft) processConflictTermL(
	peer int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) {

	//if reply.ConflictTerm > rf.currentTerm { // follower's Entries ahead but isn't committed, thus it can be erased
	//	DPrintf("%v: processConflictTermL: reset nextIndex %v to %v\n",
	//		rf.me, peer, reply.ConflictFirst)
	//	rf.nextIndex[peer] = reply.ConflictFirst
	//	fmt.Printf("%v: processConflictTermL: reset nextIndex %v to %v\n",
	//		rf.me, peer, reply.ConflictFirst)
	//} else {
	//	if rf.nextIndex[peer] > rf.Entries.lastindex() {
	//		rf.nextIndex[peer] = rf.Entries.lastindex()
	//	}
	//	DPrintf("%v: processConflictTermL: reset nextIndex %v to term %v,"+
	//		"starting %v Entries %v\n",
	//		rf.me, peer, reply.ConflictTerm, rf.nextIndex[peer], rf.Entries)
	//	for {
	//		if rf.nextIndex[peer] < rf.Entries.start()+1 || rf.Entries.entry(rf.nextIndex[peer]).Term < reply.ConflictTerm { // missed everything?
	//			break
	//		}
	//		if rf.Entries.entry(rf.nextIndex[peer]).Term == reply.ConflictTerm {
	//			break
	//		}
	//		rf.nextIndex[peer] -= 1
	//	}
	//	DPrintf("%v: processConflictTermL: back up follower to %v\n", rf.me, rf.nextIndex[peer])
	//}

	if reply.ConflictTerm == -1 {
		if reply.ConflictFirst == -1 {
			// outdated message
			return
		}
		rf.nextIndex[peer] = reply.ConflictFirst
	} else {
		lastLogIndex := -1
		for i := reply.ConflictFirst; i > rf.log.start(); i-- {
			if rf.log.entry(i).Term == reply.ConflictTerm {
				lastLogIndex = i
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
		rf.sendSnapshot(peer, args.Term)
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
		rf.advanceCommitL()
	} else if reply.ConflictValid {
		rf.processConflictTermL(peer, args, reply)
		//go rf.sendAppendL(peer, false, rf.currentTerm)
		//fmt.Printf("%v: processAppendReplyL reply.ConflictValid next:%v Index0:%v==%v\n", rf.me, rf.nextIndex[peer], rf.Entries.start(), rf.Entries.Index0)
	} else if rf.nextIndex[peer] > 1 {
		DPrintf("%v: processAppendReplyL backup by one\n", rf.me)
		//rf.nextIndex[peer] -= 1
		fmt.Printf("%v: processAppendReplyL next:%v Index0:%v==%v\n", rf.me, rf.nextIndex[peer], rf.log.start(), rf.log.Index0)
		if rf.nextIndex[peer] < rf.log.start()+1 {
			rf.sendSnapshot(peer, args.Term)
			fmt.Printf("%v: processAppendReplyTermL chocie3\n", rf.me)
		}
	}
	//rf.advanceCommitL() // Leader rule
}

func (rf *Raft) processAppendReplyL(
	peer int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) {
	DPrintf("%v: processAppendReply from %v %v\n", rf.me, peer, reply)

	if reply.Term > rf.currentTerm {
		rf.newTermL(reply.Term)
		// fix lab3
		//rf.resetTimer()
	} else if rf.currentTerm == args.Term {
		rf.processAppendReplyTermL(peer, args, reply)
	}
}
