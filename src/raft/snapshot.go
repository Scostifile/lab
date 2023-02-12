package raft

import (
	"fmt"
	"log"
)

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.snapshotIndex >= index ||
		rf.commitIndex < index {
		if rf.log.lastindex() < index || rf.commitIndex < index {
			log.Fatalf("%v: Snapshot Entries lastindex=%v < index=%v\n", rf.me, rf.log.lastindex(), index)
		}
		return
	}
	//fmt.Printf("%v:Snapshot Entries ----begin----is: %v\n", rf.me, rf.Entries.Entries)
	if index >= rf.log.lastindex()+1 {
		rf.snapshotTerm = rf.log.entry(rf.log.lastindex()).Term
		fmt.Printf("%v: Snapshot index=%v >= rfLogLastIndex=%v!!!!!!!!!!!!!!!!!!!!!!!1\n",
			rf.me, index, rf.log.lastindex())
	} else {
		rf.snapshotTerm = rf.log.entry(index).Term
	}

	rf.snapshotIndex = index
	rf.snapshot = snapshot

	tempLog := make([]Entry, rf.log.lastindex()-index+1) // len of Entries include snapshot
	copy(tempLog, rf.log.slice(index))
	tempLog[0] = Entry{Term: rf.log.entry(index).Term, Command: nil} // fix lab4B shardDeletion test
	rf.log = mkLog(tempLog, index)

	if index > rf.commitIndex {
		rf.commitIndex = index
		fmt.Printf("111111111111111111111111111\n")
	}
	if index > rf.lastApplied {
		fmt.Printf("%v: Snapshot index=%v > lastApplied=%v!!\n", rf.me, index, rf.lastApplied)
		rf.lastApplied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// InstallSnapshot RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	switch {
	case args.Term < rf.currentTerm:
		return
	case args.Term > rf.currentTerm:
		DPrintf("%v: InstallSnapShot argTerm=%v\n", rf.me, args.Term)
		rf.newTermL(args.Term)
	case args.Term == rf.currentTerm:
		if rf.state == Leader {
			log.Fatalf("InstallSnapshot Error! Another leader in current term!")
		} else if rf.state == Candidate {
			rf.state = Follower
		}
	}

	//fmt.Printf("%v: InstallSnapShot argTerm=%v, currenttime=%v\n", rf.me, args.Term, rf.currentTerm)

	if args.LastIncludeIndex <= rf.snapshotIndex {
		//fmt.Printf("%v: InstallSnapShot leadersnapshot:%v peersnapshot:%v\n", rf.me, args.LastIncludeIndex, rf.snapshotIndex)
		return
	}
	//rf.setElectionTime()
	rf.resetTimer()

	if args.LastIncludeIndex < rf.log.lastindex() &&
		rf.log.entry(args.LastIncludeIndex).Term == args.LastIncludeTerm {
		index := args.LastIncludeIndex
		tempLog := make([]Entry, rf.log.lastindex()-index+1)
		copy(tempLog, rf.log.slice(index))
		tempLog[0] = Entry{Term: rf.log.entry(index).Term, Command: nil} // fix lab4B shardDeletion test
		//fmt.Printf("%v: InstallSnapshot plus follower logs len:%v, Entries:%v\n", rf.me, len(tempLog), tempLog)
		rf.log = mkLog(tempLog, index)
	} else {
		rf.log = mkLog(make([]Entry, 1), args.LastIncludeIndex)
		rf.log.entry(args.LastIncludeIndex).Term = args.LastIncludeTerm
		//fmt.Printf("%v: InstallSnapshot with no follower Entries\n", rf.me)
	}

	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludeIndex
	rf.snapshotTerm = args.LastIncludeTerm

	rf.waitingSnapshot = args.Data
	rf.waitingIndex = args.LastIncludeIndex
	rf.waitingTerm = args.LastIncludeTerm

	//fmt.Printf("%v: InstallSnapshot end -------- lastincludeterm:%v==%v\n", rf.me, rf.Entries.entry(index).Term, rf.snapshotTerm)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	if args.LastIncludeIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludeIndex
	}

	/* wherever the lastIncludeIndex is bigger than lastApplied or not, it should update the lastApplied = lastIncludeIndex,
	otherwise may cause the situation that s2's DB is {x 0 0 y x 0 1 y}, and append a new Entries entry "x 0 2 y", now its DB is {x 0 0 y x 0 1 y x 0 2 y},
	then read a snapshot by leader so that s2's DB becomes {x 0 0 y x 0 1 y}. It causes the Entries rollback.
	*/
	if args.LastIncludeIndex < rf.lastApplied {
		fmt.Printf("InstallSnapshot arg.lastIncludeIndex=%v < rf.lastApplied=%v\n", args.LastIncludeIndex, rf.lastApplied)
	} else if args.LastIncludeIndex > rf.lastApplied {
		//fmt.Printf("InstallSnapshot arg.lastIncludeIndex=%v > rf.lastApplied=%v\n", args.LastIncludeIndex, rf.lastApplied)
	}

	rf.lastApplied = args.LastIncludeIndex
	rf.signalApplierL()
}

func (rf *Raft) sendSnapshot(server int, currentTerm int) {
	if rf.currentTerm != currentTerm || rf.state != Leader {
		return
	}
	//fmt.Printf("%v: sendSnapshot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1\n", rf.me)
	go func() {
		rf.mu.Lock()
		args := &InstallSnapshotArgs{
			//rf.currentTerm,
			currentTerm,
			rf.me,
			rf.snapshotIndex,
			rf.snapshotTerm,
			rf.snapshot,
		}
		rf.mu.Unlock()
		//fmt.Printf("%v: sendSnapshot  lastincludeindex:%v lastincludeterm:%v to peer:%v\n", rf.me, rf.snapshotIndex, rf.snapshotTerm, server)

		var reply InstallSnapshotReply
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader ||
				rf.currentTerm != args.Term || rf.snapshotIndex != args.LastIncludeIndex {

				//fmt.Printf("%v: sendSnapshot exit1 state=%v currenttime:%v==argsterm:%v snapshotindex:%v argssnapshotindex:%v\n",
				//	rf.me, rf.state, rf.currentTerm, args.Term, rf.snapshotIndex, args.LastIncludeIndex)
				return
			}

			if reply.Term > rf.currentTerm {
				rf.newTermL(reply.Term)
				//fmt.Printf("%v: sendSnapshot exit1 replyterm:%v > currenttime:%v\n", rf.me, reply.Term, rf.currentTerm)
				//rf.resetTimer()
				return
			}

			if rf.nextIndex[server] < args.LastIncludeIndex+1 {
				rf.nextIndex[server] = args.LastIncludeIndex + 1
				//fmt.Printf("%v: sendSnapshot increase nextindex=%v\n", rf.me, rf.nextIndex[server])
			}
			if rf.matchIndex[server] < args.LastIncludeIndex {
				rf.matchIndex[server] = args.LastIncludeIndex
				//fmt.Printf("%v: sendSnapshot increase matchindex=%v\n", rf.me, rf.matchIndex[server])
				//rf.advanceCommitL()
			}
		}
	}()
}
