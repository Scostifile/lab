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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.persister.RaftStateSize() > 8000 {
		fmt.Printf("%v: Snapshot before size is %v\n", rf.me, rf.persister.RaftStateSize())
	}

	if rf.snapshotIndex >= index || rf.commitIndex < index || rf.log.lastindex() < index {
		//fmt.Printf("todaybug!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!111\n")
		if rf.log.lastindex() < index {
			log.Fatalf("%v: Snapshot log lastindex=%v < index=%v\n", rf.me, rf.log.lastindex(), index)
		}
		return
	}

	rf.snapshotTerm = rf.log.entry(index).Term
	rf.snapshotIndex = index
	rf.snapshot = snapshot

	//fmt.Printf("%v: snapshot old rfloglen=%v\n", rf.me, len(rf.log.log))
	tempLog := make([]Entry, rf.log.lastindex()-index+1) // len of log include snapshot
	copy(tempLog, rf.log.slice(index))
	rf.log = mkLog(tempLog, index)
	//fmt.Printf("%v: Snapshot until index:%v, oldlastIncludeIndex:%v, oldlastIncludeTerm:%v temploglen:%v rfloglen:%v\n",
	//	rf.me, index, rf.snapshotIndex, rf.snapshotTerm, len(tempLog), len(rf.log.log))

	if index > rf.commitIndex {
		rf.commitIndex = index
		fmt.Printf("111111111111111111111111111\n")
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
		fmt.Printf("2222222222222222222222222222\n")
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
	//fmt.Printf("%v: snapshot lastincludeIndex:%v lastincludeTerm:%v\n", rf.me, rf.snapshotIndex, rf.snapshotTerm)
	if rf.persister.RaftStateSize() > 8000 {
		fmt.Printf("%v: Snapshot after size is %v\n", rf.me, rf.persister.RaftStateSize())
	}

	if rf.lastApplied < rf.log.start() {
		fmt.Printf("%v: Snapshot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! lastAppliedIndex=%v < logstartIndex:%v commitIndex=%v loglasttIndex:%v \n",
			rf.me, rf.lastApplied, rf.log.start(), rf.commitIndex, rf.log.lastindex())
	}
}

// InstallSnapshot RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	if rf.persister.RaftStateSize() > 8000 {
		fmt.Printf("%v: InstallSnapshot before size is %v\n", rf.me, rf.persister.RaftStateSize())
	}

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
	rf.setElectionTime()

	if args.LastIncludeIndex < rf.log.lastindex() &&
		rf.log.entry(args.LastIncludeIndex).Term == args.LastIncludeTerm {
		index := args.LastIncludeIndex
		tempLog := make([]Entry, rf.log.lastindex()-index+1)
		copy(tempLog, rf.log.slice(index))
		//fmt.Printf("%v: InstallSnapshot plus followe logs len:%v, log:%v\n", rf.me, len(tempLog), tempLog)
		rf.log = mkLog(tempLog, index)
	} else {
		rf.log = mkLog(make([]Entry, 1), args.LastIncludeIndex)
		rf.log.entry(args.LastIncludeIndex).Term = args.LastIncludeTerm
		//fmt.Printf("%v: InstallSnapshot with no follower log\n", rf.me)
	}

	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludeIndex
	rf.snapshotTerm = args.LastIncludeTerm

	rf.waitingSnapshot = args.Data
	rf.waitingIndex = args.LastIncludeIndex
	rf.waitingTerm = args.LastIncludeTerm

	if args.LastIncludeIndex != rf.log.start() {
		fmt.Printf("%v: InstallSnapshot argslastIncludeIndex=%v != logstartIndex:%v!!!!!!!!!!!!!!!!!!!!!\n",
			rf.me, args.LastIncludeIndex, rf.log.start())
	}
	//fmt.Printf("%v: InstallSnapshot end -------- lastincludeterm:%v==%v\n", rf.me, rf.log.entry(index).Term, rf.snapshotTerm)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	rf.signalApplierL()

	if rf.persister.RaftStateSize() > 8000 {
		fmt.Printf("%v: InstallSnapshot after size is %v\n", rf.me, rf.persister.RaftStateSize())
	}

	if args.LastIncludeIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludeIndex
	}
	//if args.LastIncludeIndex > rf.lastApplied {
	//	rf.lastApplied = args.LastIncludeIndex
	//}

	/* wherever the lastIncludeIndex is bigger than lastApplied or not, it should update the lastApplied = lastIncludeIndex,
	otherwise may cause the situation that s2's DB is {x 0 0 y x 0 1 y}, and append a new log entry "x 0 2 y", now its DB is {x 0 0 y x 0 1 y x 0 2 y},
	then read a snapshot by leader so that s2's DB becomes {x 0 0 y x 0 1 y}. It causes the log rollback.
	*/
	rf.lastApplied = args.LastIncludeIndex

	if rf.lastApplied < rf.log.start() {
		fmt.Printf("%v: InstallSnapshot after !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! lastAppliedIndex=%v < logstartIndex:%v args.lastIncludeIndex:%v commitIndex=%v loglasttIndex:%v \n",
			rf.me, rf.lastApplied, rf.log.start(), args.LastIncludeIndex, rf.commitIndex, rf.log.lastindex())
	}
}

func (rf *Raft) sendSnapshot(server int) {
	if rf.state != Leader {
		return
	}
	//fmt.Printf("%v: sendSnapshot !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1\n", rf.me)
	go func() {
		rf.mu.Lock()
		args := &InstallSnapshotArgs{
			rf.currentTerm,
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
				rf.currentTerm != args.Term ||
				rf.snapshotIndex != args.LastIncludeIndex {
				//fmt.Printf("%v: sendSnapshot exit1 state=%v currenttime:%v==argsterm:%v snapshotindex:%v argssnapshotindex:%v\n",
				//	rf.me, rf.state, rf.currentTerm, args.Term, rf.snapshotIndex, args.LastIncludeIndex)
				return
			}

			if reply.Term > rf.currentTerm {
				rf.newTermL(reply.Term)
				//fmt.Printf("%v: sendSnapshot exit1 replyterm:%v > currenttime:%v\n", rf.me, reply.Term, rf.currentTerm)
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
