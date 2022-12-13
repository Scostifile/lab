package raft

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastApplied = 0

	if rf.lastApplied+1 <= rf.log.start() {
		// restart from a snapshot
		rf.lastApplied = rf.log.start()
	}

	for !rf.killed() {
		if rf.waitingSnapshot != nil {
			am := ApplyMsg{}
			DPrintf("%v: deliver snapshot\n", rf.me)
			am.SnapshotValid = true
			am.Snapshot = rf.waitingSnapshot
			am.SnapshotIndex = rf.waitingIndex
			am.SnapshotTerm = rf.waitingTerm

			rf.waitingSnapshot = nil

			rf.mu.Unlock()
			rf.applyCh <- am
			//fmt.Printf("%v: deliver snapshot????????????????????????????????????\n", rf.me)
			rf.mu.Lock()
		} else if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.log.lastindex() &&
			rf.lastApplied+1 > rf.log.start() {
			// every time log append one by one, thus there apply one by one
			rf.lastApplied += 1
			am := ApplyMsg{}
			am.CommandValid = true
			am.CommandIndex = rf.lastApplied
			am.Command = rf.log.entry(rf.lastApplied).Command
			DPrintf("%v: Applier deliver %v\n", rf.me, am.CommandIndex)
			//fmt.Printf("Raft:%v: Applier deliver index=%v\n", rf.me, am.CommandIndex)
			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) signalApplierL() {
	rf.applyCond.Broadcast()
}
