package raft

type SnapshotCmd struct {
	Index    int
	Snapshot []byte
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) buildInstallSnapshotArgs() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	return args
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	// |rf.lastIncludedIndex| rf.logs[0]|rf.logs[1]|...|rf.logs[n]|
	//                                   args.LastIncludedIndex
	entries := make([]LogEntry, len(rf.logs)-args.LastIncludedIndex+rf.lastIncludedIndex)
	copy(entries, rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:])
	rf.logs = entries // for garbage collection

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	// accept snapshot
	rf.saveStateAndSnapshot(args.Data)

	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	// keep a mind that we should not hold the lock when calling rf.applyCh <- applyMsg
	rf.applyCh <- applyMsg
}

// leader calls this function to send InstallSnapshot RPC to follower
func (rf *Raft) installSnapshot(i int, c <-chan int, term int) {
	for {
		select {
		case retries, ok := <-c:
			if !ok {
				return // channel closed
			}

			rf.mu.Lock()
			if rf.state != Leader || rf.killed() || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			if retries >= MaxRetries {
				rf.mu.Unlock()
				continue // give up
			}

			args := rf.buildInstallSnapshotArgs()
			rf.mu.Unlock()
			go rf.doInstallSnapshot(i, term, retries, args)
		}
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the logs through (and including)
// that index. Raft should now trim its logs as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		DPrintf("Snapshot: index %v <= lastIncludedIndex %v, no need to snapshot", index, rf.lastIncludedIndex)
		return
	}

	entries := make([]LogEntry, len(rf.logs)-index+rf.lastIncludedIndex)
	copy(entries, rf.logs[index-rf.lastIncludedIndex:])
	rf.logs = entries // for garbage collection
	rf.lastIncludedIndex = index

	rf.saveStateAndSnapshot(snapshot)
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("sendInstallSnapshot: %v -> %v", rf.me, peer)
	return rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
}
