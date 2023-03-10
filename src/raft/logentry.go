package raft

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// firstIndex returns the first index of the logs
func (rf *Raft) firstIndex() int {
	return rf.lastIncludedIndex + 1
}

// lastIndex returns the last index of the logs
// | lastIncludedIndex | logs[0].Index | logs[1].Index | ... | logs[n].Index |
func (rf *Raft) lastIndex() int {
	if len(rf.logs) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) lastTerm() int {
	if len(rf.logs) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	return lastLogTerm > rf.lastTerm() || (lastLogTerm == rf.lastTerm() && lastLogIndex >= rf.lastIndex())
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
	ConflictLen   int
}

func (rf *Raft) buildAppendEntriesArgs(peer int) *AppendEntriesArgs { // with lock held
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.lastIncludedTerm
	if i := prevLogIndex - rf.lastIncludedIndex - 1; i >= 0 {
		prevLogTerm = rf.logs[i].Term
	}

	var entries []LogEntry
	if prevLogIndex >= rf.lastIncludedIndex && prevLogIndex < rf.lastIndex() {
		entries = make([]LogEntry, len(rf.logs)-prevLogIndex+rf.lastIncludedIndex)
		copy(entries, rf.logs[prevLogIndex-rf.lastIncludedIndex:])
	}

	// garbage collection
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) appendEntries(peer int, c <-chan int, term int) {
	for !rf.killed() {
		select {
		case retries, ok := <-c:
			if !ok {
				DPrintf("%d: channel closed", rf.me)
				return // channel closed
			}

			rf.mu.Lock()
			if rf.state != Leader || rf.killed() || rf.currentTerm != term {
				DPrintf("%d: not leader or killed", rf.me)
				rf.mu.Unlock()
				return
			}

			if retries >= MaxRetries {
				rf.mu.Unlock()
				continue // give up
			}

			args := rf.buildAppendEntriesArgs(peer)
			rf.mu.Unlock()
			go rf.doAppendEntries(peer, term, retries, args)
		}
	}
}

func (rf *Raft) doAppendEntries(peer, term, retries int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed() || rf.currentTerm != term {
		return
	}

	if !ok {
		if len(args.Entries) == 0 { // heartbeat
			return
		}

		if args.PrevLogIndex < rf.nextIndex[peer]-1 { // out of date
			return
		}
		// should retry? or just wait for next heartbeat?
		select {
		case rf.appendChs[peer] <- retries + 1: // retry
		default:
		}
	}

	// after brain split, transfer leadership to the new leader
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		if rf.nextIndex[peer] < args.PrevLogIndex+len(args.Entries)+1 {
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
		}
		if rf.matchIndex[peer] < args.PrevLogIndex+len(args.Entries) {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		}

		go rf.updateCommitIndex(term)
		return
	}

	// deal with conflict
	if reply.ConflictLen > 0 {
		rf.nextIndex[peer] = reply.ConflictLen
		return
	}

	if reply.ConflictTerm > 0 {
		for i := len(rf.logs) - 1; i >= 0; i-- {
			if rf.logs[i].Term == reply.ConflictTerm {
				rf.nextIndex[peer] = rf.logs[i].Index + 1
				return
			}
			if rf.logs[i].Term < reply.ConflictTerm {
				rf.nextIndex[peer] = rf.logs[i].Index
				return
			}
		}
		// no such term, truncate all logs
		rf.nextIndex[peer] = rf.firstIndex()
	}
}

func (rf *Raft) updateCommitIndex(term int) { // without lock held
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed() || rf.currentTerm != term {
		return
	}

	lastIndex := rf.lastIndex()
	if rf.commitIndex >= lastIndex {
		return
	}

	// find the largest N such that N > commitIndex, a majority of matchIndex[i] >= N, and logs[N].term == currentTerm
	commitIndex := rf.commitIndex
	firstIndex := rf.firstIndex()
	for n := commitIndex + 1; n <= lastIndex; n++ {
		if rf.logs[n-firstIndex].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			commitIndex = n
		}
	}

	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		go func() {
			rf.commitCh <- true
		}()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term, reply.Success = rf.currentTerm, false
	if args.Term < rf.currentTerm {
		DPrintf("%d: AppendEntries: term %d < %d, reject", rf.me, args.Term, rf.currentTerm)
		return
	}

	rf.becomeFollower(args.Term)

	if lastLogIndex := rf.lastIndex(); args.PrevLogIndex > lastLogIndex {
		reply.ConflictLen = lastLogIndex + 1
		return
	}

	var (
		prevLogTerm  int
		prevLogIndex int
		entries      []LogEntry
	)
	if args.PrevLogIndex < rf.lastIncludedIndex {
		prevLogIndex = rf.lastIncludedIndex
		prevLogTerm = rf.lastIncludedTerm
		for i := range args.Entries {
			if args.Entries[i].Index == rf.lastIncludedIndex && args.Entries[i].Term == rf.lastIncludedTerm {
				entries = args.Entries[i+1:]
				break
			}
		}
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		prevLogIndex = rf.lastIncludedIndex
		prevLogTerm = rf.lastIncludedTerm
		entries = args.Entries
	} else {
		prevLogIndex = args.PrevLogIndex
		prevLogTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
		entries = args.Entries
	}

	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevLogTerm
		for i := prevLogIndex; i > rf.lastIncludedIndex; i-- {
			reply.ConflictIndex = i
			if rf.logs[i-rf.lastIncludedIndex-1].Term != prevLogTerm {
				break
			}
		}
		return
	}

	reply.Success = true

	firstIndex := rf.firstIndex()
	for i, entry := range entries {
		if j := entry.Index - firstIndex; j >= len(rf.logs) || rf.logs[j].Term != entry.Term {
			rf.logs = rf.logs[:j]
			rf.logs = append(rf.logs, entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
		go func() {
			rf.commitCh <- true
		}()
	}

	rf.persist() // persist after update commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	defer DPrintf("%d: sendAppendEntries: %d, %v, %v", rf.me, server, args, reply)
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}
