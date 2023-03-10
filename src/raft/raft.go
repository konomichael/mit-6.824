package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

const (
	MaxRetries       = 3
	HeartbeatTimeout = 5  // 50ms
	ElectionTimeout  = 50 // 500ms
)

// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// Raft is a Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     NodeState
	// persistent state on all servers
	currentTerm       int
	votedFor          int
	logs              []LogEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// channels
	applyCh chan ApplyMsg

	appendChs  []chan int // for each peer, a channel to notify the peer to append logs, the value is retry count
	installChs []chan int // for each peer, a channel to notify the peer to install snapshot, the value is retry count

	commitCh chan struct{}

	tick func()

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	heartbeatTimeout int

	clock *time.Ticker
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() { // with mutex lock
	if data := rf.persistentState(); len(data) > 0 {
		rf.persister.SaveRaftState(data)
	}
}

func (rf *Raft) persistentState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("readPersist error")
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// RequestVoteArgs RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term, reply.VoteGranted = rf.currentTerm, false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.lastTerm() <= args.LastLogTerm && rf.lastIndex() <= args.LastLogIndex {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		rf.persist()
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader := rf.currentTerm, rf.state == Leader
	if !isLeader || rf.killed() {
		return 0, term, isLeader
	}

	index := rf.nextIndex[rf.me]
	DPrintf("%v: start command %v at index %v", rf.me, command, index)

	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = index

	rf.persist()

	rf.replicateLog(rf.currentTerm)
	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.clock.Stop()

	close(rf.commitCh)
	rf.commitCh = nil
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.clock.C:
			rf.tick()
		}
	}
}

func (rf *Raft) tickHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartbeatElapsed++
	rf.electionElapsed++

	if rf.electionElapsed >= rf.electionTimeout {
		rf.becomeCandidate()
	}

	if rf.state != Leader {
		return
	}

	if rf.heartbeatElapsed >= rf.heartbeatTimeout {
		DPrintf("%v: heartbeat timeout", rf.me)
		rf.replicateLog(rf.currentTerm)
	}
}

func (rf *Raft) tickElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionElapsed++

	if rf.electionElapsed >= rf.electionTimeout {
		rf.becomeCandidate()
	}
}

func (rf *Raft) becomeCandidate() { // with mutex.Lock
	if rf.state == Leader {
		return
	}

	rf.state = Candidate
	rf.reset(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.tick = rf.tickHeartbeat
	voteArgs := rf.buildRequestVoteArgs()

	granted := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(i, voteArgs, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.VoteGranted {
					DPrintf("%v: receive vote from %v, granted: %v, term: %v", rf.me, i, reply.VoteGranted, reply.Term)
					granted++
					if granted > len(rf.peers)/2 && rf.state == Candidate && rf.currentTerm == voteArgs.Term && !rf.killed() {
						rf.becomeLeader()
					}
				} else if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() { // with mutex.Lock
	if rf.state == Follower {
		panic(fmt.Sprintf("%v: become leader, but state is follower", rf.me))
	}
	if rf.state == Leader {
		return
	}
	rf.state = Leader
	rf.reset(rf.currentTerm)
	rf.tick = rf.tickHeartbeat

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.firstIndex()
	}
	rf.matchIndex = make([]int, len(rf.peers))

	rf.appendChs = make([]chan int, len(rf.peers))
	for i := range rf.appendChs {
		if i != rf.me {
			rf.appendChs[i] = make(chan int, 1)
			go rf.appendEntries(i, rf.appendChs[i], rf.currentTerm)
		}
	}

	rf.installChs = make([]chan int, len(rf.peers))
	for i := range rf.installChs {
		if i != rf.me {
			rf.installChs[i] = make(chan int, 1)
			go rf.installSnapshot(i, rf.installChs[i], rf.currentTerm)
		}
	}

	DPrintf("%v: become leader, term: %v, start broadcasting appendEntries", rf.me, rf.currentTerm)
	rf.replicateLog(rf.currentTerm)
}

func (rf *Raft) becomeFollower(term int) { // with mutex.Lock
	if rf.state == Leader {
		rf.nextIndex = nil
		rf.matchIndex = nil

		for _, ch := range rf.appendChs {
			if ch != nil {
				close(ch)
			}
		}
		rf.appendChs = nil

		for _, ch := range rf.installChs {
			if ch != nil {
				close(ch)
			}
		}
		rf.installChs = nil
	}

	rf.state = Follower
	rf.reset(term)
	rf.tick = rf.tickElection

	DPrintf("%v: become follower, term: %v", rf.me, rf.currentTerm)
}

func (rf *Raft) reset(term int) {
	if term != rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}

	rf.heartbeatElapsed = 0
	rf.electionElapsed = 0
	rf.electionTimeout = randElectionTimeout()
}

// buildRequestVoteArgs builds the RequestVoteArgs, with mutex.Lock
func (rf *Raft) buildRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
}

// replicateLog replicates logs entries or snapshot to followers
func (rf *Raft) replicateLog(term int) { // without mutex.Lock
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				if rf.state != Leader || rf.currentTerm != term || rf.killed() {
					rf.mu.Unlock()
					return
				}
				prevLogIndex := rf.nextIndex[i] - 1
				// send entries
				if prevLogIndex >= rf.lastIncludedIndex {
					rf.mu.Unlock() // avoid deadlock
					select {
					case rf.appendChs[i] <- 0:
					default:
					}
					return
				}
				// send snapshot
				rf.mu.Unlock()
				select {
				case rf.installChs[i] <- 0:
				default:
				}
			}(i)
		}
	}
}

// applier applies committed log entries to the state machine when commitIndex > lastApplied
func (rf *Raft) applier() {
	for {
		select {
		case _, ok := <-rf.commitCh:
			if !ok {
				return // channel closed
			}
			if rf.killed() {
				return
			}

			rf.mu.Lock()
			firstIndex, commitIndex, lastApplied := rf.firstIndex(), rf.commitIndex, rf.lastApplied
			// check again
			if commitIndex <= lastApplied {
				rf.mu.Unlock()
				continue
			}

			// apply logs
			entries := make([]LogEntry, commitIndex-lastApplied)
			copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
			rf.mu.Unlock()

			// apply to state machine
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index}
			}

			rf.mu.Lock()
			rf.lastApplied = commitIndex
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	if data := rf.persister.ReadRaftState(); len(data) > 0 {
		rf.persister.Save(data, snapshot)
	}
}

func (rf *Raft) doInstallSnapshot(peer, term, retries int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Unlock()
	defer rf.mu.Lock()

	if rf.state != Leader || rf.killed() || rf.currentTerm != term {
		return
	}

	if !ok {
		select {
		case rf.installChs[peer] <- retries + 1:
		default:
		}
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		applyCh:          applyCh,
		appendChs:        make([]chan int, len(peers)),
		installChs:       make([]chan int, len(peers)),
		commitCh:         make(chan struct{}),
		heartbeatTimeout: HeartbeatTimeout,
		clock:            time.NewTicker(10 * time.Millisecond),
	}
	rf.becomeFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
