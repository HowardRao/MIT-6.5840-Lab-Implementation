package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// ─────────────────────────────────────────────────────────────────────────────
// Timing constants
// §5.2 — election timeout ≫ broadcast time; heartbeat ≪ election timeout.
// Tester constraint: ≤ 10 heartbeats/second, new leader within 5 seconds.
// ─────────────────────────────────────────────────────────────────────────────
const (
	// heartbeatInterval must satisfy: ≤ 100 ms (10 Hz cap from tester).
	heartbeatInterval = 80 * time.Millisecond

	// Election timeout = electionBase + rand(0, electionRange).
	// Range [300, 500] ms satisfies: much larger than heartbeat, and a new
	// leader can be elected well within the 5-second tester deadline even
	// when multiple split votes occur in a row.
	electionBase  = 300 * time.Millisecond
	electionRange = 200 * time.Millisecond
)

type Role int

const (
	Follower  Role = iota // §5.2 — every server starts here
	Candidate             // §5.2 — started an election, waiting for votes
	Leader                // §5.2 — won majority; sends heartbeats
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// ── Persistent state (§Fig2) — persisted from 3C; declared now for correctness
	currentTerm int
	votedFor    int        // -1 = none
	log         []LogEntry // log[0] is the dummy sentinel (term=0)

	// ── Volatile state (all servers)
	// commitIndex and lastApplied are used from 3B; declared now for struct completeness.
	commitIndex int
	lastApplied int

	// ── Role & election state
	role             Role
	electionDeadline time.Time
	votesGranted     int

	dead    int32                 // set atomically by Kill()
	applyCh chan raftapi.ApplyMsg // to upper-layer service (used from 3B)
}

// ─────────────────────────────────────────────────────────────────────────────
// Role transitions
// ─────────────────────────────────────────────────────────────────────────────
// becomeFollower steps this server down and updates term.
// §5.1 — any RPC with a higher term triggers this.
// Caller must hold rf.mu.
func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.currentTerm++ // important to change current term here
	rf.votedFor = rf.me
	rf.votesGranted = 1
	rf.resetElectionDeadline()
}

// becomeLeader initialises leader state after winning an election.
// §5.3 — nextIndex = lastLogIndex+1; matchIndex = 0 for all peers.
// Caller must hold rf.mu.
func (rf *Raft) becomeLeader() {
	rf.role = Leader
	// nextIndex / matchIndex are used in 3B for log replication.
	// We initialise them here so the struct is always consistent.
	// suppress unused warning; arrays added to struct in 3B
	// 3B: rf.nextIndex  = make([]int, n)  — initialise to lastLogIndex+1
	// 3B: rf.matchIndex = make([]int, n)  — initialise to 0
	// 3B: append no-op entry to help commit previous-term entries (§5.4.2)

}

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionDeadline()
}

func (rf *Raft) stayFollower() {
	rf.role = Follower
	rf.resetElectionDeadline()
}

// ─────────────────────────────────────────────────────────────────────────────
// Election
// ─────────────────────────────────────────────────────────────────────────────

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// startElection transitions to Candidate and fans out RequestVote RPCs.
// §5.2 — increment term, self-vote, randomise next deadline, send RPCs.
// Caller must hold rf.mu.  Releases rf.mu before sending RPCs.
func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.becomeCandidate()
	// rf.persist() // 3C

	term := rf.currentTerm
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		}

		go rf.sendRequestVote(i, &args, &RequestVoteReply{})
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
	if !ok {
		return false
	}

	rf.mu.Lock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.mu.Unlock()
		return true
	}

	if rf.role != Candidate || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return true
	}

	won := false
	if reply.VoteGranted {
		rf.votesGranted++
		if rf.votesGranted > len(rf.peers)/2 {
			rf.becomeLeader()
			won = true
		}
	}

	rf.mu.Unlock()

	if won {
		rf.broadcastAppendEntries()
	}

	return true
}

// example RequestVote RPC handler.
// RequestVote — RPC handler (receiver side).
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// §5.1 — update term and step down if behind.
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	// §5.2 — reject if candidate's term is stale.
	if args.Term < rf.currentTerm {
		return
	}

	// §5.2 — vote only if we haven't voted, or already voted for this candidate.
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	// §5.4.1 — candidate's log must be at least as up-to-date as ours.
	// (In 3A the log is always just the sentinel, so this always passes.
	// Having it correct here means 3B requires no changes to this handler.)
	myLastTerm := rf.lastLogTerm()
	myLastIdx := rf.lastLogIndex()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx)
	if !upToDate {
		return
	}

	// Grant the vote.
	rf.votedFor = args.CandidateId
	// 3C: call rf.persist() here
	// §5.2 — reset election timer on granting a vote.
	rf.resetElectionDeadline()
	reply.VoteGranted = true
}

// resetElectionDeadline sets a new randomised deadline.
// §5.2 — randomisation dramatically reduces split-vote probability.
func (rf *Raft) resetElectionDeadline() {
	jitter := time.Duration(rand.Int63n(int64(electionRange)))
	rf.electionDeadline = time.Now().Add(electionBase + jitter)
}

// ─────────────────────────────────────────────────────────────────────────────
// AppendEntries RPC
// ─────────────────────────────────────────────────────────────────────────────

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // empty for heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// XTerm/XIndex/XLen added in 3C for fast log back-off.
}

// broadcastAppendEntries fans out AppendEntries to all peers.
// In 3A, Entries is always empty (pure heartbeat).
// Caller must hold rf.mu.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := rf.lastLogIndex()
	prevLogTerm := rf.lastLogTerm()
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: leaderCommit,
		}

		go rf.sendAppendEntries(i, &args)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// §5.1 — step down if we see a higher term.
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// Stale reply.
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}

	// 3B: on success, advance matchIndex/nextIndex and try to commit.
	// 3B: on failure, back off nextIndex using XTerm/XIndex/XLen.
}

// AppendEntries — RPC handler (receiver side).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// §5.1 — step down if we see a higher term.
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// §5.2 — reject stale leaders.
	if args.Term < rf.currentTerm {
		return
	}

	rf.stayFollower()

	// 3B: consistency check (prevLogIndex / prevLogTerm match), log append,
	//     commitIndex update, and XTerm/XIndex/XLen rejection hints all go here.

	reply.Success = true
}

func (rf *Raft) lastLogIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastLogTerm() int  { return rf.log[len(rf.log)-1].Term }

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// ─────────────────────────────────────────────────────────────────────────────
// ticker — single goroutine driving all timeouts
// ─────────────────────────────────────────────────────────────────────────────
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.step()
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) step() {
	rf.mu.Lock()
	role := rf.role
	expired := time.Now().After(rf.electionDeadline)
	rf.mu.Unlock()

	switch role {
	case Leader:
		rf.broadcastAppendEntries()
	default:
		if expired {
			rf.startElection()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		currentTerm: 0,
		votedFor:    -1,
		// Dummy sentinel at index 0 — term 0 is never a real term, so
		// prevLogIndex=0 / prevLogTerm=0 is always a safe "no previous entry".
		log: []LogEntry{{Term: 0}},

		commitIndex: 0,
		lastApplied: 0,
		role:        Follower,
	}

	rf.resetElectionDeadline()

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
