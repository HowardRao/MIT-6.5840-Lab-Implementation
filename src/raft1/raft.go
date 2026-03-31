// raft.go — MIT 6.5840 Spring 2026  ·  Labs 3A + 3B + 3C + 3D  (FINAL)
// src/raft1/raft.go
//
// ═══════════════════════════════════════════════════════════════════════════════
// ARCHITECTURE OVERVIEW
// ═══════════════════════════════════════════════════════════════════════════════
//
// Five layers (etcd-inspired):
//
//   § 1–3   Types         Constants · Role · LogEntry
//   § 4     raftLog       All log/index arithmetic in one struct
//   § 5     Progress      Per-peer replication tracking (next/match)
//   § 6–7   Raft struct   Field layout + constructor
//   § 8     Public API    GetState · Start · Kill · Snapshot · PersistBytes
//   § 9–10  State machine become* transitions + step* dispatch functions
//   § 11    Persistence   persist / readPersist  (3C)
//   § 12–13 Timing        Election deadline + ticker goroutine
//   § 14    Replication   replicateAll / replicateTo  (3D: snapshot branch)
//   § 15    Commit        maybeAdvanceCommitIndex / applyBackoff
//   § 16    Applier       Decoupled apply goroutine  (3D: snapshot path)
//   § 17    RequestVote   RPC sender + handler
//   § 18    AppendEntries RPC sender + handler
//   § 19    InstallSnapshot  RPC sender + handler  (NEW in 3D)
//   § 20    Utility

package raft

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// ───────────────────────────────────────────────────────────────────────────────
// § 1  CONSTANTS
// ───────────────────────────────────────────────────────────────────────────────

const (
	heartbeatInterval  = 80 * time.Millisecond
	electionMinTimeout = 300 * time.Millisecond
	electionMaxTimeout = 500 * time.Millisecond
)

// ───────────────────────────────────────────────────────────────────────────────
// § 2  ROLE
// ───────────────────────────────────────────────────────────────────────────────

type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

// ───────────────────────────────────────────────────────────────────────────────
// § 3  LOG ENTRY
// ───────────────────────────────────────────────────────────────────────────────

// LogEntry must be exported: labgob encodes it by reflection.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ───────────────────────────────────────────────────────────────────────────────
// § 4  RAFT LOG
//
// Encapsulates all log index arithmetic.  No code outside this section touches
// l.entries directly.
//
// Sentinel invariant:
//   entries[0].Term == snapshotTerm  (always)
//   entries[0].Command == nil        (always)
//   conceptual index i lives at entries[i - snapshotIndex]
//
// Fresh server:  snapshotIndex=0, snapshotTerm=0, entries=[{0,nil}]
// After Snapshot(k): snapshotIndex=k, sentinel has term of entry k.
// ───────────────────────────────────────────────────────────────────────────────

type raftLog struct {
	entries       []LogEntry // entries[0] is always the sentinel
	snapshotIndex int        // conceptual index of the sentinel
	snapshotTerm  int        // term of the sentinel
}

func newRaftLog() raftLog {
	return raftLog{entries: []LogEntry{{Term: 0}}}
}

// lastIndex returns the conceptual index of the last entry.
func (l *raftLog) lastIndex() int { return l.snapshotIndex + len(l.entries) - 1 }

// lastTerm returns the term of the last entry.
func (l *raftLog) lastTerm() int { return l.entries[len(l.entries)-1].Term }

// term returns the term at conceptual index i.
// i == snapshotIndex  →  snapshotTerm (sentinel).
func (l *raftLog) term(i int) int {
	if i == l.snapshotIndex {
		return l.snapshotTerm
	}
	return l.entries[i-l.snapshotIndex].Term
}

// entry returns the LogEntry at conceptual index i.
func (l *raftLog) entry(i int) LogEntry {
	return l.entries[i-l.snapshotIndex]
}

// slice returns a copy of entries[lo, hi).
// Caller must ensure lo > snapshotIndex and hi ≤ lastIndex()+1.
func (l *raftLog) slice(lo, hi int) []LogEntry {
	out := make([]LogEntry, hi-lo)
	copy(out, l.entries[lo-l.snapshotIndex:hi-l.snapshotIndex])
	return out
}

// append appends entries and returns the new lastIndex.
func (l *raftLog) append(entries ...LogEntry) int {
	l.entries = append(l.entries, entries...)
	return l.lastIndex()
}

// truncateAndAppend deletes entries from cutAt onward, then appends new ones.
// Implements §Fig2 AppendEntries rule 3.
func (l *raftLog) truncateAndAppend(cutAt int, entries []LogEntry) {
	l.entries = append(l.entries[:cutAt-l.snapshotIndex], entries...)
}

// trimPrefix discards all entries up to and including idx, making idx the new
// sentinel.  Used by Snapshot() and InstallSnapshot handler.
func (l *raftLog) trimPrefix(idx int, term int) {
	if idx <= l.snapshotIndex {
		return // already trimmed at least this far
	}
	if idx <= l.lastIndex() {
		// Keep the tail from idx onward; entries[0] becomes the new sentinel.
		l.entries = l.entries[idx-l.snapshotIndex:]
	} else {
		// The snapshot covers everything we have; discard the whole log.
		l.entries = []LogEntry{{Term: term}}
	}
	l.entries[0] = LogEntry{Term: term} // set sentinel term; clear Command
	l.snapshotIndex = idx
	l.snapshotTerm = term
}

// ───────────────────────────────────────────────────────────────────────────────
// § 5  PROGRESS
//
// Tracks replication state for one peer.
// Invariant: match < next always.
// ───────────────────────────────────────────────────────────────────────────────

type Progress struct {
	next  int // optimistic: next index to send
	match int // conservative: highest confirmed index
}

// maybeUpdate advances match (and next if needed) if newMatch is larger.
// Returns true when match actually changed — caller should try to advance commit.
func (p *Progress) maybeUpdate(newMatch int) bool {
	if newMatch <= p.match {
		return false
	}
	p.match = newMatch
	if p.next <= newMatch {
		p.next = newMatch + 1
	}
	return true
}

// decreaseTo sets next to n, clamped to match+1 as the safety floor.
func (p *Progress) decreaseTo(n int) {
	if n > p.match {
		p.next = n
	} else {
		p.next = p.match + 1
	}
}

// ───────────────────────────────────────────────────────────────────────────────
// § 6  RAFT STRUCT
// ───────────────────────────────────────────────────────────────────────────────

// stepFunc is the etcd-inspired role-dispatch function pointer.
// Installed by each become* transition; invoked by RPC handlers.
type stepFunc func(rf *Raft, msg interface{})

type Raft struct {
	// ── Infrastructure ──────────────────────────────────────────────────────
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond // signals the applier goroutine

	// ── Persistent state (§Fig2) ────────────────────────────────────────────
	currentTerm int
	votedFor    int     // -1 = none
	log         raftLog // all index arithmetic goes through raftLog methods

	// ── Volatile state — all servers ────────────────────────────────────────
	commitIndex int
	lastApplied int

	// ── Volatile state — leaders only ───────────────────────────────────────
	progress []Progress // nil when not leader

	// ── Role state machine ───────────────────────────────────────────────────
	role     Role
	stepFunc stepFunc

	// ── Election ─────────────────────────────────────────────────────────────
	electionDeadline time.Time
	votesGranted     int

	// ── Snapshot ─────────────────────────────────────────────────────────────
	// Raw snapshot bytes from the service layer.  Written by Snapshot() and
	// InstallSnapshot handler; read by persist() and the applier goroutine.
	snapshot []byte
}

// ───────────────────────────────────────────────────────────────────────────────
// § 7  CONSTRUCTOR
// ───────────────────────────────────────────────────────────────────────────────

func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *tester.Persister,
	applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {

	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		applyCh:     applyCh,
		currentTerm: 0,
		votedFor:    -1,
		log:         newRaftLog(),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// Install follower step function before touching persisted state.
	rf.becomeFollowerLocked(0, false /*no persist during init*/)

	// Recover Raft state (currentTerm, votedFor, log, snapshotIndex/Term).
	rf.readPersist(persister.ReadRaftState())

	// Recover snapshot bytes.  If present, the service layer will restore its
	// state from rf.snapshot via the first SnapshotValid ApplyMsg the applier
	// sends.  We initialise volatile indices at the snapshot boundary so the
	// applier starts correctly.
	if snap := persister.ReadSnapshot(); len(snap) > 0 {
		rf.snapshot = snap
		rf.lastApplied = rf.log.snapshotIndex
		rf.commitIndex = rf.log.snapshotIndex
	}

	rf.resetElectionDeadline()
	go rf.ticker()
	go rf.applier()
	return rf
}

// ───────────────────────────────────────────────────────────────────────────────
// § 8  PUBLIC API
// ───────────────────────────────────────────────────────────────────────────────

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == RoleLeader
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Start proposes a new command to the replicated log.
// Returns (index, term, isLeader).  Non-leaders return (0, 0, false).
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RoleLeader {
		return 0, 0, false
	}
	index := rf.log.append(LogEntry{Term: rf.currentTerm, Command: command})
	rf.progress[rf.me].maybeUpdate(index)
	rf.persist()
	rf.replicateAll()
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Snapshot is called by the service when it has compacted state up through
// `index`.  We trim the log and persist the new state atomically.
//
// §7 — "The service calls Snapshot() on every peer, not just the leader."
// The tester calls this on all servers after entries are applied.
//
// Invariant S1: ignore if index ≤ snapshotIndex (already covered).
// Invariant S2: persist() writes rf.snapshot as the second argument so the
//
//	trimmed log and the new snapshot are always written together.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.log.snapshotIndex {
		return // idempotent: already at or past this point
	}

	// Trim the log prefix up through index.  raftLog.trimPrefix() handles the
	// case where index > lastIndex (discard everything and reset to fresh sentinel).
	term := rf.log.term(index)
	rf.log.trimPrefix(index, term)
	rf.snapshot = snapshot

	// Persist both the trimmed Raft state and the new snapshot atomically.
	// After this call, a crash-restart will load the trimmed log and the new
	// snapshot, so the server comes back up at the right point.
	rf.persist()
}

// ───────────────────────────────────────────────────────────────────────────────
// § 9  ROLE STATE MACHINE
// ───────────────────────────────────────────────────────────────────────────────

// becomeFollowerLocked transitions to follower.
// doPersist=false only during Make() initialisation.
// Caller must hold rf.mu.
func (rf *Raft) becomeFollowerLocked(term int, doPersist bool) {
	rf.role = RoleFollower
	rf.stepFunc = stepFollower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	if doPersist {
		rf.persist()
	}
}

// becomeCandidateLocked starts a new election.
// Increments term, votes for self, fans out RequestVote RPCs.
// Releases rf.mu before sending RPCs; goroutines re-acquire on reply.
// Caller must hold rf.mu on entry.
func (rf *Raft) becomeCandidateLocked() {
	rf.role = RoleCandidate
	rf.stepFunc = stepCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesGranted = 1
	rf.resetElectionDeadline()
	rf.persist()

	term, lastIdx, lastTerm := rf.currentTerm, rf.log.lastIndex(), rf.log.lastTerm()
	rf.mu.Unlock() // release before blocking on network

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, term, lastIdx, lastTerm)
		}
	}
	// mu remains unlocked; ticker() re-acquires it on the next iteration.
}

// becomeLeaderLocked transitions to leader, initialises Progress arrays,
// appends the §5.4.2 no-op, and begins replication.
// Caller must hold rf.mu.
func (rf *Raft) becomeLeaderLocked() {
	rf.role = RoleLeader
	rf.stepFunc = stepLeader

	n := len(rf.peers)
	rf.progress = make([]Progress, n)
	last := rf.log.lastIndex()

	for i := range rf.progress {
		rf.progress[i] = Progress{
			next:  last + 1,
			match: 0,
		}
	}

	// in this lab, no-op is nonsense
	rf.progress[rf.me] = Progress{
		next:  last + 1,
		match: last,
	}

	// do not
	// rf.log.append(...)
	// rf.progress[rf.me].maybeUpdate(...)

	rf.persist()

	// 立即发心跳（空 AppendEntries）
	rf.replicateAll()
}

// ───────────────────────────────────────────────────────────────────────────────
// § 10  STEP FUNCTIONS  (etcd pattern: one function per role)
// ───────────────────────────────────────────────────────────────────────────────

func stepFollower(_ *Raft, _ interface{}) {}

func stepCandidate(rf *Raft, msgI interface{}) {
	reply := msgI.(*RequestVoteReply)
	if !reply.VoteGranted {
		return
	}
	rf.votesGranted++
	if rf.votesGranted > len(rf.peers)/2 {
		rf.becomeLeaderLocked()
	}
}

// stepLeader handles both AppendEntries replies and InstallSnapshot replies.
func stepLeader(rf *Raft, msgI interface{}) {
	switch r := msgI.(type) {
	case *appendEntriesResult:
		if r.reply.Success {
			if rf.progress[r.peer].maybeUpdate(r.matchIfSuccess) {
				rf.maybeAdvanceCommitIndex()
			}
		} else {
			rf.applyBackoff(r.peer, r.reply)
		}
	case *installSnapshotResult:
		// Advance the peer's progress to the snapshot boundary.
		if rf.progress[r.peer].maybeUpdate(r.snapshotIndex) {
			rf.maybeAdvanceCommitIndex()
		}
	}
}

// ───────────────────────────────────────────────────────────────────────────────
// § 11  PERSISTENCE
// ───────────────────────────────────────────────────────────────────────────────

// persist writes currentTerm, votedFor, and log to stable storage.
// The snapshot is written as the second argument so trimmed log and snapshot
// are always consistent after a crash-restart.
// Caller must hold rf.mu.
//
// Encoding order (must match readPersist exactly):
//  1. currentTerm
//  2. votedFor
//  3. log.entries      ([]LogEntry including sentinel)
//  4. log.snapshotIndex
//  5. log.snapshotTerm
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log.entries) != nil ||
		e.Encode(rf.log.snapshotIndex) != nil ||
		e.Encode(rf.log.snapshotTerm) != nil {
		panic("raft: persist encode failed")
	}
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// readPersist restores persisted state into rf.
// Returns immediately for a fresh server (empty data).
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, voted, snapIdx, snapTerm int
	var entries []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voted) != nil ||
		d.Decode(&entries) != nil ||
		d.Decode(&snapIdx) != nil ||
		d.Decode(&snapTerm) != nil {
		panic("raft: readPersist decode failed")
	}
	rf.currentTerm = term
	rf.votedFor = voted
	rf.log.entries = entries
	rf.log.snapshotIndex = snapIdx
	rf.log.snapshotTerm = snapTerm
}

// ───────────────────────────────────────────────────────────────────────────────
// § 12  ELECTION UTILITIES
// ───────────────────────────────────────────────────────────────────────────────

func (rf *Raft) resetElectionDeadline() {
	spread := int64(electionMaxTimeout - electionMinTimeout)
	rf.electionDeadline = time.Now().Add(electionMinTimeout + time.Duration(rand.Int63n(spread)))
}

func (rf *Raft) pastElectionDeadline() bool {
	return time.Now().After(rf.electionDeadline)
}

// ───────────────────────────────────────────────────────────────────────────────
// § 13  TICKER
// ───────────────────────────────────────────────────────────────────────────────

// ticker is the single goroutine driving all timing.
// Leader: send heartbeats every heartbeatInterval.
// Others: start election if deadline passed.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		switch rf.role {
		case RoleLeader:
			rf.replicateAll()
			rf.mu.Unlock()
		default:
			if rf.pastElectionDeadline() {
				rf.becomeCandidateLocked() // releases mu
			} else {
				rf.mu.Unlock()
			}
		}
		time.Sleep(heartbeatInterval)
	}
}

// ───────────────────────────────────────────────────────────────────────────────
// § 14  REPLICATION
// ───────────────────────────────────────────────────────────────────────────────

// replicateAll fans out one replication goroutine per peer.
// Caller must hold rf.mu.
func (rf *Raft) replicateAll() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateTo(i)
		}
	}
}

// replicateTo sends either AppendEntries or InstallSnapshot to peer,
// depending on whether the peer needs entries that have been compacted.
//
// 3D branch: if nextIndex[peer] ≤ snapshotIndex, the entries the peer needs
// no longer exist in our log — send the full snapshot instead.
func (rf *Raft) replicateTo(peer int) {
	rf.mu.Lock()
	if rf.role != RoleLeader {
		rf.mu.Unlock()
		return
	}

	// ── 3D: snapshot path ────────────────────────────────────────────────────
	if rf.progress[peer].next <= rf.log.snapshotIndex {
		// The entries this peer needs have been compacted into a snapshot.
		// Send the full snapshot so it can catch up.
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.log.snapshotIndex,
			LastIncludedTerm:  rf.log.snapshotTerm,
			Data:              rf.snapshot,
		}
		rf.mu.Unlock()

		var reply InstallSnapshotReply
		if !rf.peers[peer].Call("Raft.InstallSnapshot", args, &reply) {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term, true)
			return
		}
		if rf.role != RoleLeader || rf.currentTerm != args.Term {
			return
		}
		rf.stepFunc(rf, &installSnapshotResult{
			peer:          peer,
			snapshotIndex: args.LastIncludedIndex,
		})
		return
	}

	// ── Normal AppendEntries path ─────────────────────────────────────────────
	prevIdx := rf.progress[peer].next - 1
	prevTerm := rf.log.term(prevIdx)
	entries := rf.log.slice(rf.progress[peer].next, rf.log.lastIndex()+1)
	matchIfSuccess := prevIdx + len(entries)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.peers[peer].Call("Raft.AppendEntries", args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term, true)
		return
	}
	if rf.role != RoleLeader || rf.currentTerm != args.Term {
		return
	}
	rf.stepFunc(rf, &appendEntriesResult{
		peer:           peer,
		reply:          reply,
		matchIfSuccess: matchIfSuccess,
	})
}

// appendEntriesResult carries reply context into stepLeader.
type appendEntriesResult struct {
	peer           int
	reply          AppendEntriesReply
	matchIfSuccess int
}

// installSnapshotResult carries reply context into stepLeader.
type installSnapshotResult struct {
	peer          int
	snapshotIndex int
}

// ───────────────────────────────────────────────────────────────────────────────
// § 15  COMMIT INDEX ADVANCEMENT
// ───────────────────────────────────────────────────────────────────────────────

// maybeAdvanceCommitIndex finds the highest index on a strict majority and,
// if it belongs to currentTerm, advances commitIndex.
// §5.4.2 — only commit entries from the current term directly.
// Caller must hold rf.mu.
func (rf *Raft) maybeAdvanceCommitIndex() {
	n := len(rf.peers)
	matches := make([]int, n)
	for i, p := range rf.progress {
		matches[i] = p.match
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matches)))
	quorum := matches[n/2]

	if quorum > rf.commitIndex && rf.log.term(quorum) == rf.currentTerm {
		rf.commitIndex = quorum
		rf.applyCond.Signal()
	}
}

// applyBackoff decreases nextIndex for a peer that rejected AppendEntries,
// using the XTerm/XIndex/XLen fast back-off scheme.
// Caller must hold rf.mu.
func (rf *Raft) applyBackoff(peer int, reply AppendEntriesReply) {
	var next int
	if reply.XTerm == -1 {
		// Case A: follower log too short.
		next = reply.XLen
	} else {
		// Search for last entry of XTerm in our log.
		found := -1
		for i := rf.log.lastIndex(); i > rf.log.snapshotIndex; i-- {
			if rf.log.term(i) == reply.XTerm {
				found = i
				break
			}
		}
		if found != -1 {
			next = found + 1 // Case B: we have XTerm
		} else {
			next = reply.XIndex // Case C: we lack XTerm
		}
	}
	rf.progress[peer].decreaseTo(next)
}

// ───────────────────────────────────────────────────────────────────────────────
// § 16  APPLIER
//
// Delivers committed entries (and snapshots) to the service via applyCh.
//
// Invariant I2: rf.mu is NEVER held during a channel send.
//
// 3D snapshot path (Invariant S4):
//   If snapshotIndex > lastApplied, a new snapshot arrived via InstallSnapshot.
//   Deliver it first (SnapshotValid=true), advance lastApplied to snapshotIndex,
//   then loop back to deliver any log entries above that boundary.
// ───────────────────────────────────────────────────────────────────────────────

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		// Sleep until there is something to deliver.
		for !rf.killed() && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// ── Priority 1: deliver snapshot if one arrived since last iteration ──
		//
		// InstallSnapshot handler advances snapshotIndex WITHOUT touching
		// lastApplied (Invariant S3).  We detect it here and deliver first.
		if rf.log.snapshotIndex > rf.lastApplied {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log.snapshotTerm,
				SnapshotIndex: rf.log.snapshotIndex,
			}
			snapIdx := rf.log.snapshotIndex
			rf.mu.Unlock()

			rf.applyCh <- msg // no lock held

			rf.mu.Lock()
			if snapIdx > rf.lastApplied {
				rf.lastApplied = snapIdx
			}
			rf.mu.Unlock()
			continue // re-check commitIndex vs lastApplied
		}

		// ── Priority 2: deliver committed log entries ─────────────────────────
		start := rf.lastApplied + 1
		end := rf.commitIndex
		msgs := make([]raftapi.ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log.entry(i).Command,
				CommandIndex: i,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs { // no lock held during send
			rf.applyCh <- msg
		}

		rf.mu.Lock()
		if end > rf.lastApplied {
			rf.lastApplied = end
		}
		rf.mu.Unlock()
	}
}

// ───────────────────────────────────────────────────────────────────────────────
// § 17  RequestVote RPC
// ───────────────────────────────────────────────────────────────────────────────

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, term, lastIdx, lastTerm int) bool {
	args := &RequestVoteArgs{term, rf.me, lastIdx, lastTerm}
	var reply RequestVoteReply
	if !rf.peers[server].Call("Raft.RequestVote", args, &reply) {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term, true)
		return true
	}
	if rf.role != RoleCandidate || rf.currentTerm != args.Term {
		return true
	}
	rf.stepFunc(rf, &reply)
	return true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term, true)
		reply.Term = rf.currentTerm
	}
	if args.Term < rf.currentTerm {
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	// §5.4.1 — up-to-date check.
	upToDate := args.LastLogTerm > rf.log.lastTerm() ||
		(args.LastLogTerm == rf.log.lastTerm() && args.LastLogIndex >= rf.log.lastIndex())
	if !upToDate {
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist() // votedFor changed; persist before replying
	rf.resetElectionDeadline()
	reply.VoteGranted = true
}

// ───────────────────────────────────────────────────────────────────────────────
// § 18  AppendEntries RPC
// ───────────────────────────────────────────────────────────────────────────────

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
	XTerm   int // conflict term; -1 if log too short
	XIndex  int // first index of XTerm in follower log
	XLen    int // follower log length when XTerm == -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term, true)
		reply.Term = rf.currentTerm
	}
	if args.Term < rf.currentTerm {
		return
	}
	if rf.role == RoleCandidate {
		rf.becomeFollowerLocked(args.Term, false)
	}
	rf.resetElectionDeadline()

	// ── 3D: handle PrevLogIndex inside our snapshot ───────────────────────────
	// If PrevLogIndex < snapshotIndex, the leader's previous index is already
	// covered by our snapshot.  Trim the overlap and accept the remaining entries.
	if args.PrevLogIndex < rf.log.snapshotIndex {
		overlap := rf.log.snapshotIndex - args.PrevLogIndex
		if overlap >= len(args.Entries) {
			// All entries already applied via snapshot.
			reply.Success = true
			return
		}
		// Advance past the overlap.
		args.Entries = args.Entries[overlap:]
		args.PrevLogIndex = rf.log.snapshotIndex
		args.PrevLogTerm = rf.log.snapshotTerm
	}

	// ── Consistency check ─────────────────────────────────────────────────────
	if args.PrevLogIndex > rf.log.lastIndex() {
		reply.XTerm = -1
		reply.XLen = rf.log.lastIndex() + 1
		return
	}
	if rf.log.term(args.PrevLogIndex) != args.PrevLogTerm {
		reply.XTerm = rf.log.term(args.PrevLogIndex)
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > rf.log.snapshotIndex+1 &&
			rf.log.term(reply.XIndex-1) == reply.XTerm {
			reply.XIndex--
		}
		return
	}

	// ── Reconcile entries ─────────────────────────────────────────────────────
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.log.lastIndex() {
			if rf.log.term(idx) != entry.Term {
				rf.log.truncateAndAppend(idx, args.Entries[i:])
				break
			}
		} else {
			rf.log.append(args.Entries[i:]...)
			break
		}
	}
	rf.persist()

	// ── Advance commitIndex ───────────────────────────────────────────────────
	if args.LeaderCommit > rf.commitIndex {
		nc := min(args.LeaderCommit, rf.log.lastIndex())
		if nc > rf.commitIndex {
			rf.commitIndex = nc
			rf.applyCond.Signal()
		}
	}
	reply.Success = true
}

// ───────────────────────────────────────────────────────────────────────────────
// § 19  InstallSnapshot RPC  (NEW in 3D)
//
// §7 Figure 13.  The leader sends its full snapshot to a lagging peer.
// We send the snapshot as a single chunk (no offset mechanism — per the spec).
// ───────────────────────────────────────────────────────────────────────────────

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int    // snapshot replaces all entries ≤ this index
	LastIncludedTerm  int    // term of the last included entry
	Data              []byte // complete snapshot (single chunk)
}

type InstallSnapshotReply struct {
	Term int // currentTerm; lets leader step down if stale
}

// InstallSnapshot is the RPC handler on the receiver side.
//
// Key design points:
//   - We call rf.log.trimPrefix() which handles both cases: snapshot covers
//     only part of our log (retain tail) or all of it (reset to fresh sentinel).
//   - We do NOT advance lastApplied here (Invariant S3).  The applier goroutine
//     detects snapshotIndex > lastApplied and delivers the SnapshotValid msg.
//   - commitIndex is advanced to snapshotIndex if currently behind, so the
//     applier's wakeup condition (lastApplied < commitIndex) is satisfied.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term, true)
		reply.Term = rf.currentTerm
	}
	if args.Term < rf.currentTerm {
		return
	}

	// Valid leader contact — reset election timer.
	if rf.role == RoleCandidate {
		rf.becomeFollowerLocked(args.Term, false)
	}
	rf.resetElectionDeadline()

	// Ignore stale or duplicate snapshots.
	if args.LastIncludedIndex <= rf.log.snapshotIndex {
		return
	}

	// Trim the log.  raftLog.trimPrefix handles both:
	//   • Snapshot covers part of our log  → retain entries above the boundary.
	//   • Snapshot covers all of our log   → reset to single sentinel entry.
	rf.log.trimPrefix(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.snapshot = args.Data

	// Advance commitIndex to be consistent with the snapshot boundary.
	// Do NOT touch lastApplied — applier goroutine owns that field.
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	// Persist the trimmed log and new snapshot atomically.
	rf.persist()

	// Wake the applier so it detects snapshotIndex > lastApplied and delivers
	// the SnapshotValid ApplyMsg.
	rf.applyCond.Signal()
}

// ───────────────────────────────────────────────────────────────────────────────
// § 20  UTILITY
// ───────────────────────────────────────────────────────────────────────────────

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
