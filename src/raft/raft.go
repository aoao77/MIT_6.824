package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

type entry struct {
	Term    int
	Command interface{}
}

const (
	state_undefined = iota
	state_follower
	state_candidate
	state_leader
	state_dead
)

const (
	null = -1
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	voteFor     int
	log         []entry //first index is 1
	// volatile on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	// reinitialized after election
	nextIndex  []int //last log index + 1
	matchIndex []int //0
	//define by self
	state         int
	votedNum      int
	electionTimer int64
	applyChan     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// DebugPrint(dInfo, "S%d  get lock at  \n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == state_leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	DebugPrint(dLog, "S%d persist voteFor:%d logLen:%d T:%d\n", rf.me, rf.voteFor, len(rf.log), rf.currentTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	DebugPrint(dLog, "S%d begin readPersist voteFor:%d logLen:%d T:%d\n", rf.me, rf.voteFor, len(rf.log), rf.currentTerm)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []entry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		DebugPrint(dError, "S%d  Decode Error  \n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
	}
	DebugPrint(dLog, "S%d readPersist voteFor:%d logLen:%d T:%d\n", rf.me, rf.voteFor, len(rf.log), rf.currentTerm)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) lastLogIndex() int {
	index := len(rf.log) - 1
	return index
}

func (rf *Raft) follwer_initial(inTerm int) {
	rf.state = state_follower
	rf.currentTerm = inTerm
	rf.voteFor = null
	rf.electionTime(200, 200)
	rf.votedNum = 0
}

func (rf *Raft) candidate_initial() {
	rf.state = state_candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.electionTime(200, 200)
	rf.votedNum = 0
}

func (rf *Raft) leader_initial() {
	rf.state = state_leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = 0
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		DebugPrint(dDrop, "S%d <- S%d term is lower ,  rejecting (%d < %d) \n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		//turn to follwer
		DebugPrint(dTerm, "S%d <- S%d term is higher , ,updating (%d > %d) \n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.follwer_initial(args.Term)
	}
	//updated
	if rf.voteFor == null || rf.voteFor == args.CandidateId {
		DebugPrint(dVote, "S%d <- S%d argsTerm:%d excuTerm:%d argsIdx:%d excuIdx:%d, T%d\n", rf.me, args.CandidateId, args.LastLogTerm, rf.log[rf.lastLogIndex()].Term, args.LastLogIndex, rf.lastLogIndex(), rf.currentTerm)
		if (args.LastLogTerm > rf.log[rf.lastLogIndex()].Term) || (args.LastLogTerm == rf.log[rf.lastLogIndex()].Term && args.LastLogIndex >= rf.lastLogIndex()) {
			rf.voteFor = args.CandidateId
			rf.electionTime(200, 200)
			//reply
			reply.VoteGranted = true
			//debug
			DebugPrint(dVote, "S%d granting vote to S%d T%d\n", rf.me, args.CandidateId, rf.currentTerm)
		}
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type RequestAppendArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry //empty when heart beats
	LeaderCommit int
}

// capital letters!
type RequestAppendReply struct {
	// Your data here (2A).
	Term          int
	Success       bool
	Inconsistency bool
	ConflictTerm  int
	LogLen        int
	XIndex        int
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) RequestAppend(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//initial
	reply.Success = false
	reply.Inconsistency = false
	reply.LogLen = 0
	reply.ConflictTerm = 0
	reply.XIndex = 0
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DebugPrint(dDrop, "S%d <- L%d term is lower ,  rejecting (%d < %d) \n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		DebugPrint(dTerm, "S%d <- L%d term is higher ,  updating (%d > %d) \n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.follwer_initial(args.Term)
		rf.voteFor = args.LeaderId
		rf.persist()
	} else if rf.state == state_candidate {
		DebugPrint(dError, "C%d <- L%d Stepping down T:%d\n", rf.me, args.LeaderId, rf.currentTerm)
		rf.follwer_initial(args.Term)
		rf.voteFor = args.LeaderId
		rf.persist()
	} else if args.LeaderId != rf.voteFor {
		DebugPrint(dDrop, "S%d <- L%d leader is time out ,  vote for :S%d T:%d \n", rf.me, args.LeaderId, rf.voteFor, rf.currentTerm)
		return
	}

	if rf.state == state_follower {
		//RESET TIMER
		rf.electionTime(200, 200)
		//REPLY
		reply.Term = rf.currentTerm

		if args.PrevLogIndex >= len(rf.log) {
			reply.LogLen = len(rf.log)
			reply.Inconsistency = true
			DebugPrint(dDrop, "S%d <- L%d dismatched InLogIdx:%d LogLen:%d \n", rf.me, args.LeaderId, args.PrevLogIndex, reply.LogLen)
			return
		} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			for k, v := range rf.log {
				if v.Term == rf.log[args.PrevLogIndex].Term {
					reply.XIndex = k
					break
				}
			}
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			reply.Inconsistency = true
			DebugPrint(dDrop, "S%d <- L%d dismatched InTerm:%d CurTerm:%d XIndex:%d \n", rf.me, args.LeaderId, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term, reply.XIndex)
			return
		}
		//matched
		//heart beat
		reply.Success = true
		if len(args.Entries) == 0 {
			DebugPrint(dTimer, "S%d <- L%d received heartbeats T:%d\n", rf.me, args.LeaderId, rf.currentTerm)
		} else {
			i := args.PrevLogIndex + 1
			entry_idx := 0
			for i < len(rf.log) && entry_idx < len(args.Entries) {
				//delete
				if rf.log[i].Term != args.Entries[entry_idx].Term {
					DebugPrint(dRecv, "S%d <- L%d Term Confict BeforeLogLen:%d AfterLogLen:%d T:%d\n", rf.me, args.LeaderId, len(args.Entries), i, rf.currentTerm)
					rf.log = rf.log[:i]
					break
				}
				//update
				i++
				entry_idx++
			}
			//entry_idx > len(rf.log)
			if entry_idx < len(args.Entries) {
				appendArray := make([]entry, len(args.Entries[entry_idx:]))
				copy(appendArray, args.Entries[entry_idx:])
				rf.log = append(rf.log, appendArray...)
			}
			// for key, val := range rf.log {
			// 	DebugPrint(dLog, "S%d LogIdx:%d  LogVal:%v T:%d\n", rf.me, key, val, rf.currentTerm)
			// }
			DebugPrint(dRecv, "S%d <- S%d receiving LogLen:%d LogIdx:%d T:%d\n", rf.me, args.LeaderId, len(args.Entries), rf.lastLogIndex(), rf.currentTerm)
			rf.persist()
		}
		//update commitIndexing CommitIdx:1 AppliedIdx:
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				//Q:apply status machine
				applyMsg := ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.log[rf.lastApplied].Command
				applyMsg.CommandIndex = rf.lastApplied
				//send
				rf.applyChan <- applyMsg
			}
			DebugPrint(dCommit, "S%d Follower CI:%d AI:%d T:%d\n", rf.me, rf.commitIndex, rf.lastApplied, rf.currentTerm)
		}
	} else {
		DebugPrint(dDrop, "S%d <- S%d dropping is not follower T:%d \n", rf.me, args.LeaderId, rf.currentTerm)
	}
}

// heart beats and RequestAppend
func (rf *Raft) sendRequestAppend(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppend", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == state_leader
	if isLeader {
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, entry{term, command})
		DebugPrint(dClient, "S%d client add command:%v idx:%d T:%d\n", rf.me, command, rf.lastLogIndex(), rf.currentTerm)
		rf.persist()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTime(start int64, length int64) {
	rf.electionTimer = start + (rand.Int63() % length)
}

func (rf *Raft) isMajority(num int) bool {
	return num >= (len(rf.peers) / 2)
}

// ProcessRequestVote
func (rf *Raft) ProcessRequestVote(i int, args *RequestVoteArgs) {
	//need goroutine
	reply := RequestVoteReply{}

	rf.sendRequestVote(i, args, &reply)
	//receive
	rf.mu.Lock()
	if rf.state == state_candidate {
		if reply.Term > rf.currentTerm {
			DebugPrint(dTrans, "C%d Stepping down ,  updating (%d > %d) \n", rf.me, reply.Term, rf.currentTerm)
			rf.follwer_initial(reply.Term)
			rf.persist()
		} else if reply.VoteGranted && rf.currentTerm == args.Term {
			rf.votedNum++
			DebugPrint(dVote, "S%d <- S%d Got Vote \n", rf.me, i)
			if rf.isMajority(rf.votedNum) && rf.state != state_leader {
				DebugPrint(dLeader, "S%d Achieved Majority for T%d \n", rf.me, rf.currentTerm)
				rf.leader_initial()
			}
		}
	}
	rf.mu.Unlock()
}

// ProcessRequestAppend
func (rf *Raft) ProcessRequestAppend(i int) {
	//initial
	args := RequestAppendArgs{0, 0, 0, 0, nil, 0}
	reply := RequestAppendReply{0, false, false, 0, 0, 0}
	prevLogIndex := 0
	appendEntries := []entry{}
	//lock
	rf.mu.Lock()
	if rf.state == state_leader {
		if rf.nextIndex[i] > 0 {
			prevLogIndex = rf.nextIndex[i] - 1
		}
		appendEntries = make([]entry, len(rf.log[rf.nextIndex[i]:len(rf.log)]))
		copy(appendEntries, rf.log[rf.nextIndex[i]:len(rf.log)])
		args = RequestAppendArgs{rf.currentTerm, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, appendEntries, rf.commitIndex}
		reply = RequestAppendReply{0, false, false, 0, 0, 0}
		if len(appendEntries) == 0 {
			DebugPrint(dTimer, "S%d -> S%d sending heartbeats T:%d\n", rf.me, i, rf.currentTerm)
		} else {
			DebugPrint(dAppend, "S%d -> S%d appending logs N:%d len:%d T:%d\n", rf.me, i, rf.nextIndex[i], len(appendEntries), rf.currentTerm)
		}
	}
	rf.mu.Unlock()
	rf.sendRequestAppend(i, &args, &reply)
	// receive
	rf.mu.Lock()
	if rf.state == state_leader {
		if reply.Term > rf.currentTerm {
			rf.follwer_initial(reply.Term)
			rf.persist()
			DebugPrint(dTrans, "S%d Step down from Leader ,  updating (%d > %d) \n", rf.me, reply.Term, rf.currentTerm)
		} else if reply.Success {
			//update nextIndex matchIndex
			rf.matchIndex[i] = prevLogIndex + len(appendEntries)
			rf.nextIndex[i] = rf.lastLogIndex() + 1
			if len(appendEntries) == 0 {
				DebugPrint(dTimer, "S%d <- S%d received heartbeats M:%d N:%d T:%d\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], rf.currentTerm)
			} else {
				DebugPrint(dAppend, "S%d <- S%d Appended Log M:%d N:%d T:%d\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], rf.currentTerm)
			}
		} else { //failed
			if reply.Inconsistency {
				if reply.LogLen != 0 && reply.LogLen < rf.nextIndex[i] {
					rf.nextIndex[i] = reply.LogLen
					DebugPrint(dAppend, "S%d <- S%d LogLen Failed M:%d N:%d T:%d\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], rf.currentTerm)
				} else {
					hasTerm := false
					tempIndex := 0
					for k, v := range rf.log {
						if v.Term == reply.ConflictTerm {
							tempIndex = k
							hasTerm = true
						}
					}
					if hasTerm && tempIndex < rf.nextIndex[i] {
						rf.nextIndex[i] = tempIndex
						DebugPrint(dError, "S%d <- S%d HasTerm Failed M:%d N:%d T:%d\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], rf.currentTerm)
					}
					if !hasTerm && reply.XIndex < rf.nextIndex[i] {
						rf.nextIndex[i] = reply.XIndex
						DebugPrint(dError, "S%d <- S%d DontHaveTerm Failed M:%d N:%d T:%d\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], rf.currentTerm)
					}
				}
			} else {
				DebugPrint(dError, "S%d <- S%d Append Failed M:%d N:%d T:%d RT:%d\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], rf.currentTerm, reply.Term)
			}
		}
	}
	rf.mu.Unlock()
}

// ticker
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == state_follower || rf.state == state_candidate {
			if rf.electionTimer <= 0 {
				rf.candidate_initial()
				rf.persist()
				DebugPrint(dLeader, "S%d Converting to Candidate, call election T:%d\n", rf.me, rf.currentTerm)
				//	peers     []*labrpc.ClientEnd // RPC end points of all peers
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex(), rf.log[rf.lastLogIndex()].Term}
					DebugPrint(dVote, "S%d asking for vote T:%d\n", rf.me, rf.currentTerm)
					go rf.ProcessRequestVote(i, &args)
				}
			}
		} else if rf.state == state_leader { //Q: Leader term change
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.ProcessRequestAppend(i)
			}
			//update commitIndex
			temp_commit_idx := rf.commitIndex
			for temp_commit_idx < len(rf.log)-1 {
				temp_append_num := 0
				for i := 0; i < len(rf.matchIndex); i++ {
					if rf.matchIndex[i] > temp_commit_idx {
						temp_append_num++
					}
				}
				if rf.isMajority(temp_append_num) {
					temp_commit_idx++
					if rf.log[temp_commit_idx].Term == rf.currentTerm {
						rf.commitIndex = temp_commit_idx
						// for key, val := range rf.log {
						// 	DebugPrint(dLog, "S%d LogIdx:%d  LogVal:%v T:%d\n", rf.me, key, val, rf.currentTerm)
						// }
					}
				} else {
					break
				}
			}
			//update lastApplied
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				//Q:apply status machine
				applyMsg := ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.log[rf.lastApplied].Command
				applyMsg.CommandIndex = rf.lastApplied
				//send
				rf.applyChan <- applyMsg
				DebugPrint(dCommit, "S%d Leader CI:%d AI:%d at T:%d \n", rf.me, rf.commitIndex, rf.lastApplied, rf.currentTerm)
			}
		}

		// pause for a random amount of time between 100 and 200
		// milliseconds.
		ms := 50 + (rand.Int63() % 100)
		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		rf.electionTimer -= ms
		rf.mu.Unlock()
		// DebugPrint(dInfo, "S%d time pass %d  T:%d\n", rf.me, ms, rf.currentTerm)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh
	// Your initialization code here (2A, 2B, 2C).
	DebugPrint(dLog, "S%d start \n", rf.me)
	// persistent state on all servers
	rf.log = []entry{}               //first index is 1
	rf.log = append(rf.log, entry{}) //intial index 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	//define by self
	rf.follwer_initial(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
