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

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Conmmand interface{}
	Term     int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu         sync.Mutex          // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	aeInterval int
	applyChan  chan ApplyMsg
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	stopElect          bool
	stopLeader         bool
	currentTerm        int
	votedFor           int
	log                []Entry
	commitIndex        int
	lastApplied        int
	nextIndex          []int
	matchIndex         []int
	role               int
	tot                int
	aeCnt              int
	resetElectTimer    chan bool
	resetAETimer       chan bool
	stopProcessRoutine chan bool
	stopAESend         chan bool
	applySignal        chan bool
	stopRun            chan bool
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // 用来更新candidate的term
	VoteGranted bool // 其他follower是否投票
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int // currentTerm, for leader to update itself
	Success  bool
	ServerId int
	XIndex   int
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
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, Entry{
		Conmmand: command,
		Term:     term,
	})
	Printf(dClient, "s%v: command %v, lastidx: %v", rf.me, command, len(rf.log)-1)
	rf.mu.Unlock()
	rf.resetAETimer <- true
	return index, term, true
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
	Printf(dInfo, "s%v is killed", rf.me)
	rf.resetElectTimer <- true
	rf.stopProcessRoutine <- true
	rf.stopProcessRoutine <- true
	rf.stopAESend <- true
	rf.stopRun <- true
	rf.stopRun <- true
	rf.mu.Lock()
	rf.stopLeader = true
	rf.stopElect = true
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		if rf.role == LEADER {
			Printf(dRole, "s%v: leader -> follower", rf.me)
			rf.stopLeader = true
			rf.stopProcessRoutine <- true // 终止处理AE response的goroutine
			rf.stopAESend <- true
		} else if rf.role == FOLLOWER {

		} else if rf.role == CANDIDATE {
			rf.stopElect = true
			rf.stopProcessRoutine <- true
		}
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	term := -1
	if args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log)-1 {
		term = rf.log[args.PrevLogIndex].Term
	}
	if args.Term == rf.currentTerm && (args.PrevLogIndex <= len(rf.log)-1 && term == args.PrevLogTerm) {
		Printf(dAE, "s%v receive ae from s%v: T%v, T%v, li %v lt %v", rf.me, args.LeaderId, rf.currentTerm, args.Term, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.resetElectTimer <- true
		reply.Success = true
		reply.Term = rf.currentTerm
		reply.ServerId = rf.me
		if len(args.Entries) > 0 {
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
			Printf(dLog, "s%v add log len %v, li: %v, lt: %v", rf.me, len(args.Entries), len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		}

		if rf.role == LEADER {
			Printf(dAE, "s%v: leader -> follower", rf.me)
			rf.stopLeader = true
			rf.stopProcessRoutine <- true
			rf.resetAETimer <- true
		} else if rf.role == FOLLOWER {

		} else if rf.role == CANDIDATE {
			rf.stopElect = true
			rf.stopProcessRoutine <- true
		}
		rf.role = FOLLOWER
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(len(rf.log)-1, args.LeaderCommit)
			Printf(dCommit, "s%v commit: %v", rf.me, rf.commitIndex)
			rf.applySignal <- true
		}
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ServerId = rf.me
		if args.Term == rf.currentTerm {
			if args.PrevLogIndex <= len(rf.log)-1 {
				for i := args.PrevLogIndex; i > 0; i-- {
					if rf.log[i].Term == term {
						reply.XIndex = i
					} else {
						break
					}
				}
			} else {
				reply.XIndex = len(rf.log)
			}
		} else {
			reply.XIndex = -1
		}
		Printf(dAE, "s%v don't receive ae: li %v, lt %v, T %v; arg: pli %v, plt %v, T %v", rf.me, len(rf.log)-1, term, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Term)
		// 返回冲突信息，减少AE次数
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, apResult chan *AppendEntriesReply) {
	reply := AppendEntriesReply{}
	retryTimes := 3
	for {
		rf.mu.Lock()
		flag := rf.stopLeader
		rf.mu.Unlock()
		if flag {
			return
		}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		if ok {
			rf.mu.Lock()
			Printf(dAE, "s%v receive ae response from s%v: T%v, T%v", rf.me, server, rf.currentTerm, reply.Term)
			rf.mu.Unlock()
			apResult <- &reply
			return
		} else {
			retryTimes--
			if retryTimes < 0 {
				return
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}
func (rf *Raft) sendAE(apResult chan *AppendEntriesReply) {
	for {
		rf.mu.Lock()
		flag := (rf.role == LEADER)
		rf.aeCnt = 1
		rf.mu.Unlock()
		if !flag {
			return
		}

		select {
		case <-time.After(time.Duration(rf.aeInterval) * time.Millisecond):
			for i := 0; i < rf.tot; i++ {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				// 发送AP
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := -1
				if prevLogIndex > 0 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      rf.log[Max(rf.nextIndex[i], 1):],
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				Printf(dAE, "s%v send ae to s%d: T%v, pli: %v, plt: %v", rf.me, i, args.Term, prevLogIndex, prevLogTerm)
				go rf.sendAppendEntries(i, &args, apResult)
			}
		case <-rf.resetAETimer:
		case <-rf.stopAESend:
			return
		}
	}
}
func (rf *Raft) processAE(apResult chan *AppendEntriesReply) {
	for {
		select {
		case res := <-apResult:
			rf.mu.Lock()
			if res.Term > rf.currentTerm {
				// 转变为follower
				Printf(dRole, "s%v: leader -> follower", rf.me)
				rf.currentTerm = res.Term
				rf.role = FOLLOWER
				rf.stopAESend <- true
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			} else {
				if res.Success {
					rf.matchIndex[res.ServerId] = len(rf.log) - 1
					rf.nextIndex[res.ServerId] = len(rf.log)
					rf.aeCnt++
					Printf(dAE, "s%v update s%v's mi: %v, ni: %v", rf.me, res.ServerId, len(rf.log)-1, len(rf.log))
					if rf.aeCnt > rf.tot/2 {
						// receive response
						Printf(dAE, "s%v receive ae response half: %d", rf.me, rf.aeCnt)
						tmp := make([]int, len(rf.matchIndex))
						copy(tmp, rf.matchIndex)
						sort.Ints(tmp)
						n := tmp[rf.tot/2]
						if n > rf.commitIndex && rf.log[n].Term == rf.currentTerm {
							rf.commitIndex = n
							Printf(dCommit, "s%v commit: %v", rf.me, n)
							rf.applySignal <- true
						}
					}
				} else {
					if res.XIndex != -1 {
						rf.nextIndex[res.ServerId] = res.XIndex
						Printf(dAE, "s%v decrease ni %v for %v", rf.me, rf.nextIndex[res.ServerId], res.ServerId)
					}
				}
			}
			rf.mu.Unlock()
		case <-rf.stopProcessRoutine:
			return
		}
	}
}

func (rf *Raft) leader() {
	// run leader
	rf.mu.Lock()
	rf.role = LEADER
	rf.stopLeader = false
	for i := 0; i < rf.tot; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = 0x3f3f3f3f
	rf.mu.Unlock()
	Printf(dRole, "s%v start leader: T%v", rf.me, rf.currentTerm)
	apResult := make(chan *AppendEntriesReply, rf.tot)
	// process AE
	go rf.processAE(apResult)
	// send AE
	go rf.sendAE(apResult)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	Printf(dVote, "s%v receive vote request from s%v: T%v, T%v", rf.me, args.CandidateId, rf.currentTerm, args.Term)

	if args.Term > rf.currentTerm {
		if rf.role == LEADER {
			Printf(dRole, "s%v: leader -> follower", rf.me)
			rf.stopLeader = true
			rf.stopProcessRoutine <- true // 终止处理AE response的goroutine
			rf.stopAESend <- true
		} else if rf.role == FOLLOWER {

		} else if rf.role == CANDIDATE {
			Printf(dRole, "s%v: candidate -> follower", rf.me)
			rf.stopElect = true
			rf.stopProcessRoutine <- true
		}
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	lastLogIdx := len(rf.log) - 1
	lastLogTerm := -1
	if lastLogIdx > 0 {
		lastLogTerm = rf.log[lastLogIdx].Term
	}
	// 可以投票
	if args.Term == rf.currentTerm && rf.votedFor == -1 &&
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx ||
			args.LastLogTerm > lastLogTerm) {
		// 投票
		Printf(dVote, "s%v vote for s%v", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.resetElectTimer <- true
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, voteResult chan *RequestVoteReply) {
	reply := RequestVoteReply{}
	retryTimes := 3
	for {
		rf.mu.Lock()
		flag := rf.stopElect
		rf.mu.Unlock()
		if flag {
			return
		}

		Printf(dVote, "s%v send vote request to s%v: T%v", rf.me, server, args.Term)

		ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
		if ok {
			voteResult <- &reply
			Printf(dVote, "s%v receive vote response from s%v", rf.me, server)
			return
		} else {
			retryTimes--
			if retryTimes < 0 {
				return
			}
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.role = CANDIDATE
	rf.votedFor = rf.me
	rf.stopElect = false
	term := rf.currentTerm
	lastlogIdx := len(rf.log) - 1
	lastLogTerm := -1
	if lastlogIdx > 0 {
		lastLogTerm = rf.log[lastlogIdx].Term
	}
	rf.mu.Unlock()
	Printf(dVote, "s%v start elect: T%v", rf.me, rf.currentTerm)
	voteResult := make(chan *RequestVoteReply, rf.tot)
	// 同时向其他server发送request vote
	for i := 0; i < rf.tot; i++ {
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastlogIdx,
			LastLogTerm:  lastLogTerm,
		}
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, voteResult)
	}
	voteNum := 1
	for {
		select {
		case res := <-voteResult:
			if res.VoteGranted {
				// server vote
				voteNum++
				Printf(dVote, "s%v vote num: %v", rf.me, voteNum)
				if voteNum > rf.tot/2 {
					// 收到一半以上投票
					// 转为Leader
					Printf(dVote, "s%v vote half num: %v", rf.me, voteNum)
					go rf.leader()
					return
				}
			} else {
				if res.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.role = FOLLOWER
					rf.currentTerm = res.Term
					rf.votedFor = -1
					rf.stopElect = true
					rf.mu.Unlock()
				}
			}
		case <-rf.stopProcessRoutine:
			return
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 400 + (rand.Int63() % 200)
		Printf(dTimer, "s%v reset timer: %v", rf.me, ms)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			rf.mu.Lock()
			if rf.role != LEADER {
				if rf.role == CANDIDATE {
					// 结束选举进程
					rf.stopElect = true
				}
				// 开启新的一轮选举
				go rf.elect()
			}
			rf.mu.Unlock()
		case <-rf.resetElectTimer:

		}
	}
	rf.mu.Lock()
	rf.stopElect = true
	rf.stopLeader = true
	rf.mu.Unlock()
}

func (rf *Raft) applyEntries() {
	for {
		select {
		case <-rf.applySignal:
			// apply committed log entries
			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Conmmand,
					CommandIndex: i,
				}
				rf.lastApplied++
				Printf(dApply, "s%v apply %v: %v", rf.me, i, msg.Command)
				rf.mu.Unlock()
				rf.applyChan <- msg
				rf.mu.Lock()
			}
			rf.mu.Unlock()
		case <-rf.stopRun:
			return
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.applyChan = applyCh
	rf.tot = len(peers)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, rf.tot)
	rf.matchIndex = make([]int, rf.tot)
	rf.stopElect = true
	rf.role = FOLLOWER
	rf.aeCnt = 0
	rf.aeInterval = 100
	rf.resetElectTimer = make(chan bool, 1)
	rf.resetAETimer = make(chan bool, 1)
	rf.stopProcessRoutine = make(chan bool, 2)
	rf.applySignal = make(chan bool, 30)
	rf.stopRun = make(chan bool, 10)
	rf.stopAESend = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyEntries()

	return rf
}
