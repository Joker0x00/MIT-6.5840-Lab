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
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER
	NONE
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
	currentTerm        int     // 持久化存储
	votedFor           int     // 持久化存储
	log                []Entry // 持久化存储
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
	routineCnt         int
	G                  map[string]int
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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
	XIdx     int
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
	rf.persist()
	Printf(dClient, "s%v: command %v, term: %v, lastidx: %v", rf.me, command, rf.currentTerm, len(rf.log)-1)
	rf.mu.Unlock()
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
	rf.mu.Lock()
	rf.stopLeader = true
	rf.stopElect = true
	rf.role = NONE
	rf.mu.Unlock()
	rf.stopProcessRoutine <- true // 终止Vote or AE process
	// 终止AE
	rf.stopRun <- true // 终止Apply和ticker
	rf.stopRun <- true

	close(rf.applyChan)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
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
		} else {
			return
		}
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	term := -1
	if args.PrevLogIndex > 0 && args.PrevLogIndex <= len(rf.log)-1 {
		term = rf.log[args.PrevLogIndex].Term
	}
	reply.XIdx = -1
	if args.Term == rf.currentTerm && (args.PrevLogIndex <= len(rf.log)-1 && term == args.PrevLogTerm) {
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.resetElectTimer <- true
		reply.Success = true
		reply.Term = rf.currentTerm
		reply.ServerId = rf.me
		reply.XIdx = args.PrevLogIndex
		if len(args.Entries) > 0 {
			Printf(dAE, "s%v receive ae from s%v: T%v, T%v; pi %v pt %v", rf.me, args.LeaderId, rf.currentTerm, args.Term, args.PrevLogIndex, args.PrevLogTerm)
			i, j := args.PrevLogIndex+1, 0
			for i < len(rf.log) && j < len(args.Entries) {
				if rf.log[i] != args.Entries[j] {
					break
				}
				i++
				j++
			}
			Printf(dLog, "s%v, i: %v, j: %v, pi: %v, log len: %v, entry len: %v", rf.me, i, j, args.PrevLogIndex, len(rf.log), len(args.Entries))
			if j == len(args.Entries) {

			} else if i == len(rf.log) {
				rf.log = append(rf.log, args.Entries[j:]...)
			} else if rf.log[i] != args.Entries[j] {
				// 有冲突，但并未走到头
				rf.log = rf.log[:i]
				rf.log = append(rf.log, args.Entries[j:]...) // 不能直接截断
			}
			reply.XIdx = len(rf.log) - 1

			Printf(dLog, "s%v add log, arg entries: %v, after: %v, li: %v, lt: %v, XIdx: %v", rf.me, args.Entries[j:], rf.log, len(rf.log)-1, rf.log[len(rf.log)-1].Term, reply.XIdx)
		}
		rf.persist()

		if rf.role == LEADER {
			Printf(dAE, "s%v: leader -> follower", rf.me)
			rf.stopLeader = true
			rf.stopProcessRoutine <- true
			rf.stopAESend <- true
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
			// 返回冲突信息，减少AE次数
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
			rf.resetElectTimer <- true // 尽管没有AE成功，但是也要重置计时器，因为大多数服务器认为它就是leader，否则当需要回退log较多时超过选举计时器触发选举
			Printf(dAE, "s%v <- s%v: log confilict: i %v, t_me %v, t %v", rf.me, args.LeaderId, args.PrevLogIndex, term, args.PrevLogTerm)
		} else {
			reply.XIndex = -1
			Printf(dAE, "s%v <- s%v: drop AE", rf.me, args.LeaderId)
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, apResult chan *AppendEntriesReply) {
	rf.addG("send1AE")
	defer rf.deleteG("send1AE")
	reply := AppendEntriesReply{}
	// retryTimes := 3
	rf.mu.Lock()
	flag := rf.role != LEADER
	rf.mu.Unlock()
	if flag {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if ok {
		rf.mu.Lock()
		flag := args.Term != rf.currentTerm || rf.role != LEADER
		rf.mu.Unlock()
		if flag {
			return
		}
		apResult <- &reply
	}
}
func (rf *Raft) sendAE(apResult chan *AppendEntriesReply) {
	rf.addG("sendAllAE")
	defer rf.deleteG("sendAllAE")
	for !rf.killed() {
		rf.mu.Lock()
		flag := (rf.role != LEADER)
		rf.aeCnt = 1
		rf.mu.Unlock()
		if flag {
			return
		}
		select {
		case <-time.After(time.Duration(rf.aeInterval) * time.Millisecond):
			for i := 0; i < rf.tot; i++ {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				if rf.role != LEADER || rf.killed() {
					rf.mu.Unlock()
					return
				}
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
		case <-rf.stopAESend:
			return
		}
	}
}
func (rf *Raft) processAE(apResult chan *AppendEntriesReply) {
	rf.addG("processAE")
	defer rf.deleteG("processAE")
	for !rf.killed() {
		select {
		case res := <-apResult:
			rf.mu.Lock()
			if res.Term > rf.currentTerm {
				// 转变为follower
				Printf(dRole, "s%v: leader -> follower, processAE", rf.me)
				rf.currentTerm = res.Term
				rf.role = FOLLOWER
				rf.stopAESend <- true
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			} else {
				if res.Success {
					// 大问题
					// rf.matchIndex[res.ServerId] += res.XLen // 这个就有问题了，leader会提前提交没有agree的entry
					// rf.nextIndex[res.ServerId] += res.XLen  // 虽然next可以调整，但是会浪费时间
					rf.matchIndex[res.ServerId] = res.XIdx
					rf.nextIndex[res.ServerId] = res.XIdx + 1
					rf.aeCnt++
					Printf(dAE, "s%v update s%v's mi: %v, ni: %v", rf.me, res.ServerId, rf.matchIndex[res.ServerId], rf.nextIndex[res.ServerId])
					if rf.aeCnt > rf.tot/2 {
						// receive response
						Printf(dAE, "s%v receive ae response half: %d", rf.me, rf.aeCnt)
						tmp := make([]int, len(rf.matchIndex))
						copy(tmp, rf.matchIndex)
						sort.Ints(tmp)
						n := tmp[rf.tot/2]
						Printf(dCommit, "s%v pre commit idx: %v, pre: %v", rf.me, n, rf.commitIndex)
						if n > rf.commitIndex && rf.log[n].Term == rf.currentTerm {
							rf.commitIndex = n
							Printf(dCommit, "s%v commit: %v", rf.me, n)
							rf.applySignal <- true
						}
					}
				} else {
					if res.XIndex != -1 {
						rf.nextIndex[res.ServerId] = Min(rf.nextIndex[res.ServerId], res.XIndex) // 超时rpc覆盖next数组
						Printf(dAE, "s%v <- s%v: update ni: %v", rf.me, res.ServerId, rf.nextIndex[res.ServerId])
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
	rf.addG("leader")
	defer rf.deleteG("leader")
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
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	Printf(dVote, "s%v receive vote request from s%v: T%v, T%v, li %v, lt %v", rf.me, args.CandidateId, rf.currentTerm, args.Term, args.LastLogIndex, args.LastLogTerm)

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
		rf.persist()
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
		rf.persist()
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
	rf.addG("send1Vote")
	defer rf.deleteG("send1Vote")
	reply := RequestVoteReply{}
	// retryTimes := 3

	rf.mu.Lock()
	flag := rf.role != CANDIDATE
	rf.mu.Unlock()
	if flag {
		return
	}

	Printf(dVote, "s%v send vote request to s%v: T%v", rf.me, server, args.Term)

	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		rf.mu.Lock()
		flag := (args.Term != rf.currentTerm || rf.role != CANDIDATE)
		rf.mu.Unlock()
		if flag {
			return
		}
		voteResult <- &reply
		// Printf(dVote, "s%v receive vote response from s%v", rf.me, server)
	}
}

func (rf *Raft) elect() {
	rf.addG("elect")
	defer rf.deleteG("elect")
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.role = CANDIDATE
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
		// 选举期间运行
		go rf.sendRequestVote(i, &args, voteResult)
	}
	voteNum := 1
	// 选举期间运行
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
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
					// 不是leader了要立即退出
					go rf.leader()
					return
				}
			} else {
				rf.mu.Lock()
				if res.Term > rf.currentTerm {
					rf.role = FOLLOWER
					rf.currentTerm = res.Term
					rf.votedFor = -1
					rf.persist()
					rf.stopElect = true
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}

		case <-rf.stopProcessRoutine:
			return
		}
	}
}

// 被kill前一直保持运行状态
func (rf *Raft) ticker() {
	rf.addG("ticker")
	defer rf.deleteG("ticker")

	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 400 + (rand.Int63() % 350)
		Printf(dTimer, "s%v reset timer: %v, routine: %v", rf.me, ms, runtime.NumGoroutine())
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			rf.mu.Lock()
			if rf.role != LEADER {
				if rf.role == CANDIDATE {
					// 结束选举进程
					Printf(dVote, "s%v last elect over", rf.me)
					// 结束选举相关内容
					rf.stopElect = true
					rf.role = FOLLOWER
					rf.stopProcessRoutine <- true // 有点用处，基本能过1000，但还是偶尔有问题
				}
				// 开启新的一轮选举 只有在选举期间运行，不在选举立即退出
				go rf.elect()
			}
			rf.mu.Unlock()
		case <-rf.resetElectTimer:
		case <-rf.stopRun:
			return
		}
	}
}

// server被kill后关闭，否则一直运行
func (rf *Raft) applyEntries() {
	rf.addG("apply")
	defer rf.deleteG("apply")

	for !rf.killed() {
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
		case <-rf.stopRun: // 一个终止运行即可
			return
		}
	}
}

func (rf *Raft) addG(g string) {
	// rf.mu.Lock()
	// rf.routineCnt++
	// rf.G[g]++
	// Printf(dRoutine, "s%v addG: %v; %v", rf.me, g, rf.G)
	// rf.mu.Unlock()
}
func (rf *Raft) deleteG(g string) {
	// rf.mu.Lock()
	// rf.routineCnt--
	// rf.G[g]--
	// Printf(dRoutine, "s%v deleteG: %v; %v", rf.me, g, rf.G)
	// rf.mu.Unlock()
}

// func (rf *Raft) getG() int {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	return rf.routineCnt
// }

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
	rf.aeInterval = 150
	rf.resetElectTimer = make(chan bool, 1)
	rf.resetAETimer = make(chan bool, 1)
	rf.stopProcessRoutine = make(chan bool, 2)
	rf.applySignal = make(chan bool, 30)
	rf.stopRun = make(chan bool, 10)
	rf.stopAESend = make(chan bool, 1)
	rf.routineCnt = 0
	rf.G = make(map[string]int)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	Printf(dPersist, "s%v start", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyEntries()

	return rf
}
