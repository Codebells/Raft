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
	"bytes"
	"sync"
	"sync/atomic"
	"log"
	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
type NodeState string 
const(
	Leader    		NodeState = "Leader"
	Candidate       NodeState = "Candidate"
	Follower       	NodeState = "Follower"
)
//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        			sync.Mutex          // Lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd // RPC end points of all peers
	persister 			*Persister          // Object to hold this peer's persisted state
	me        			int                 // this peer's index into peers[]
	dead      			int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []LogEntry


	commitIndex int
	lastApplied int
	nextIndex  []int
	matchIndex []int
	state       NodeState
	voteCount   int

	applyCh     chan ApplyMsg
	winElectCh  chan bool
	becomeFollowerCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool
}
type LogEntry struct{
	Term    int
	Command interface{}
	
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader=(rf.state==Leader)
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// log.Println("Encode Term: ",rf.currentTerm,"raft logs :",rf.logs)
	w :=new(bytes.Buffer)
	e :=labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm)!=nil|| e.Encode(rf.votedFor)!=nil ||e.Encode(rf.logs)!=nil{
		log.Println("EncodeError")
		return 
	}
	data :=w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor	int 
	var logs		[]LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Println("DecodeError")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs=logs
	  log.Println("Decode Term: ",rf.currentTerm,"raft logs :",rf.logs)
	}
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

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int

	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted		bool
}

type AppendEntriesArgs struct{
	Term 			int
	LeaderId		int

	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct{
	Term 			int
	Success			bool
	FailIndex 	int
	FailTerm  	int
}


func (rf *Raft)getLastIndex() int{
	return rf.getLogLenth()-1
}
func (rf *Raft)getLogLenth() int {
	return len(rf.logs)
}
func (rf *Raft)getLastTerm() int{
	return rf.logs[rf.getLastIndex()].Term
}
func (rf *Raft) noBlockChan(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}

func (rf *Raft) isLogTheLatest(cLastIndex int, cLastTerm int) bool {
	myLastIndex, myLastTerm := rf.getLastIndex(), rf.getLastTerm()

	if cLastTerm == myLastTerm {
		return cLastIndex >= myLastIndex
	}

	return cLastTerm > myLastTerm
}
func (rf *Raft) applyLogs() {
	// log.Println("commitID: ",rf.commitIndex,"server ID : ",rf.me)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command: rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	
	if args.Term < rf.currentTerm {  
		reply.Term = rf.currentTerm
		reply.FailIndex = -1
		reply.FailTerm = -1
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	lastIndex := rf.getLastIndex()
	rf.noBlockChan(rf.heartbeatCh, true)
	// rf.heartbeatCh<-true
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.FailIndex = -1
	reply.FailTerm = -1

	//log check
	if args.PrevLogIndex > lastIndex {// follower log is shorter than leader
		reply.FailIndex = lastIndex + 1 // quick find the postion 
		return
	}
	// log.Println("args : ",args,"rf term: ",rf.currentTerm,"rf logs: ",rf.logs)
	if  rf.logs[args.PrevLogIndex].Term  != args.PrevLogTerm {	//has different term at prevLogIndex with leader
		// start check whether the log has entry with the same term with rf.logs[args.PrevLogIndex].Term 
		// if there has, return the index,quick find the sync postion ,if not set FailIndex = 0
		reply.FailTerm = rf.logs[args.PrevLogIndex].Term 				  
		for i := args.PrevLogIndex; i >= 0 && rf.logs[i].Term == rf.logs[args.PrevLogIndex].Term ; i-- { 
			reply.FailIndex = i
		}
		return
	}
	//if pass the log check,server has the same log with the Leader before args.PrevLogIndex + 1
	startClean := args.PrevLogIndex + 1
	startAppend :=0
	for ;startClean<len(rf.logs)&&startAppend<len(args.Entries);startClean,startAppend=startClean+1,startAppend+1{
		if rf.logs[startClean].Term!=args.Entries[startAppend].Term{
			break
		}
	}
	rf.logs = rf.logs[:startClean] // save the log without conflict
	args.Entries=args.Entries[startAppend:]
	rf.logs = append(rf.logs, args.Entries...)
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		go rf.applyLogs()
	}
}
func (rf *Raft) sendAppendEntries(server int,args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	// log.Println("args: ",args,"reply :",reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state == Leader && args.Term == rf.currentTerm{
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
			return
		}else if reply.Term == rf.currentTerm{
			// update matchIndex and nextIndex of the follower
			if reply.Success {
				// match index should not regress in case of stale rpc response
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				if newMatchIndex > rf.matchIndex[server] {
					rf.matchIndex[server] = newMatchIndex
				}
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else{
				if reply.FailTerm < 0 {
					rf.nextIndex[server] = reply.FailIndex
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				} else {
					newNextIndex := rf.getLastIndex()
					for ; newNextIndex >= 0; newNextIndex-- {
						if rf.logs[newNextIndex].Term == reply.FailTerm {
							break
						}
					}
					if newNextIndex < 0 {
						rf.nextIndex[server] = reply.FailIndex
					} else {
						rf.nextIndex[server] = newNextIndex
					}
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
			}
			rf.updateCommit()
		}
	}
	

	
	
}
func (rf *Raft) broadcastAppendEntries() { 
	if rf.state == Leader {
		for server := range rf.peers {
			if server != rf.me {
				args := AppendEntriesArgs{
					Term:			rf.currentTerm,
					LeaderId:		rf.me,
					PrevLogIndex:	rf.nextIndex[server]-1,
					PrevLogTerm:	rf.logs[rf.nextIndex[server]-1].Term,
					LeaderCommit:	rf.commitIndex,
				}
				entries := rf.logs[rf.nextIndex[server]:]
				args.Entries = make([]LogEntry, len(entries))
				copy(args.Entries, entries)
				reply :=AppendEntriesReply{
					Term:			rf.currentTerm,
					Success:		false,
				}
				go rf.sendAppendEntries(server, &args, &reply)
			}
		}
	}
}

func (rf *Raft) updateCommit(){
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.logs[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers) / 2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}
//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	term := rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{term, command})
	
	return rf.getLastIndex(), term, true
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term) // reset raft node state
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		if rf.VoteCheck(args){
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.noBlockChan(rf.grantVoteCh, true)
		}
	}else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		if rf.VoteCheck(args){
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.noBlockChan(rf.grantVoteCh, true)
		}
	}
	
}
func (rf *Raft) VoteCheck(args *RequestVoteArgs) bool{
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId{
		if rf.isLogTheLatest(args.LastLogIndex, args.LastLogTerm){
			return true
		}
	}
	return false 
}
//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Candidate && args.Term == rf.currentTerm{
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
			rf.persist()
			return
		}else if reply.Term == rf.currentTerm{
			if reply.VoteGranted {
				rf.voteCount++
				// only send once when vote count just reaches majority
				if rf.voteCount > len(rf.peers)/2  {
					rf.noBlockChan(rf.winElectCh, true)
					// rf.winElectCh<-true
				}
			}
		}
	}
}
func (rf *Raft) broadcastRequestVote() { 
	if rf.state == Candidate {
		args := RequestVoteArgs{
			Term:         	rf.currentTerm,
			CandidateId:  	rf.me,
			LastLogIndex: 	rf.getLastIndex(),
			LastLogTerm:  	rf.getLastTerm(),
		}
		reply := RequestVoteReply{
			Term:		  	rf.currentTerm,
			VoteGranted:	false,	
		}
		for server := range rf.peers {
			if server != rf.me {
				go rf.sendRequestVote(server, &args, &reply)
			}
		}
	}
}
//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state == Candidate {
		rf.resetChannels()
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for server := range rf.peers {
			rf.nextIndex[server] = rf.getLastIndex() + 1
		}
		rf.broadcastAppendEntries()
	}
}
func (rf *Raft) becomeCandidate(fromState NodeState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == fromState {
		rf.resetChannels()
		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.voteCount = 1
		rf.persist()
		rf.broadcastRequestVote()
	}
}
func (rf *Raft) becomeFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	if state != Follower {
		rf.noBlockChan(rf.becomeFollowerCh, true)
	}
}
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.becomeFollowerCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.becomeFollowerCh:
				// state should already be follower
			case <-time.After(100 * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.getTimeout() * time.Millisecond):
				rf.becomeCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.becomeFollowerCh:
				// state should already be follower
			case <-rf.winElectCh:
				rf.becomeLeader()
			case <-time.After(rf.getTimeout() * time.Millisecond):
				rf.becomeCandidate(Candidate)
			}
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func (rf *Raft) getTimeout() time.Duration {
	return time.Duration(400 + rand.Intn(200))
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// log.Printf("")
	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.resetChannels()
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
