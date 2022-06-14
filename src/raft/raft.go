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
type NodeState int 
const(
	Leader    		NodeState = 1
	Candidate       NodeState = 2
	Follower       	NodeState = 3
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
	grantVoteCh chan bool
	heartbeatCh chan bool

	lastIncludedIndex	int
	lastIncludedTerm 	int
}
type LogEntry struct{
	Term    int
	Command interface{}
}
//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//


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
type InstallSnapshotArgs struct{
	Term 				int 
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	// Offset				int
	Data				[]byte
	// Done				bool	//Figure 13 split the snapshot,but I don't
}
type InstallSnapshotReply struct{
	Term			int
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//modify logs and other state
	if args.Term<rf.currentTerm||args.LastIncludedIndex<rf.lastIncludedIndex{  // in the normal situation not occur
		reply.Term=rf.currentTerm
		return 
	}
	rf.currentTerm=args.Term
	reply.Term=args.Term
	if args.LastIncludedIndex<rf.getLastIndex(){
		rf.lastIncludedIndex=args.LastIncludedIndex
		rf.lastIncludedTerm=args.LastIncludedTerm
		newLogs := make([]LogEntry,0)
		newLogs = append(newLogs,LogEntry{
			Term:	rf.lastIncludedTerm,
		})
		for snapIndex:=1;snapIndex<rf.getLogLenth();snapIndex++{
			if rf.getReorderedLogIndex(snapIndex)>args.LastIncludedIndex{
				newLogs=append(newLogs,rf.logs[snapIndex])
			}
		}
		if rf.commitIndex<args.LastIncludedIndex{
			rf.commitIndex=args.LastIncludedIndex
		}
		if rf.lastApplied<args.LastIncludedIndex{
			rf.lastApplied=args.LastIncludedIndex
		}
		rf.logs=newLogs
	}else{
		// empty the logs
		rf.lastIncludedIndex=args.LastIncludedIndex
		rf.lastIncludedTerm=args.LastIncludedTerm
		newLogs := make([]LogEntry,0)
		newLogs = append(newLogs,LogEntry{
			Term:	rf.lastIncludedTerm,
		})
		if rf.commitIndex<args.LastIncludedIndex{
			rf.commitIndex=args.LastIncludedIndex
		}
		if rf.lastApplied<args.LastIncludedIndex{
			rf.lastApplied=args.LastIncludedIndex
		}
		rf.logs=newLogs
	}
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.applyCh<-msg
	rf.persister.SaveStateAndSnapshot(rf.persistData(),args.Data)
}

func (rf *Raft) leaderSendInstallSnapshot(server int){
	args:=InstallSnapshotArgs{
		Term :				rf.currentTerm,
		LeaderId:			rf.me,
		LastIncludedIndex:	rf.lastIncludedIndex,
		LastIncludedTerm:	rf.lastIncludedTerm,
		Data:				rf.persister.ReadSnapshot(),
	}
	reply:=InstallSnapshotReply{}
	LogPrint("args : %v\n",args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm{
		rf.becomeFollower()
		rf.currentTerm=reply.Term
		rf.persist()

	}
	rf.updateNextAndMatch()
	if !ok{
		return 
	}
}
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) { 
	// Your code here (2D).
	// LogPrint("Before Take %d snapshot!!!------%d logs %v\n",index,rf.getLogLenth(),rf.logs)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex<index || rf.lastIncludedIndex>=index{  // if commitIndex < index can't snapshot the log before index
							//if rf.lastIncludedIndex>=index means the snapshot has been installed don't need to be install again
		return 
	}
	
	newLogs := make([]LogEntry,0)
	newLogs = append(newLogs,LogEntry{Term : rf.getLogTerm(index)})
	// newLogs = append(newLogs,rf.logs[index+1:]...)
	for i:=index+1;i<=rf.getLastIndex();i++{
		newLogs=append(newLogs,rf.logs[rf.getLogIndex(i)])
	}
	rf.lastIncludedTerm=rf.getLogTerm(index)
	rf.lastIncludedIndex=index
	if rf.commitIndex<index{
		rf.commitIndex=index
	}
	if rf.lastApplied<index{
		rf.lastApplied=index
	}
	rf.logs=newLogs
	// LogPrint("After Take %d snapshot!!!***********%d  rf commitIndex = %d, rf lastApply = %d last<%d,%d>\n",index,rf.getLogLenth(),
	// rf.commitIndex,rf.lastApplied,rf.lastIncludedIndex,rf.lastIncludedTerm)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	return data
}
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
	data :=rf.persistData()
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
	var currentTerm 		int
	var votedFor			int 
	var logs				[]LogEntry
	var lastIncludedIndex	int
	var lastIncludedTerm	int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || 
	   d.Decode(&lastIncludedIndex) != nil|| d.Decode(&lastIncludedTerm) != nil{
		log.Println("DecodeError")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.logs=make([]LogEntry, len(logs))
	  rf.lastIncludedIndex=lastIncludedIndex
	  rf.lastIncludedTerm=lastIncludedTerm
	  copy(rf.logs,logs)
	//   log.Println("Decode Term: ",rf.currentTerm,"raft logs :",rf.logs)
	}
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
	RollBackIndex 	int
	RollBackTerm  	int
}


func (rf *Raft) noBlockChan(ch chan bool, value bool) {
	select {
	case ch <- value:
	default:
	}
}

func (rf *Raft) isLogTheLatest(LastIndex int, LastTerm int) bool {
	if LastTerm == rf.getLastTerm() {
		return LastIndex >= rf.getLastIndex()
	}
	return LastTerm > rf.getLastTerm()
}
func (rf *Raft) applyLogs() {
	// log.Println("commitID: ",rf.commitIndex,"server ID : ",rf.me)
	// LogPrint("raft info :%d ",rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.lastApplied = i
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			// Command: rf.logs[i].Command,
			Command: rf.logs[rf.getLogIndex(i)].Command,
			CommandIndex: i,
		}
		
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	LogPrint("server%d logs length:%d lastApplied %d lastSnapshot %d args %v",rf.me,rf.getLogLenth(),rf.lastApplied,rf.lastIncludedIndex,args)
	if args.Term < rf.currentTerm {  
		reply.Term = rf.currentTerm
		reply.RollBackIndex = -1
		reply.RollBackTerm = -1
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower()
		rf.currentTerm=args.Term
	}

	lastIndex := rf.getLastIndex()
	rf.noBlockChan(rf.heartbeatCh, true)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.RollBackIndex = -1
	reply.RollBackTerm = -1

	//log check
	if args.PrevLogIndex > lastIndex {// follower log is shorter than leader
		reply.RollBackIndex = lastIndex + 1 // quick find the postion 
		return
	}
	
	// if  rf.logs[args.PrevLogIndex].Term  != args.PrevLogTerm {	//has different term at prevLogIndex with leader
	if  rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// start check whether the log has entry with the same term with rf.logs[args.PrevLogIndex].Term 
		// if there has, return the index,quick find the sync postion ,if not set RollBackIndex = 0
		reply.RollBackTerm = rf.getLogTerm(args.PrevLogIndex)				  
		for i := args.PrevLogIndex; i >= rf.lastIncludedIndex && rf.getLogTerm(i) == rf.getLogTerm(args.PrevLogIndex) ; i-- { 
			reply.RollBackIndex = i
		}
		return
	}
	//if pass the log check,server has the same log with the Leader before args.PrevLogIndex + 1
	startClean := args.PrevLogIndex + 1
	startAppend :=0
	newlogs := make([]LogEntry,0)
	for ; startClean<rf.getLastIndex()+1&& startAppend<len(args.Entries);startClean,startAppend=startClean+1,startAppend+1{
		if rf.getLogTerm(rf.getLogIndex(startClean))!=args.Entries[startAppend].Term{
			break
		}
	}
	for index:=rf.lastIncludedIndex;index<startClean;index++{
		newlogs=append(newlogs,rf.logs[rf.getLogIndex(index)])
	}
	rf.logs=newlogs
	args.Entries = args.Entries[startAppend:]
	rf.logs=append(rf.logs,args.Entries...)
	
	reply.Success = true
	rf.checkCommitIndex(args.LeaderCommit)
}

func (rf *Raft) checkCommitIndex(commit int){
	if  commit> rf.commitIndex {
		if commit < rf.getLastIndex() {
			rf.commitIndex = commit
		} else {
			rf.commitIndex = rf.getLastIndex()
		}
		go rf.applyLogs()
	}
}

func (rf *Raft) sendAppendEntries(server int,args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	LogPrint("leader lastSnapshot %d logs length %d,send args %v , reply %v\n",rf.lastIncludedIndex,rf.getLogLenth(),args,reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state == Leader && args.Term == rf.currentTerm{
		if rf.nextIndex[server]-1 < rf.lastIncludedIndex{
			go rf.leaderSendInstallSnapshot(server)
			return 
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower()
			rf.currentTerm=args.Term
			return
		}else if reply.Term == rf.currentTerm{
			if reply.Success {
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				if newMatchIndex > rf.matchIndex[server] {
					rf.matchIndex[server] = newMatchIndex
				}
				rf.nextIndex[server] = newMatchIndex + 1
				// LogPrint("Dead\n")
				rf.updateCommit()
			} else{
				// LogPrint("fail\n")
				if reply.RollBackTerm < rf.lastIncludedTerm {
					rf.nextIndex[server] = rf.lastIncludedIndex +1
					rf.matchIndex[server] = rf.lastIncludedIndex
					LogPrint("nextIndex[%d] = %d \n",server,rf.nextIndex[server])
				} else {
					newNextIndex := rf.getLastIndex()
					for ; newNextIndex >= rf.lastIncludedIndex; newNextIndex-- {
						// if rf.logs[newNextIndex].Term == reply.RollBackTerm {
						if rf.getLogTerm(newNextIndex) == reply.RollBackTerm {
							break
						}
					}
					if newNextIndex < rf.lastIncludedIndex {  
						rf.nextIndex[server] = reply.RollBackIndex
					} else {
						rf.nextIndex[server] = newNextIndex
					}
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
				rf.updateCommit()
				
				// args.PrevLogIndex=rf.nextIndex[server]-1
				// args.PrevLogTerm=rf.logs[args.PrevLogIndex].Term
				// args.LeaderCommit=rf.commitIndex
				// go rf.sendAppendEntries(server,args,reply)
			}

		}
		
	}
}

func (rf *Raft) broadcastAppendEntries() { 
	if rf.state == Leader {
		for server := range rf.peers {
			if server != rf.me {
				LogPrint("Bordcast lastSnapshot%d lastApplied%d nextindex[%d] -1 = %d  getLastIndex = %d\n",
				rf.lastIncludedIndex,rf.lastApplied,server,rf.nextIndex[server]-1 ,rf.getLastIndex()) 
				args := AppendEntriesArgs{
					Term:			rf.currentTerm,
					LeaderId:		rf.me,
					PrevLogIndex:	rf.nextIndex[server]-1,
					// PrevLogTerm:	rf.logs[rf.nextIndex[server]-1].Term,
					PrevLogTerm:	rf.getLogTerm(rf.nextIndex[server]-1),
					LeaderCommit:	rf.commitIndex,
				}
				// log.Printf("getLogTerm(%v) : %v \n",rf.nextIndex[server]-1,rf.getLogTerm(rf.nextIndex[server]-1))
				// entries := rf.logs[rf.nextIndex[server]:]
				entries :=make([]LogEntry,0)
				for index:=rf.nextIndex[server];index<=rf.getLastIndex();index++{
					entries=append(entries,rf.logs[rf.getLogIndex(index)])
				}
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
		// if rf.logs[n].Term == rf.currentTerm {
		if rf.getLogTerm(n) == rf.currentTerm {
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
	if rf.commitIndex<rf.lastIncludedIndex{
		rf.commitIndex=rf.lastIncludedIndex
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
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	term := rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{term, command})
	rf.persist()
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
		rf.becomeFollower()
		rf.currentTerm=args.Term
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
	// log.Println("Vote Candidate:",rf.me,"Servers size ",len(rf.peers),"args ",args,"reply ",reply,"logs ",rf.logs)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Candidate && args.Term == rf.currentTerm{
		if reply.Term > rf.currentTerm {
			rf.becomeFollower()
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}else if reply.Term == rf.currentTerm{
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount == len(rf.peers)/2 +1 {
					rf.noBlockChan(rf.winElectCh, true)
					// rf.winElectCh<-true
				}
			}
		}
	}
}

func (rf *Raft) broadcastRequestVote() { 
	if rf.state == Candidate {
		for server := range rf.peers {
			if server != rf.me {
				args := RequestVoteArgs{
					Term:         	rf.currentTerm,
					CandidateId:  	rf.me,
					LastLogIndex: 	rf.getLastIndex(),
					LastLogTerm:  	rf.getLastTerm(),
				}
				reply := RequestVoteReply{
					Term:			rf.currentTerm,
					VoteGranted:	false,
				}
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
			case <-rf.heartbeatCh:
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
				rf.becomeCandidate()
			}
		case Candidate:
			select {
			case <-rf.heartbeatCh:
			case <-rf.winElectCh:
				rf.becomeLeader()
			case <-time.After(rf.getTimeout() * time.Millisecond):
				rf.becomeCandidate()
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
	rf.lastIncludedIndex=0
	rf.lastIncludedTerm=0
	rf.resetChannels()
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
