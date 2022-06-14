package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func LogPrint(format string, a ...interface{}) (n int, err error) {
	// log.Printf(format, a...)
	return
}
func (rf *Raft) getReorderedLogIndex(index int) int{  //input a true index of log return the log after snapshot
	return index+rf.lastIncludedIndex
}
func (rf *Raft) getLogIndex(index int) int{  //input a index larger than logs lenth return the true index of the log
	if index < rf.lastIncludedIndex{
		return 0
	}
	return index-rf.lastIncludedIndex
}
func (rf *Raft) getLogTerm(index int) int{
	// LogPrint("GetLogTerm(%d) lastApply%d ,lastSnapshot %d\n",index,rf.lastApplied,rf.lastIncludedIndex)
	if index < rf.lastIncludedIndex{
		return -1
	}
	if index == rf.lastIncludedIndex{
		return rf.lastIncludedTerm
	}
	index = index - rf.lastIncludedIndex  
	if index>rf.getLogLenth()-1 {
		return -1
	}
	return rf.logs[index].Term
}
func (rf *Raft)getLastIndex() int{
	return rf.getLogLenth()-1 + rf.lastIncludedIndex
}
func (rf *Raft)getLogLenth() int {
	return len(rf.logs)
}
func (rf *Raft)getLastTerm() int{
	if rf.getLogLenth()==1{
		return rf.lastIncludedTerm
	}
	return rf.logs[rf.getLogLenth()-1].Term
}
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
}
func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Candidate {
		rf.state = Leader
		rf.resetChannels()
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for server := range rf.peers {
			rf.nextIndex[server] = rf.getLastIndex() + 1
		}
		rf.broadcastAppendEntries()
	}
}
func (rf *Raft) updateNextAndMatch(){
	for server := range rf.peers {
		if rf.nextIndex[server] <= rf.lastIncludedIndex{
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			rf.matchIndex[server] = rf.nextIndex[server]-1
		}
	}
}
func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetChannels()
	rf.state = Candidate
	rf.noBlockChan(rf.heartbeatCh, true)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
	rf.broadcastRequestVote()
	
}
func (rf *Raft) becomeFollower() {
	rf.state = Follower
	rf.votedFor = -1
	// rf.persist()
	rf.noBlockChan(rf.heartbeatCh, true)
}
