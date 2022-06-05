#include<stdlib.h>
#include"log.h"
#include<vector>
namespace raft{
    class AppendEntriesRpc{
        private:
            int term;
            int leaderID;
            int prevLogIndex;
            int prevLogTerm;
            std::vector<LogEntry> entries;
            int leaderCommitIndex;
    };
    class AppendEntriesRpcResult{
        private:
            int term;
            bool success;
    };
    class RequestVoteRpc{
        private:
            int term; //now term
            int candidateID;
            int lastLogIndex;
            int lastLogTerm;
    };
    class RequestVoteRpcResult{
        private:
            int term; 
            bool voteGranted;//true means candidate received vote
    };
}

