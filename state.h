#include<stdlib.h>
#include<vector>
#include"log.h"
#include"rpc.h"
namespace raft{
    class State{
        public:
            enum nodestate{
                Leader=0,
                Follower=1,
                Candidate=2
            };
            void becomeCanditate();
            int  leader_election();
            int  rpc_send_term();
            void increase_term();
            void becomeLeader();
            void becomeFollower();
            bool vote(RequestVoteRpc req);
        private:
            // node state info
            int currentTerm;//the latest Term in Server
            int voteFor; //vote for which candidate
            Log log;//node's log 

            nodestate state; 
            int commitIndex;// the highest index of log entry that have been committed
            int lastApplied;// the highest index of log entry that have been applied to state machine
            // leader state info
            
    };
}
/*
typedef struct persstate{
    int currentTerm;
    int VoteFor;
    std::vector<int> log;
}PersState;

typedef struct volatilestate{
    int commitIndex;
    int lastApplied;
}VolatileState;

typedef struct leaderState{
    std::vector<int> nextIndex[MAX_LENGTH];
    std::vector<int> matchIndex[MAX_LENGTH];
}LeaderState;
*/

