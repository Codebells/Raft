#include<stdlib.h>
#include<ctime>
#include<vector>

#include"state.h"

namespace raft{
    class RaftNode{
        public:
            int  raft_periodic();//周期检查
            void random_time();//随机超时时间
            int  raft_send_append_entries_rpc();
            int  raft_receive_append_entries_rpc();
            int  raft_send_request_vote_rpc();
            int  raft_receive_request_vote_rpc();
            int  raft_apply_state_machine();

        private:
            int nodeId;
            State node_state;
            int time_elasped;
            int elect_timeout;
            int request_timeout;
            int node_num;
            std::vector<int>nodes_id;
            int leaderID;    
            int nextIndex; //the index of next log entry to send to node 
            int matchIndex;//the highest index of log entry that have been replicated in node    
              
    };
}