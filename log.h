#include<stdlib.h>
#include<vector>
#include<string>
namespace raft{
    class Log{
        private:
            std::vector<LogEntry> LogEntries;
    };
    class LogEntry{
        private:
            int logIndex;
            int term;
            std::string command;
    };
}