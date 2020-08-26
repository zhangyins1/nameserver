#ifndef _NAME_SERVER_H
#define _NAME_SERVER_H
#include <mutex>
#include <condition_variable>
#include <vector>
#include <map>
#include <unistd.h>
#include "filetree.h"

struct dataserver_{
    int bufsize=0;
    char* buf=nullptr;
    double size=0;
};
struct header_
{
  int32_t type;
  int32_t length;
};
class tcpserver;
class NameServer{
private:
    std::vector<char*> bufs;
    std::vector<int> bufsizes;
    FileTree fileTree_;
    int numReplicate_;
    int idCnt_;
    
    std::vector<std::string> parse_cmd();
public:
    std::map<std::string, std::pair<int, int>> meta;
    
    explicit NameServer(int numReplicate);
    tcpserver* server;
    void operator()();
    std::mutex mtx;
    std::condition_variable cv;
    bool finish;
    bool allconnection;
    int count;
    std::vector<dataserver_> dataservers;
};

#endif