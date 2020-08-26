#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include "nameserver.h"
#include "util.h"
#include "muduo/net/EventLoopThread.h"
#include "tcpserver.h"
#include <thread>
NameServer::NameServer(int numReplicate):numReplicate_(numReplicate), idCnt_(0){
    dataserver_ data;
    dataservers.push_back(data);
    dataservers.push_back(data);
    dataservers.push_back(data);
    dataservers.push_back(data);
}




std::vector<std::string> NameServer::parse_cmd(){
    std::cout << "MiniDFS> ";
    std::string cmd, tmp;
    std::getline(std::cin, cmd);
    std::vector<std::string> parameters;
    std::stringstream ss(cmd);
    while(ss >> tmp)
        parameters.push_back(tmp);
    return parameters;
}
void NameServer::operator()(){

    allconnection=false;
    std::unique_lock<std::mutex> lk(mtx);
    cv.wait(lk, [&](){return this->allconnection;});
    lk.unlock();
    allconnection=false;
    count=0;
    while(true){
        std::vector<std::string> parameters = parse_cmd();
        std::vector<int> idx;
        std::ifstream is;
        char *buf = nullptr;
        // md5 checksum for replicate chunks;
        MD5 md5;
        finish=false;
        if(parameters.empty()){
            std::cerr << "input a blank line" << std::endl;
            continue;
        }
        if(parameters[0] == "quit"){
            exit(0);
        }
        // list all the files in name server.
        else if (parameters[0] == "list" || parameters[0] == "ls"){
            if (parameters.size() != 1){
                std::cerr << "useage: " << "list (list all the files in name server)" << std::endl;
                finish=true;
            }
            else{
                std::cout << "file\tFileID\tChunkNumber" << std::endl;
                fileTree_.list(meta);
                finish=true;
            }
            continue;
        }
        // upload file to miniDFS
        else if (parameters[0] == "put"){
            if(parameters.size()!=3){
                std::cerr << "useage: " << "put source_file_path des_file_path" << std::endl;
                continue;
            }
            is.open(parameters[1], std::ifstream::ate | std::ifstream::binary);
            if(!is){
                std::cerr << "open file error: file " << parameters[1] << std::endl;
                continue;
            }
            else if (!fileTree_.insert_node(parameters[2], true)){
                std::cerr << "create file error \n.maybe the file : " << parameters[2] << "exists" << std::endl;
                continue;
            }
            else{
                //std::cout<<"###"<<std::endl;
                int totalSize = is.tellg();
                buf = new char[totalSize];
                is.seekg(0, is.beg);
                is.read(buf, totalSize);
                std::vector<double> serverSize;
                for (int i=0;i<4;i++)
                    serverSize.push_back(dataservers[i].size);
                idx = argsort<double>(serverSize);
                //std::cout<<idx[1];
                // std::cout << "total size " << totalSize << std::endl;
                ++idCnt_;
                //std::cout<<"!!!!"<<std::endl;
                for(int i=0; i<numReplicate_; ++i){
                    std::string message;
                    message+="put\r\n";
                    message+=std::to_string(idCnt_)+"\r\n";
                    header_ head;
                    head.type=0;
                    head.length=message.length();
                    
                    meta[parameters[2]] = std::make_pair(idCnt_, totalSize);
                    //std::cout<<"???"<<std::endl;
                    //std::cout<<idx[i]<<std::endl;
                    server->connections_[idx[i]]->send(&head,sizeof(header_));
                   // std::cout<<"@@@@"<<std::endl;
                    server->connections_[idx[i]]->send(message.c_str(),message.length());
                    head.type=1;
                    head.length=totalSize;
                    server->connections_[idx[i]]->send(&head,sizeof(header_));
                    server->connections_[idx[i]]->send(buf,totalSize);
                    //std::cout<<"!!!!"<<std::endl;
                }
            }
        }
        // fetch file from miniDFS
        else if (parameters[0] == "read" || parameters[0] == "fetch"){
            if(parameters.size() != 3 && parameters.size()!=4){
                std::cerr << "useage: " << "read source_file_path dest_file_path" << std::endl;
                std::cerr << "useage: " << "fetch FileID Offset dest_file_path" << std::endl;
                continue;
            }
            else{
                if(parameters[0] == "read" && meta.find(parameters[1]) == meta.end()){
                    std::cerr << "error: no such file in miniDFS." << std::endl;
                    continue;
                }
                for(int i=0; i<4; ++i){
                    
                    std::string message=parameters[0]+"\r\n";
                    if(parameters[0] == "read"){
                        std::pair<int, int> metaData = meta[parameters[1]];
                        message+=std::to_string(metaData.first)+"\r\n";
                        message+=std::to_string(metaData.second)+"\r\n";
                       
                    }
                    else{
                        message+=parameters[1]+"\r\n";
                        message+=parameters[2]+"\r\n";
                        
                    }
                    header_ head;
                    head.type=0;
                    head.length=message.length();
                    server->connections_[i]->send(&head,sizeof(head));
                    server->connections_[i]->send(message.c_str(),message.length());
                }
            }
        }
        // locate the data server given file ID and Offset.
        else if (parameters[0] == "locate"){
            if (parameters.size() != 3){
                std::cerr << "useage: " << "locate fileID Offset" << std::endl;
                continue;
            }
            else{
                for(int i=0; i<4; ++i){
                
                    std::string message="locate\r\n";
                    message+=parameters[1]+"\r\n";
                    message+=parameters[2]+"\r\n";
                    header_ head;
                    head.type=0;
                    head.length=message.length();
                    server->connections_[i]->send(&head,sizeof(head));
                    server->connections_[i]->send(message.c_str(),message.length());
                }
            }
        }
        else
            std::cerr << "wrong command." << std::endl;

        // waiting for the finish of data server.
        
        printf("发送结束\n");
         std::unique_lock<std::mutex> lk(mtx);
         cv.wait(lk, [&](){return finish;});
         lk.unlock();
         count=0;
         printf("处理结束\n");
       
        

        // work after processing of data server
        if(parameters[0] == "read" || parameters[0] == "fetch"){
            std::string md5_checksum, pre_checksum;
            for (int i=0; i<4; ++i){
                if(dataservers[i].bufsize){
                    std::ofstream os;
                    if(parameters[0] == "read")
                        os.open(parameters[2]);
                    else if (parameters[0] == "fetch")
                        os.open(parameters[3]);
                    if(!os)
                        std::cerr << "create file failed. maybe wrong directory." << std::endl;
                    else{
                        os.write(dataservers[i].buf, dataservers[i].bufsize);
                        os.close();
                        md5.update(dataservers[i].buf, dataservers[i].bufsize);
                        md5.finalize();
                        md5_checksum = md5.toString();
                        if(!pre_checksum.empty() && pre_checksum != md5_checksum){
                            std::cerr << "error: unequal checksum for files from different dataServers. File got may be wrong." << std::endl;
                        }
                        pre_checksum = md5_checksum;
                    }
                    delete []dataservers[i].buf;
                }
            }
        }
        else if (parameters[0] == "put") {
            std::cout << "Upload success. The file ID is " << idCnt_ << std::endl;
        }
        else if (parameters[0] == "locate" || parameters[0] == "ls") {
            bool notFound = true;
            for(int i=0; i<4; ++i){
                if(dataservers[i].bufsize){
                    notFound = false;
                    std::cout << "found FileID " << parameters[1] << " offset " << parameters[2] << " at " << "ds"<<i << std::endl;
                }
            }
            if (notFound)
                std::cout << "not found FileID " << parameters[1] << " offset " << parameters[2] << std::endl;
        }
        delete []buf;
        is.close();
    }
}

int main()
{
    EventLoop loop;
    
    InetAddress listenAddr(2021);
    NameServer nameserver(3);
    tcpserver server(&loop,listenAddr,&nameserver);
    nameserver.server=&server;
    std::thread th1(std::ref(nameserver));
    th1.detach();
    server.start();
    loop.loop();
}