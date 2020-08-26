

#include "muduo/base/Logging.h"
#include "muduo/base/Mutex.h"
#include "muduo/net/EventLoop.h"
#include "muduo/net/TcpServer.h"
#include "nameserver.h"
#include <set>
#include <stdio.h>
#include <unistd.h>
using namespace muduo;
using namespace muduo::net;
int kHeaderLen=sizeof(header_);
class tcpserver
{
 public:
  tcpserver(EventLoop* loop,
             const InetAddress& listenAddr,NameServer* p)
  : server_(loop, listenAddr, "tcpserver"),
    nameserver(p)
  {
    server_.setConnectionCallback(
        std::bind(&tcpserver::onConnection, this, _1));
    server_.setMessageCallback(
        std::bind(&tcpserver::onMessage, this, _1, _2, _3));
  }

  void start()
  {
    
    server_.start();
    
  }
  typedef std::vector<TcpConnectionPtr> ConnectionList;
  ConnectionList connections_;
 private:
  void onConnection(const TcpConnectionPtr& conn)
  {
    LOG_INFO << conn->localAddress().toIpPort() << " -> "
             << conn->peerAddress().toIpPort() << " is "
             << (conn->connected() ? "UP" : "DOWN");

    if (conn->connected())
    {
      connections_.push_back(conn);

      
      if(connections_.size()==4)
      {
        std::unique_lock<std::mutex> lk(nameserver->mtx);
        nameserver->allconnection=true;
        lk.unlock();
        nameserver->cv.notify_all();
      }
    }
    else
    {
      
    }
  }

  void onMessage(const muduo::net::TcpConnectionPtr& conn,
                 muduo::net::Buffer* buf,
                 muduo::Timestamp receiveTime)
  {
    
    while (buf->readableBytes() >= kHeaderLen) // kHeaderLen == 4
    {
      // FIXME: use Buffer::peekInt32()
      header_ head;
      memcpy(&head,buf->peek(),kHeaderLen);
      //printf("yes!!!\n");
      if(head.type==0)
      {
        for(int i=0;i<4;i++)
        {
          
          if(conn==nameserver->server->connections_[i])
          {

            nameserver->dataservers[i].size=head.length;
            nameserver->count++;
            //printf("%d\n",nameserver->count);
          }
        }
        buf->retrieve(kHeaderLen);
        if(nameserver->count==3)
          {
            std::unique_lock<std::mutex> lk(nameserver->mtx);
            nameserver->finish=true;
            lk.unlock();
            nameserver->cv.notify_all();
          }
      }
      else if(head.type==2)
      {
        //printf("type=2\n");
        if(buf->readableBytes()>=kHeaderLen)
        {
          for(int i=0;i<4;i++)
          {
            if(conn==nameserver->server->connections_[i])
            {
              nameserver->dataservers[i].bufsize=head.length;
              nameserver->count++;
            }
          }
          buf->retrieve(kHeaderLen);
          if(nameserver->count==4)
          {
            std::unique_lock<std::mutex> lk(nameserver->mtx);
            nameserver->finish=true;
            lk.unlock();
            nameserver->cv.notify_all();
          }
        }
        else
        {
          break;
        }
        
      }
      else if(head.type==1)
      {
        //printf("type==1\n");
        if(head.length==0)
        {
          //printf("length==0\n");
          for(int i=0;i<4;i++)
          {
            if(conn==nameserver->server->connections_[i])
              {nameserver->dataservers[i].bufsize=0;
              nameserver->count++;
              }
          }
          buf->retrieve(kHeaderLen);
          
        }
        else
        {
          if(buf->readableBytes()>=kHeaderLen+head.length)
          {
            //printf("inread\n");
            buf->retrieve(kHeaderLen);
            for(int i=0;i<4;i++)
            {
              if(conn==nameserver->server->connections_[i])
              {
                //printf("1179***\n");
                nameserver->dataservers[i].buf=new char[head.length];
                memcpy(nameserver->dataservers[i].buf,buf->peek(),head.length);
                buf->retrieve(head.length);
                nameserver->dataservers[i].bufsize=head.length;
                nameserver->count++;
              }
              
            }
           // printf("inretr\n");
            buf->retrieve(head.length);
          }
          else 
          {
            break;
          }
        }
       // std::cout<<nameserver->count<<std::endl;
        if(nameserver->count==4)
              {
                std::unique_lock<std::mutex> lk(nameserver->mtx);
                nameserver->finish=true;
                lk.unlock();
                nameserver->cv.notify_all();
              }
      }
    }
  }

  
  TcpServer server_;
  
  
  NameServer* nameserver;
};