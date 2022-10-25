#pragma once
#include <unistd.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>

#include <vector>
#include <unordered_map>
#include <iostream>
#include <thread>
#include <atomic>
#include <queue>
#include <mutex>

#include <boost/thread.hpp>
#include <boost/asio.hpp>

// #include "buffer_manager.h"
#include "MergeQueryManager.hpp"
// #include "CSDScheduler.h"
#include "test/testmodule.hpp"

#include "document.h"

#include "writer.h"
#include "stringbuffer.h"
#include "prettywriter.h"

#include "grpc/snippet_sample.grpc.pb.h"

using namespace rapidjson;
using namespace std;
#define LBA2PBAPORT 8081


class SnippetStructQueue {
    public:
        // SnippetStructQueue();
        void initqueue();
        void enqueue(SnippetStruct tmpsnippet);
        SnippetStruct dequeue();


    private:
        vector<SnippetStruct> tmpvec;
        int queuecount;
};



class SavedRet{
    // SavedRet(queue<SnippetStruct> queue_){
    //     QueryQueue = queue_;
    // }
    public:
        void NewQuery(queue<SnippetStruct> newqueue);
        unordered_map<string,vector<vectortype>> ReturnResult();
        void lockmutex();
        void unlockmutex();
        int GetSnippetSize();
        SnippetStruct Dequeue();
        void SetResult(unordered_map<string,vector<vectortype>> result);
    private:
        queue<SnippetStruct> QueryQueue;
        unordered_map<string,vector<vectortype>> result_;
        mutex mutex_;
};


class SnippetManager{
    
    public:
        // SnippetManager(TableManager &table, Scheduler &scheduler,CSDManager &csdmanager){
        //     tableManager_ = table;
        //     scheduler_ = scheduler;
        //     csdManager_ = csdmanager;
        // };
        void InitSnippetManager(TableManager &table, Scheduler &scheduler,CSDManager &csdmanager);
        void NewQuery(queue<SnippetStruct> newqueue, BufferManager &buff,TableManager &tableManager_,Scheduler &scheduler_,CSDManager &csdManager_);
        // void NewQuery(SnippetStructQueue &newqueue, BufferManager &buff,TableManager &tableManager_,Scheduler &scheduler_,CSDManager &csdManager_);
        void SnippetRun(SnippetStruct& snippet, BufferManager &buff, TableManager &tableManager_,Scheduler &scheduler_,CSDManager &csdmanager);
        void ReturnColumnType(SnippetStruct& snippet, BufferManager &buff);
        // void NewQuery(queue<SnippetStruct> newqueue, BufferManager &buff,TableManager &tableManager_,Scheduler &scheduler_,CSDManager &csdManager_);
        void NewQueryMain(SavedRet &snippetdata, BufferManager &buff,TableManager &tblM,Scheduler &scheduler_,CSDManager &csdManager_);
        void GetIndexNumber(string TableName, vector<int> &IndexNumber);
        bool GetWALTable(SnippetStruct& snippet);
        unordered_map<string,vector<vectortype>> ReturnResult(int queryid);
    private:
        unordered_map<int,SavedRet> SavedResult;
        
        // TableManager &tableManager_;
        // Scheduler &scheduler_;
        // CSDManager &csdManager_;
};

int my_LBA2PBA(std::string &req_json,std::string &res_json);

void sendToSnippetScheduler(SnippetStruct &snippet, BufferManager &buff, Scheduler &scheduler_, TableManager &tableManager_, CSDManager &csdManager);