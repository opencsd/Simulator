#pragma once
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

// #include "buffer_manager.h"
#include "MergeQueryManager.hpp"
// #include "CSDScheduler.h"
#include "document.h"
#include "grpc/snippet_sample.grpc.pb.h"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "test/testmodule.hpp"
#include "writer.h"

using namespace rapidjson;
using namespace std;

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

class SavedRet {
  // SavedRet(queue<SnippetStruct> queue_){
  //     QueryQueue = queue_;
  // }
 public:
  void NewQuery(queue<SnippetStruct> newqueue);
  unordered_map<string, vector<vectortype>> ReturnResult();
  void lockmutex();
  void unlockmutex();
  int GetSnippetSize();
  SnippetStruct Dequeue();
  void SetResult(unordered_map<string, vector<vectortype>> result);

 private:
  queue<SnippetStruct> QueryQueue;
  unordered_map<string, vector<vectortype>> result_;
  mutex mutex_;
};

class SnippetManager {
 public:
  // SnippetManager(TableManager &table, Scheduler &scheduler,CSDManager
  // &csdmanager){
  //     tableManager_ = table;
  //     scheduler_ = scheduler;
  //     csdManager_ = csdmanager;
  // };
  void InitSnippetManager(TableManager &table, Scheduler &scheduler,
                          CSDManager &csdmanager);
  void NewQuery(queue<SnippetStruct> newqueue, BufferManager &buff,
                TableManager &tableManager_, Scheduler &scheduler_,
                CSDManager &csdManager_);
  // void NewQuery(SnippetStructQueue &newqueue, BufferManager
  // &buff,TableManager &tableManager_,Scheduler &scheduler_,CSDManager
  // &csdManager_);
  void SnippetRun(SnippetStruct &snippet, BufferManager &buff,
                  TableManager &tableManager_, Scheduler &scheduler_,
                  CSDManager &csdmanager);
  void ReturnColumnType(SnippetStruct &snippet, BufferManager &buff);
  // void NewQuery(queue<SnippetStruct> newqueue, BufferManager
  // &buff,TableManager &tableManager_,Scheduler &scheduler_,CSDManager
  // &csdManager_);
  void NewQueryMain(SavedRet &snippetdata, BufferManager &buff,
                    TableManager &tblM, Scheduler &scheduler_,
                    CSDManager &csdManager_);
  void GetIndexNumber(string TableName, vector<int> &IndexNumber);
  bool GetWALTable(SnippetStruct &snippet);
  unordered_map<string, vector<vectortype>> ReturnResult(int queryid);

 private:
  unordered_map<int, SavedRet> SavedResult;

  // TableManager &tableManager_;
  // Scheduler &scheduler_;
  // CSDManager &csdManager_;
};

void sendToSnippetScheduler(SnippetStruct &snippet, BufferManager &buff,
                            Scheduler &scheduler_, TableManager &tableManager_,
                            CSDManager &csdManager);