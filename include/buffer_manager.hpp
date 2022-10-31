#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <any>
#include <bitset>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "CSDScheduler.hpp"
#include "TableManager.hpp"
#include "document.h"
#include "keti_type.hpp"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

using namespace std;
using namespace rapidjson;

#define NUM_OF_BLOCKS 15
#define BUFF_SIZE (NUM_OF_BLOCKS * 4096)
#define PORT_BUF 8888
#define NCONNECTION 8

template <typename T>
class WorkQueue;
struct Block_Buffer;
struct Work_Buffer;

template <typename T>
class WorkQueue {
  condition_variable work_available;
  mutex work_mutex;
  queue<T> work;

 public:
  void push_work(T item) {
    unique_lock<mutex> lock(work_mutex);

    bool was_empty = work.empty();
    work.push(item);

    lock.unlock();

    if (was_empty) {
      work_available.notify_one();
    }
  }

  T wait_and_pop() {
    unique_lock<mutex> lock(work_mutex);
    while (work.empty()) {
      work_available.wait(lock);
    }

    T tmp = work.front();

    work.pop();
    return tmp;
  }

  bool is_empty() { return work.empty(); }

  void qclear() { work = queue<T>(); }

  int get_size() { return work.size(); }
};

struct BlockResult {
  int query_id;
  int work_id;
  string csd_name;
  unordered_map<string, vector<vectortype>> csd_table_data;
  vector<string> sstList;

  BlockResult() {}
  BlockResult(const char *json_) {
    Document document;
    document.Parse(json_);

    query_id = document["queryID"].GetInt();
    work_id = document["workID"].GetInt();
    csd_name = document["csdName"].GetString();
    Value &data = document["data"];
    int data_size = data.Size();
    for (int i = 0; i < data_size; i++) {
      string colName = data["colName"].GetString();
      csd_table_data[colName].clear();
      Value &colData = data["colData"];
      int colData_size = colData.Size();
      for (int j = 0; j < colData_size; j++) {
        Value &colVec = colData[j];
        vectortype tmpType;
        tmpType.strvec = colVec["strVec"].GetString();
        tmpType.type = colVec["type"].GetInt();
        csd_table_data[colName].push_back(tmpType);
      }
    }
    Value &sstList_ = document["sstList"];
    int sstList_size = sstList_.Size();
    for (int i = 0; i < sstList_size; i++) {
      sstList.push_back(sstList_[i].GetString());
    }
  }
};

struct Work_Buffer {
  string table_alias;           //*결과 테이블의 별칭
  vector<string> table_column;  //*결과의 컬럼 이름
  vector<int> return_datatype;  //*결과의 컬럼 데이터 타입(돌아올때 확인)
  vector<int> table_datatype;  //저장되는 결과의 컬럼 데이터 타입(위에서 확인)
  vector<int> table_offlen;  //*결과의 컬럼 길이
  unordered_map<string, vector<vectortype>> table_data;  //결과의 컬럼 별 데이터
  int left_block_count;                                  //*남은 블록 수
  bool all_csd_done;                                     //작업 완료 여부
  condition_variable cond;
  mutex mu;
  unordered_map<string, vector<string>> csd_sst_map;
  vector<string> sstList;
  int csdCNT;
  // int table_type;//테이블 생성 타입?

  Work_Buffer(string table_alias_, vector<string> table_column_,
              vector<int> return_datatype_, vector<int> table_offlen_,
              int total_blobk_cnt_, vector<string> sstList_) {
    table_alias = table_alias_;
    sstList.assign(sstList_.begin(), sstList_.end());
    table_column.assign(table_column_.begin(), table_column_.end());
    return_datatype.assign(return_datatype_.begin(), return_datatype_.end());
    table_offlen.assign(table_offlen_.begin(), table_offlen_.end());
    left_block_count = sstList_.size();
    all_csd_done = false;
    csdCNT = 0;
    table_datatype.clear();
    for (int i = 0; i < table_column.size(); i++) {
      string colName = table_column[i];
      table_data[colName].clear();
    }
    vector<int>::iterator ptr2;
    for (ptr2 = return_datatype.begin(); ptr2 != return_datatype.end();
         ptr2++) {
      if ((*ptr2) == static_cast<int>(MySQL_DataType::MySQL_BYTE)) {
        table_datatype.push_back(static_cast<int>(KETI_Type::KETI_INT8));
      } else if ((*ptr2) == static_cast<int>(MySQL_DataType::MySQL_VARSTRING)) {
        table_datatype.push_back(static_cast<int>(KETI_Type::KETI_STRING));
      } else {
        table_datatype.push_back((*ptr2));
      }
    }
  }
};

struct Query_Buffer {
  int query_id;                                        //쿼리ID
  int work_cnt;                                        //저장된 워크 개수
  unordered_map<int, Work_Buffer *> work_buffer_list;  //워크버퍼
  unordered_map<string, pair<int, int>>
      table_status;  //테이블별 상태<key:table_name||alias,
                     // value:<work_id,is_done> >

  Query_Buffer(int qid) : query_id(qid) {
    work_cnt = 0;
    work_buffer_list.clear();
    table_status.clear();
  }
};

struct TableData {
  bool valid;                                            //결과의 유효성
  unordered_map<string, vector<vectortype>> table_data;  //결과의 컬럼 별 데이터

  TableData() {
    valid = true;
    table_data.clear();
  }
};

struct TableInfo {
  vector<string> table_column;  //*결과의 컬럼 이름
  vector<int> table_datatype;   //저장되는 결과의 컬럼 데이터 타입
  vector<int> table_offlen;     //*결과의 컬럼 길이

  TableInfo() {
    table_column.clear();
    table_datatype.clear();
    table_offlen.clear();
  }
};

class BufferManager : public Socket {
 public:
  BufferManager() {}
  BufferManager(Scheduler &scheduler) { InitBufferManager(scheduler); }
  int InitBufferManager(Scheduler &scheduler);
  int Join();
  void BlockBufferInput();
  void BufferRunning(Scheduler &scheduler);
  void MergeBlock(BlockResult result, Scheduler &scheduler);
  // int GetData(Block_Buffer &dest);
  int InitWork(int qid, int wid, string table_alias,
               vector<string> table_column_, vector<int> table_datatype,
               vector<int> table_offlen_, int total_blobk_cnt_,
               vector<string> sstList);
  void SendCSDReturn(int qid, int wid, string table_alias,
                     vector<string> table_column_, vector<int> table_datatype,
                     vector<int> table_offlen_, int total_blobk_cnt_);
  void InitQuery(int qid);
  int CheckTableStatus(int qid, string tname);
  TableInfo GetTableInfo(int qid, string tname);
  TableData GetTableData(int qid, string tname);
  int SaveTableData(int qid, string tname,
                    unordered_map<string, vector<vectortype>> table_data_);
  int DeleteTableData(int qid, string tname);
  int EndQuery(int qid);
  virtual void Accept(int client_fd) override {
    std::string json = "";
    int njson;
    size_t ljson;

    recv(client_fd, &ljson, sizeof(ljson), 0);

    char buffer[ljson] = {0};

    while (1) {
      if ((njson = recv(client_fd, buffer, BUFF_SIZE - 1, 0)) == -1) {
        perror("read");
        exit(1);
      }
      ljson -= njson;
      buffer[njson] = '\0';
      json += buffer;

      if (ljson == 0) break;
    }

    BlockResultQueue.push_work(BlockResult(json.c_str()));
  }

  unordered_map<int, struct Query_Buffer *> my_buffer_m() {
    return this->m_BufferManager;
  }

 private:
  unordered_map<int, struct Query_Buffer *> m_BufferManager;
  WorkQueue<BlockResult> BlockResultQueue;
  thread BufferManager_Input_Thread;
  thread BufferManager_Thread;
};

/*
// const std::string currentDateTime() {
//     time_t     now = time(0);
//     struct tm  tstruct;
//     char       buf[80];
//     tstruct = *localtime(&now);
//     strftime(buf, sizeof(buf), "%Y-%m-%d %X", &tstruct);

//     return buf;
// }

// #define log(fmt, ...) \
//     printf("[%s: function:%s > line:%d] ", fmt ,"\t\t\t (%s)\n", \
//     __FILE__, __LINE__, __func__, currentDateTime());
*/
