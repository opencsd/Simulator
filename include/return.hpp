#pragma once
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>

#include <any>
#include <bitset>
#include <cstdlib>
#include <sstream>
#include <tuple>

#include "csd_table_manager.hpp"
#include "input.hpp"

#define BUFF_M_PORT 8888
#define BUFF_M_IP "10.0.5.123"
struct MergeResult {
  int query_id;
  int work_id;
  string csd_name;
  string tableName;
  string sst_name;
  int row_count;
  vector<string> colNames;
  unordered_map<string, vector<string>> data;

  MergeResult() {}
  // merge.cc의 최초 생성자
  MergeResult(int query_id_, int work_id_, string csd_name_, string tableName_,
              string sst_name_, unordered_map<string, vector<string>> data_,
              vector<string> colNames_)
      : query_id(query_id_),
        work_id(work_id_),
        tableName(tableName_),
        csd_name(csd_name_),
        sst_name(sst_name_),
        data(data_),
        colNames(colNames_) {
    row_count = 0;
  }

  void InitMergeResult() { row_count = 0; }
};

struct vectortype_r {
  string strvec;
  int64_t intvec;
  double floatvec;
  int type;  // 0 string, 1 int, 2 float
};

class Return {
 public:
  Return() {}
  Return(TableManager tManager) { this->tManager = tManager; }
  void InitBuffer();
  void ReturnResult();
  void MergeCast(MergeResult work);
  void SendDataToBufferManager(MergeResult mergeResult);
  void push_work(MergeResult work);
  void getColOffset(const char *row_data, int *col_offset_list,
                    vector<int> return_datatype, vector<int> table_offlen);

 private:
  WorkQueue<MergeResult> ReturnQueue;
  TableManager tManager;
};

struct message {
  long msg_type;
  char msg[2000];
};
