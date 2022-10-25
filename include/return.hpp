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
  vector<int> row_offset;
  int row_count;
  string data;
  int length;
  map<string, vector<std::any>> groupby_map;

  MergeResult() {}
  // merge.cc의 최초 생성자
  MergeResult(int query_id_, int work_id_, string csd_name_, string tableName_,
              string sst_name_)
      : query_id(query_id_),
        work_id(work_id_),
        tableName(tableName_),
        csd_name(csd_name_),
        sst_name(sst_name_) {
    row_offset.clear();
    row_count = 0;
    length = 0;
    data = "";
    groupby_map.clear();
  }

  void InitMergeResult() {
    row_offset.clear();
    data = "";
    row_count = 0;
    length = 0;
    groupby_map.clear();
  }
};

struct vectortype_r {
  string strvec;
  int64_t intvec;
  double floatvec;
  int type;  // 0 string, 1 int, 2 float
};
struct ReturnBuffer {
  bool justInit;
  MergeResult result;
  string table_alias;           //*결과 테이블의 별칭
  vector<string> table_column;  //*결과의 컬럼 이름
  vector<int> return_datatype;  //*결과의 컬럼 데이터 타입(돌아올때 확인)
  vector<int> table_datatype;  //저장되는 결과의 컬럼 데이터 타입(위에서 확인)
  vector<int> table_offlen;  //*결과의 컬럼 길이
  unordered_map<string, vector<vectortype_r>>
      table_data;                               //결과의 컬럼 별 데이터
  unordered_map<string, int> left_block_count;  //*남은 블록 수
  unordered_map<string, bool> is_done;          //작업 완료 여부
  bool all_sst_done;
  vector<string> sstList;
  int fileCNT;
  ReturnBuffer() {}
  ReturnBuffer(const char *jsonStr) {
    justInit = true;
    Document document;
    document.Parse(jsonStr);
    table_alias = document["tableAlias"].GetString();

    sstList.clear();

    table_column.clear();
    Value &table_column_ = document["tableCol"];
    for (int i = 0; i < table_column_.Size(); i++) {
      table_column.push_back(table_column_[i].GetString());
    }

    return_datatype.clear();
    Value &return_datatype_ = document["returnType"];
    for (int i = 0; i < return_datatype_.Size(); i++) {
      return_datatype.push_back(return_datatype_[i].GetInt());
    }

    table_offlen.clear();
    Value &table_offlen_ = document["tableOfflen"];
    for (int i = 0; i < table_offlen_.Size(); i++) {
      table_offlen.push_back(table_offlen_[i].GetInt());
    }
    all_sst_done = false;
    fileCNT = 0;
    table_datatype.clear();
    vector<string>::iterator ptr1;
    for (ptr1 = table_column.begin(); ptr1 != table_column.end(); ptr1++) {
      table_data.insert({(*ptr1), {}});
    }
    vector<int>::iterator ptr2;
    for (ptr2 = return_datatype.begin(); ptr2 != return_datatype.end();
         ptr2++) {
      if ((*ptr2) == static_cast<int>(MySQL_DataType::MySQL_BYTE)) {
        table_datatype.push_back(static_cast<int>(KETI_VALUE_TYPE::INT8));
      } else if ((*ptr2) == static_cast<int>(MySQL_DataType::MySQL_VARSTRING)) {
        table_datatype.push_back(static_cast<int>(KETI_VALUE_TYPE::STRING));
      } else {
        table_datatype.push_back((*ptr2));
      }
    }
  }
};

class Return {
 public:
  Return() {}
  Return(TableManager tManager) { this->tManager = tManager; }
  void InitBuffer();
  void ReturnResult();
  void MergeCast(MergeResult work);
  void SendDataToBufferManager();
  void push_work(MergeResult work);
  void getColOffset(const char *row_data, int *col_offset_list,
                    vector<int> return_datatype, vector<int> table_offlen);

 private:
  WorkQueue<MergeResult> ReturnQueue;
  ReturnBuffer returnBuffer;
  TableManager tManager;
};

struct message {
  long msg_type;
  char msg[2000];
};
