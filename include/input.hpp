#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <condition_variable>
#include <iostream>
#include <list>
#include <mutex>
#include <queue>
#include <unordered_map>

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rocksdb/sst_file_reader.h"

using namespace rapidjson;
using namespace std;
using namespace ROCKSDB_NAMESPACE;

#define NUM_OF_BLOCKS 15
#define BUFF_SIZE (NUM_OF_BLOCKS * 4096)
#define TEST_SIZE 54272  // 60K에서 key 빠지는거 고려해서 53K 기준 // 임시!!
#define INPUT_IF_PORT 18080
#define PACKET_SIZE 36

struct Snippet;
template <typename T>
class WorkQueue;

typedef enum MySQL_DataType {
  MySQL_BYTE = 1,
  MySQL_INT16 = 2,
  MySQL_INT32 = 3,
  MySQL_INT64 = 8,
  MySQL_FLOAT32 = 4,
  MySQL_DOUBLE = 5,
  MySQL_NEWDECIMAL = 246,
  MySQL_DATE = 14,
  MySQL_TIMESTAMP = 7,
  MySQL_STRING = 254,
  MySQL_VARSTRING = 15,
} MySQL_DataType;

typedef enum KETI_SELECT_TYPE {
  COL_NAME = 0,  // DEFAULT
  SUM = 1,
  AVG = 2,
  COUNT = 3,
  COUNTSTAR = 4,
  TOP = 5,
  MIN = 6,
  MAX = 7,
} KETI_SELECT_TYPE;

typedef enum KETI_VALUE_TYPE {
  INT8 = 0,
  INT16 = 1,
  INT32 = 2,
  INT64 = 3,
  FLOAT32 = 4,
  FLOAT64 = 5,
  NUMERIC = 6,
  DATE = 7,
  TIMESTAMP = 8,
  STRING = 9,
  COLUMN = 10,
  OPERATOR = 11,
} KETI_VALUE_TYPE;

typedef enum STACK_TYPE {
  INT_ = 50,
  Long_ = 51,
  FLOAT_ = 52,
  DOUBLE_ = 53,
  STRING_ = 54,
  DECIMAL_ = 55,
} STACK_TYPE;

struct Projection {
  int projection;  // KETI_SELECT_TYPE
  int aliasType;
  vector<string> values;
  vector<int> types;  // KETI_ValueType
};

/*
                            Scan Type
  +--------------------+-----------------------------------------+
  | Full_Scan_Filter   | full table scan / only scan (no filter) |
  | Full_Scan          | full table scan / scan and filter       |
  | Index_Scan_Filter  | index pk scan   / only scan (no filter) |
  | Index_Scan         | index pk scan   / scan and filter       |
  +--------------------+-----------------------------------------+
*/

enum Scan_Type { Full_Scan_Filter, Full_Scan, Index_Scan_Filter, Index_Scan };

struct PrimaryKey {
  string key_name;
  int key_type;
  int key_length;
};

struct FilterValue {
  vector<bool> isColumn;
  vector<string> value;
};

struct TableFilter {
  FilterValue LeftValue;
  int FilterOperator;
  FilterValue RightValue;
};

struct Snippet {
  int work_id;         //*워크ID
  int query_id;        //*쿼리ID
  string csd_name;     //*csd이름
  string sstfilename;  //*sstFile Name;
  string table_name;   //*스캔 테이블 이름
  string table_hex_key;
  vector<string> table_col;                //*스캔 테이블 컬럼명
  vector<int> table_offset;                //*스캔 테이블 컬럼오프셋
  vector<int> table_offlen;                //*스캔 테이블 컬럼길이
  vector<int> table_datatype;              //*스캔 테이블 컬럼타입
  list<BlockInfo> block_info_list;         //*스캔 블록 리스트
  int total_block_count;                   //전체 블록 수
  unordered_map<string, int> colindexmap;  //컬럼의 순서
  list<PrimaryKey> primary_key_list;       //테이블 pk 정보 *primary_count
  // char index_num[4];//*테이블 인덱스 넘버 >> 보내주기
  // string index_pk;//*인덱스 스캔시 필요한 pk >> 나중에 고려
  uint64_t kNumInternalBytes;  // key에 붙는 디폴트값 길이
  int primary_length;          //총 pk 길이
  // vector<string> filtered_col;//*컬럼 필터링후 남는 컬럼명
  // vector<int> filtered_datatype;//컬럼 필터링후 남는 컬럼타입(계산)
  // unordered_map<string, int> filtered_colindexmap;//컬럼 필터링후 컬럼의 순서

  vector<TableFilter> table_filter;  //*where절 정보
  vector<string> columnAlias;
  vector<Projection> column_projection;  //*select문 정보
  vector<int> projection_datatype;  //*컬럼 프로젝션 후 컬럼의 데이터타입
  vector<string> groupby_col;  //*group by 정보
  int scan_type;               //스캔 타입 결정
  vector<string> deleted_key;
  vector<string> inserted_key;
  vector<string> inserted_value;
  bool is_deleted;
  bool is_inserted;

  // const char* file_path;//TEST

  Snippet(const char *json_) {
    bool index_scan = false;
    bool only_scan = true;

    Document document;
    document.Parse(json_);

    query_id = document["queryID"].GetInt();
    work_id = document["workID"].GetInt();
    sstfilename = document["fileName"].GetString();
    csd_name = document["csdName"].GetString();
    table_name = document["tableName"].GetString();
    if (table_name == "customer") {
      table_hex_key = "00000109";
    } else if (table_name == "orders") {
      table_hex_key = "0000010A";
    } else if (table_name == "partsupp") {
      table_hex_key = "00000108";
    } else if (table_name == "supplier") {
      table_hex_key = "00000107";
    } else if (table_name == "lineitem") {
      table_hex_key = "0000010B";
    } else if (table_name == "nation") {
      table_hex_key = "00000104";
    } else if (table_name == "part") {
      table_hex_key = "00000106";
    } else if (table_name == "region") {
      table_hex_key = "00000105";
    }

    // int index_num_ = document["indexNum"].GetInt();
    // union{
    //   int value;
    //   char byte[4];
    // }inum;
    // inum.value = index_num_;
    // memcpy(index_num,inum.byte,4);

    groupby_col.clear();
    Value &groupby_col_ = document["groupBy"];
    for (int i = 0; i < groupby_col_.Size(); i++) {
      groupby_col.push_back(groupby_col_[i].GetString());
    }

    int primary_count = document["primaryKey"].GetInt();
    if (primary_count == 0) {
      kNumInternalBytes = 8;
    } else {
      kNumInternalBytes = 0;
    }

    uint64_t block_offset_, block_length_;
    int block_id_ = 0;  //필요없을듯

    // block list 정보 저장
    block_info_list.clear();
    total_block_count = 0;
    Value &block_list = document["blockList"];
    for (int i = 0; i < block_list.Size(); i++) {
      block_offset_ = block_list[i]["offset"].GetInt64();
      for (int j = 0; j < block_list[i]["length"].Size(); j++) {
        block_length_ = block_list[i]["length"][j].GetInt64();
        BlockInfo newBlock(block_id_, block_offset_, block_length_);
        block_info_list.push_back(newBlock);
        block_offset_ = block_offset_ + block_length_;
        block_id_++;
        total_block_count++;
      }
    }

    //테이블 스키마 정보 저장
    table_col.clear();
    table_offset.clear();
    table_offlen.clear();
    table_datatype.clear();
    colindexmap.clear();
    Value &table_col_ = document["tableCol"];
    for (int i = 0; i < table_col_.Size(); i++) {
      string col = table_col_[i].GetString();
      int startoff = document["tableOffset"][i].GetInt();
      int offlen = document["tableOfflen"][i].GetInt();
      int datatype = document["tableDatatype"][i].GetInt();
      table_col.push_back(col);
      table_offset.push_back(startoff);
      table_offlen.push_back(offlen);
      table_datatype.push_back(datatype);
      colindexmap.insert({col, i});

      // pk 정보 저장
      primary_key_list.clear();
      primary_length = 0;
      if (i < primary_count) {
        string key_name_ = document["tableCol"][i].GetString();
        int key_type_ = document["tableDatatype"][i].GetInt();
        int key_length_ = document["tableOfflen"][i].GetInt();
        primary_key_list.push_back(
            PrimaryKey{key_name_, key_type_, key_length_});
        primary_length += key_length_;
      }
    }
    Value &columnAlias_ = document["columnAlias"];
    for (int i = 0; i < columnAlias_.Size(); i++) {
      columnAlias.emplace_back(columnAlias_[i].GetString());
    }

    //컬럼 프로젝션 정보 저장
    column_projection.clear();
    projection_datatype.clear();
    Value &column_projection_ = document["columnProjection"];
    Value &projection_datatype_ = document["projectionDatatype"];
    for (int i = 0; i < column_projection_.Size(); i++) {
      int ptype = column_projection_[i]["selectType"].GetInt();
      vector<string> vlist;
      vlist.clear();
      vector<int> vtypes;
      vtypes.clear();
      int aliasType = 0;
      Value &v = column_projection_[i]["value"];
      Value &vt = column_projection_[i]["valueType"];
      for (int j = 0; j < v.Size(); j++) {
        vlist.push_back(v[j].GetString());
        int tmpType = vt[j].GetInt();
        switch (tmpType) {
          case 0: {
            vtypes.push_back(tmpType);
            aliasType = 0;
            break;
          }
          case 1: {
            vtypes.push_back(tmpType);
            aliasType = 0;
            break;
          }
          case 2: {
            vtypes.push_back(tmpType);
            aliasType = 0;
            break;
          }
          case 3: {
            vtypes.push_back(tmpType);
            aliasType = 1;
            break;
          }
          case 4: {
            vtypes.push_back(tmpType);
            aliasType = 4;
            break;
          }
          case 5: {
            vtypes.push_back(tmpType);
            aliasType = 4;
            break;
          }
          case 6: {
            vtypes.push_back(tmpType);
            aliasType = 4;
            break;
          }
          case 7: {
            vtypes.push_back(tmpType);
            aliasType = 7;
            break;
          }
          case 8: {
            vtypes.push_back(tmpType);
            aliasType = 0;
            break;
          }
          case 9: {
            vtypes.push_back(tmpType);
            aliasType = 2;
            break;
          }
        }
      }
      column_projection.push_back(Projection{ptype, aliasType, vlist, vtypes});
      projection_datatype.push_back(projection_datatype_[i].GetInt());
    }

    // filter 작업인 경우 확인
    Value &table_filter_ = document["tableFilter"];
    for (int i = 0; i < table_filter_.Size(); i++) {
      FilterValue lv;
      FilterValue rv;
      TableFilter tmpfilter;
      for (int j = 0; j < table_filter_[i]["LV"]["type"].Size(); i++) {
        if (table_filter_[i]["LV"]["type"][j] == COLUMN) {
          lv.isColumn.emplace_back(true);
        } else {
          lv.isColumn.emplace_back(false);
        }
        lv.value.emplace_back(table_filter_[i]["LV"]["value"][j].GetString());
      }
      for (int j = 0; j < table_filter_[i]["RV"]["type"].Size(); i++) {
        if (table_filter_[i]["RV"]["type"][j] == COLUMN) {
          rv.isColumn.emplace_back(true);
        } else {
          rv.isColumn.emplace_back(false);
        }
        rv.value.emplace_back(table_filter_[i]["RV"]["value"][j].GetString());
      }
      tmpfilter.FilterOperator = table_filter_[i]["Operator"].GetInt();
      tmpfilter.LeftValue = lv;
      tmpfilter.RightValue = rv;
      table_filter.emplace_back(tmpfilter);
    }

    //스캔 타입 결정
    if (!index_scan && !only_scan) {
      scan_type = Full_Scan_Filter;
    } else if (!index_scan && only_scan) {
      scan_type = Full_Scan;
    } else if (index_scan && !only_scan) {
      scan_type = Index_Scan_Filter;
    } else {
      scan_type = Index_Scan;
    }
  }
};

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

  int length() {
    int ret;
    unique_lock<mutex> lock(work_mutex);

    ret = work.size();

    lock.unlock();
    return ret;
  }
};
