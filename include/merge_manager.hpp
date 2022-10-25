#pragma once
#include <limits.h>

#include <bitset>
#include <stack>
#include <string>

#include "return.hpp"

struct Result;

typedef std::pair<int, int> pair_key;

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};

struct T {
  string varString;
  int varInt;
  int64_t varLong;
  float varFloat;
  double varDouble;
};

class MergeManager {
 public:
  MergeManager(Return *returnManager) { this->returnManager = returnManager; }
  void Merging();
  void MergeBlock(Result &result);
  void SendDataToBufferManager(MergeResult &mergedBlock);
  void push_work(Result result);

 private:
  unordered_map<pair_key, MergeResult, pair_hash>
      m_MergeManager;  // key=<qid,wid>
  Return *returnManager;
  WorkQueue<Result> MergeQueue;
};

struct FilterInfo {
  vector<string> table_col;  //스캔테이블
  vector<int> table_offset;
  vector<int> table_offlen;
  vector<int> table_datatype;
  unordered_map<string, int> colindexmap;  // col index
  // vector<string> filtered_col;//컬럼필터테이블
  // vector<int> filtered_datatype;
  // unordered_map<string, int> filtered_colindexmap;//filter후 col index
  string table_filter;                   // where절 정보
  vector<Projection> column_projection;  // select절 정보
  vector<int> projection_datatype;  //*컬럼 프로젝션 후 컬럼의 데이터타입
  vector<string> groupby_col;  // goup by절 정보
  bool need_col_filtering;

  FilterInfo() {}
  FilterInfo(vector<string> table_col_, vector<int> table_offset_,
             vector<int> table_offlen_, vector<int> table_datatype_,
             unordered_map<string, int> colindexmap_, /*vector<string>
             filtered_col_, vector<int> filtered_datatype_,*/
             string table_filter_, vector<Projection> column_projection_,
             vector<int> projection_datatype_, vector<string> groupby_col_)
      : table_col(table_col_),
        table_offset(table_offset_),
        table_offlen(table_offlen_),
        table_datatype(table_datatype_),
        colindexmap(colindexmap_), /*
         filtered_col(filtered_col_),
         filtered_datatype(filtered_datatype_),*/
        table_filter(table_filter_),
        column_projection(column_projection_),
        projection_datatype(projection_datatype_),
        groupby_col(groupby_col_) {}
};
/*
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
*/

struct typeDate {
  int year;
  int month;
  int day;
  typeDate(int year_, int month_, int day_)
      : year(year_), month(month_), day(day_) {}
};
struct typeVar {
  int type;
  int intVar;
  string strVar;
  typeDate dataVar;
  double doubleVar;
  typeVar(int type_, auto data) : type(type_) {
    switch (type_) {
      case 1:  // MySQL_BYTE
        intVar = data;
        break;
      case 2:  // MySQL_INT16
        intVar = data;
        break;
      case 3:  // MySQL_INT32
        intVar = data;
        break;
      case 8:  // MySQL_INT64
        intVar = data;
        break;
      case 4:  // MySQL_FLOAT32
        doubleVar = data;
        break;
      case 5:  // MySQL_DOUBLE
        doubleVar = data;
        break;
      case 246:  // MySQL_NEWDECIMAL
        doubleVar = data;

        break;
      case 14:  // MySQL_DATE
        /* code */
        break;
      case 7:  // MySQL_TIMESTAMP
        /* code */
        break;
      case 254:  // MySQL_STRING
        /* code */
        break;
      case 15:  // MySQL_VARSTRING
        /* code */
        break;
    }
  }
};

struct Result {
  int query_id;
  int work_id;
  string csd_name;
  string table_name;
  string sst_name;
  int length;
  FilterInfo filter_info;
  int row_count;
  unordered_map<string, string> string data;
  vector<int> row_offset;

  // scan, filter의 최초 생성자
  Result(int query_id_, int work_id_, string csd_name_, string table_name_,
         string sst_name_, FilterInfo filter_info_)
      : query_id(query_id_),
        work_id(work_id_),
        csd_name(csd_name_),
        table_name(table_name_),
        sst_name(sst_name_),
        filter_info(filter_info_) {
    row_count = 0;
    length = 0;
    data = "";
    row_offset.clear();
  }

  void InitResult() {
    row_count = 0;
    length = 0;
    data = "";
    row_offset.clear();
  }
};
