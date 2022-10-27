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
  string table_filter;
  vector<string> table_col;  //스캔테이블
  vector<char> postFix;
  vector<char> logicalOperators;
  vector<char> filterResults;
  vector<Projection> column_projection;  // select절 정보
  vector<int> projection_datatype;  //*컬럼 프로젝션 후 컬럼의 데이터타입
  vector<string> groupby_col;  // goup by절 정보
  bool need_col_filtering;

  FilterInfo() {}
  FilterInfo(vector<string> table_col_, vector<Projection> column_projection_,
             vector<int> projection_datatype_, vector<string> groupby_col_,
             string table_filter_)
      : table_col(table_col_),
        column_projection(column_projection_),
        projection_datatype(projection_datatype_),
        groupby_col(groupby_col_),
        table_filter(table_filter_) {}
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
  typeDate() {}
  typeDate(int year_, int month_, int day_)
      : year(year_), month(month_), day(day_) {}
  string strconv() {
    string yearstr = to_string(year);
    string monthstr = to_string(month);
    if (monthstr.size() <= 1) {
      monthstr = "0" + monthstr;
    }
    string daystr = to_string(day);
    if (daystr.size() <= 1) {
      daystr = "0" + daystr;
    }
    return yearstr + "-" + monthstr + "-" + daystr;
  }
  string toRawData() { return to_string((year * 512) + (month * 32) + day); }
};
struct typeDecimal {
  int integerPart;
  int floatingPart;
  typeDecimal() {}
  typeDecimal(int integerPart_, int floatingPart_)
      : integerPart(integerPart_), floatingPart(floatingPart_) {}
  string printDecimal() {
    string integerstr = to_string(integerPart);
    string floatingstr = to_string(floatingPart);
    if (floatingstr.size() <= 1) {
      floatingstr = floatingstr + "0";
    }
    return integerstr + "." + floatingstr;
  }
};
struct typeVar {
  int type;
  int intVar;
  __int64_t int64Var;
  string strVar;
  typeDate dateVar;
  typeDecimal doubleVar;
  typeVar(int type_, const char *data, int len) : type(type_) {
    switch (type_) {
      case 1: {  // MySQL_BYTE
        intVar = (uint8_t)data[0];
        break;
      }
      case 2: {  // MySQL_INT16
        intVar = static_cast<int>(((uint8_t)data[0]) | ((uint8_t)data[1] << 8));
        break;
      }
      case 3: {  // MySQL_INT32
        intVar = static_cast<int>(((uint8_t)data[0]) | ((uint8_t)data[1] << 8) |
                                  ((uint8_t)data[2] << 16) |
                                  ((uint8_t)data[3] << 24));
        break;
      }
      case 8: {  // MySQL_INT64
        int64Var = static_cast<__int64_t>(
            ((uint8_t)data[0]) | ((uint8_t)data[1] << 8) |
            ((uint8_t)data[2] << 16) | ((uint8_t)data[3] << 24) |
            ((uint8_t)data[4] << 32) | ((uint8_t)data[5] << 40) |
            ((uint8_t)data[6] << 48) | ((uint8_t)data[7] << 56));
        break;
      }
      case 4:  // MySQL_FLOAT32
        // doubleVar = data;
        break;
      case 5:  // MySQL_DOUBLE
        // doubleVar = data;
        break;
      case 246: {  // MySQL_NEWDECIMAL
        __int64_t tmpIntegerData = static_cast<__int64_t>(
            (((uint8_t)data[4]) | ((uint8_t)data[3] << 8) |
             ((uint8_t)data[2] << 16) | ((uint8_t)data[1] << 24) |
             ((uint8_t)data[0] << 32)));
        int tmpFloatData = static_cast<int>(((uint8_t)data[5]));
        typeDecimal tmpdcm(tmpIntegerData, tmpFloatData);
        doubleVar = tmpdcm;

        break;
      }
      case 14: {  // MySQL_DATE
        dateVar.day = static_cast<int>(((uint8_t)data[0])) % 32;
        dateVar.month = static_cast<int>(((uint8_t)data[0])) / 32;
        dateVar.year =
            static_cast<int>(((uint8_t)data[1]) | ((uint8_t)data[2] << 8)) / 2;

        /* code */
        break;
      }
      case 7:  // MySQL_TIMESTAMP
        /* code */
        break;
      case 254: {
        // char charVal[len + 1];
        // memcpy(charVal, data, len);
        // charVal[len] = '\0';
        // strVar = charVal;
        strVar = string(data, len);

        strVar = trim(strVar);
        break;  // MySQL_STRING
      }
      case 15: {  // MySQL_VARSTRING

        if (len > 255) {
          len = static_cast<int>(((uint8_t)data[0]) | ((uint8_t)data[1] << 8));
          // char charVal[len + 1];
          // memcpy(charVal, data + 2, len);
          // charVal[len] = '\0';
          // strVar = charVal;
          strVar = string(data + 2, len);
          strVar = trim(strVar);
        } else {
          len = static_cast<int>(((uint8_t)data[0]));
          // char charVal[len + 1];
          // memcpy(charVal, data + 1, len);
          // charVal[len] = '\0';
          // strVar = charVal;
          strVar = string(data + 1, len);
          strVar = trim(strVar);
        }

        break;
      }
    }
  }
  string getIntVal() { return to_string(intVar); }
  string getInt64Val() { return to_string(int64Var); }
  string getStrVal() { return strVar; }
  string getDateVal() { return dateVar.strconv(); }
  string getDoubleVal() { return doubleVar.printDecimal(); }
};

struct Result {
  int query_id;
  int work_id;
  string csd_name;
  string table_name;
  string sst_name;
  FilterInfo filter_info;
  int row_count;
  unordered_map<string, vector<typeVar>> data;

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
  }

  void InitResult() { row_count = 0; }
};
