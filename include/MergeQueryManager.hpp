#pragma once
#include <algorithm>
#include <any>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <ctime>
#include <iostream>
#include <stack>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <vector>

#include "buffer_manager.hpp"
#include "document.h"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

using namespace std;
using namespace rapidjson;

namespace mergequery {
const int BufferSize = 40960;
}

struct SnippetStruct {
  int snippetType;
  int query_id;
  int work_id;
  string sstfilename;
  // Value block_info_list;
  vector<string> table_col;
  // Value table_filter;
  vector<filterstruct> table_filter;
  vector<int> table_offset;
  vector<int> table_offlen;
  vector<int> table_datatype;
  vector<string> column_alias;
  int tableblocknum;
  int tablerownum;
  vector<string> tablename;
  string tableAlias;
  vector<vector<Projection>> columnProjection;
  vector<string> columnFiltering;
  vector<string> groupBy;
  // Value& table_Having;
  vector<string> orderBy;
  vector<int> orderType;
  vector<int> return_datatype;
  vector<int> return_offlen;
  vector<vector<Projection>> dependencyProjection;
  vector<filterstruct> dependencyFilter;
  bool IsJoin;
  // unordered_map<string,VectorType> tabledata;
  // unordered_map<string,VectorType> resultdata;
  // unordered_map<string, StackType> resultstack;
  char data[mergequery::BufferSize];  // wal manager data
  string wal_json;

  // SnippetStruct(int work_id_, string sstfilename_,
  //     Value& block_info_list_,
  //     vector<string> table_col_, Value& table_filter_,
  //     vector<int> table_offset_, vector<int> table_offlen_,
  //     vector<int> table_datatype_, Value& table_Having_)
  //     : work_id(work_id_), sstfilename(sstfilename_),
  //     block_info_list(block_info_list_),
  //     table_col(table_col_),
  //     table_filter(table_filter_),
  //     table_offset(table_offset_),
  //     table_offlen(table_offlen_),
  //     table_datatype(table_datatype_), table_Having(table_Having_) {};

  // SnippetStruct(Value& tablefiler, Value& blockinfo)
  // : table_filter(tablefiler), block_info_list(blockinfo){};

  // SnippetStruct(Value& blockinfo)
  // : block_info_list(blockinfo){};

  // SnippetStruct();

  // SnippetStruct(const SnippetStruct&);
  // SnippetStruct& operator=(SnippetStruct&);
  // SnippetStruct& operator=(const SnippetStruct& input){
  //     // cout << 1 << endl;
  //     // Document d;
  //     // Document::AllocatorType& a = d.GetAllocator();
  //     // cout << 2 << endl;
  //     this->query_id = input.query_id;
  //     this->work_id = input.work_id;
  //     this->sstfilename = input.sstfilename;
  //     // this->block_info_list = input.block_info_list;
  //     // this->block_info_list.CopyFrom(input.block_info_list,a);
  //     this->table_col = input.table_col;
  //     this->table_filter = input.table_filter;
  //     // this->table_filter.CopyFrom(input.table_filter,a);
  //     // this->table_filter(input.table_filter, a);
  //     this->table_offset = input.table_offset;
  //     this->table_offlen = input.table_offlen;
  //     this->table_datatype = input.table_datatype;
  //     this->column_alias = input.column_alias;
  //     this->tableblocknum = input.tableblocknum;
  //     this->tablerownum = input.tablerownum;
  //     this->tablename = input.tablename;
  //     this->tableAlias = input.tableAlias;
  //     this->columnProjection = input.columnProjection;
  //     this->columnFiltering = input.columnFiltering;
  //     this->groupBy = input.groupBy;
  //     // this->table_Having = input.table_Having;
  //     this->orderBy = input.orderBy;
  //     this->return_datatype = input.return_datatype;
  //     this->return_offlen = input.return_offlen;
  //     this->IsJoin = input.IsJoin;
  //     this->data[BufferSize] = *input.data;
  //     return *this;
  // };
};

struct groupby {
  int count;
  int rowcount;
  vector<vectortype> value;
  vector<vectortype> savedkey;
};

struct VectorType {
  vector<int> intvec;
  vector<string> stringvec;
  vector<float> floatvec;
  int type;
};

struct StackType {
  stack<int> intstack;
  stack<float> floatstack;
  // type_info type;
  int type;
};

class FilterClass {
 public:
  void FilterThreadRun(int start, int finish,
                       unordered_map<string, vector<vectortype>> tablelist,
                       SnippetStruct &snippet,
                       vector<vector<string>> tablenamelist);
  void insertTable(string tablename, vectortype data) {
    tablemutex.lock();
    savedmap_[tablename].push_back(data);
    cout << savedmap_[tablename].size() << endl;
    tablemutex.unlock();
  };
  unordered_map<string, vector<vectortype>> returntable() { return savedmap_; };

 private:
  unordered_map<string, vector<vectortype>> savedmap_;
  mutex tablemutex;
};

class joinclass {
 public:
  void JoinThreadRun(int start, int finish,
                     unordered_map<string, vector<vectortype>> tablelist,
                     SnippetStruct &snippet,
                     vector<vector<string>> tablenamelist);
  void insertTable(string tablename, vectortype data) {
    tablemutex.lock();
    savedmap_[tablename].push_back(data);
    // cout << savedmap_[tablename].size() << endl;
    tablemutex.unlock();
  };
  unordered_map<string, vector<vectortype>> returntable() {
    tablemutex.lock();
    unordered_map<string, vector<vectortype>> savedmap = savedmap_;
    savedmap_.clear();
    tablemutex.unlock();
    return savedmap;
  };

 private:
  unordered_map<string, vector<vectortype>> savedmap_;
  mutex tablemutex;
};

class sortclass {
 public:
  unordered_map<string, vectortype> value;
  int ordercount;
  vector<string> ordername;
  vector<int> ordertype;

  bool operator<(sortclass &sortbuf) {
    for (int i = 0; i < ordercount; i++) {
      if (this->value[ordername[i]].type == 1) {
        if (this->value[ordername[i]].intvec ==
            sortbuf.value[ordername[i]].intvec) {
          continue;
        } else {
          // if() asc desc 구분 필요
          if (ordertype[i] == 1) {
            return this->value[ordername[i]].intvec >
                   sortbuf.value[ordername[i]].intvec;
          } else {
            return this->value[ordername[i]].intvec <
                   sortbuf.value[ordername[i]].intvec;
          }
        }
      } else if (this->value[ordername[i]].type == 2) {
        if (this->value[ordername[i]].floatvec ==
            sortbuf.value[ordername[i]].floatvec) {
          continue;
        } else {
          // if() asc desc 구분 필요
          if (ordertype[i] == 1) {
            return this->value[ordername[i]].floatvec >
                   sortbuf.value[ordername[i]].floatvec;
          } else {
            return this->value[ordername[i]].floatvec <
                   sortbuf.value[ordername[i]].floatvec;
          }
        }
      } else {
        if (this->value[ordername[i]].strvec ==
            sortbuf.value[ordername[i]].strvec) {
          continue;
        } else {
          // if() asc desc 구분 필요
          if (ordertype[i] == 1) {
            return this->value[ordername[i]].strvec >
                   sortbuf.value[ordername[i]].strvec;
          } else {
            return this->value[ordername[i]].strvec <
                   sortbuf.value[ordername[i]].strvec;
          }
        }
      }
    }
    if (this->value[ordername[0]].type == 1) {
      if (ordertype[0] == 1) {
        return this->value[ordername[0]].intvec >
               sortbuf.value[ordername[0]].intvec;
      } else {
        return this->value[ordername[0]].intvec <
               sortbuf.value[ordername[0]].intvec;
      }
    } else if (this->value[ordername[0]].type == 2) {
      if (ordertype[0] == 1) {
        return this->value[ordername[0]].floatvec >
               sortbuf.value[ordername[0]].floatvec;
      } else {
        return this->value[ordername[0]].floatvec <
               sortbuf.value[ordername[0]].floatvec;
      }
    } else {
      if (ordertype[0] == 1) {
        return this->value[ordername[0]].strvec >
               sortbuf.value[ordername[0]].strvec;
      } else {
        return this->value[ordername[0]].strvec <
               sortbuf.value[ordername[0]].strvec;
      }
    }
  }
};

void LOJoin(SnippetStruct &snippet, BufferManager &buff);

bool compare(sortclass &sortbuffer);

void Init(Value snippet);

unordered_map<string, vector<vectortype>> GetBufMTable(string tablename,
                                                       SnippetStruct &snippet,
                                                       BufferManager &buff);

void GetAccessData();

void Join();

void DependencyExist(SnippetStruct &snippet, BufferManager &buff);

void DependencyNotExist(SnippetStruct &snippet, BufferManager &buff);

void DependencyOPER(SnippetStruct &snippet, BufferManager &buff);

void DependencyIN(SnippetStruct &snippet, BufferManager &buff);

void Storage_Filter(SnippetStruct &snippet, BufferManager &buff);

void GetColOff();

void ColumnProjection(SnippetStruct &snippet);

void SaveTable();

void makeTable(SnippetStruct &snippet);

void JoinTable(SnippetStruct &snippet, BufferManager &buff);

void JoinThread(SnippetStruct &snippet, BufferManager &buff);

void Storage_Filter_Thread(SnippetStruct &snippet, BufferManager &buff);

bool IsJoin(SnippetStruct &snippet);

void Aggregation(SnippetStruct &snippet, BufferManager &buff, bool tablecount);

vector<vectortype> Postfix(
    unordered_map<string, vector<vectortype>> tablelist,
    vector<Projection> data,
    unordered_map<string, vector<vectortype>> savedTable);

void InnerJoin(SnippetStruct &snippet, BufferManager &buff);

void NaturalJoin(SnippetStruct &snippet, BufferManager &buff);

void OuterFullJoin(SnippetStruct &snippet, BufferManager &buff);

void OuterRightJoin(SnippetStruct &snippet, BufferManager &buff);

void CrossJoin(SnippetStruct &snippet, BufferManager &buff);

void SelfJoin(SnippetStruct &snippet, BufferManager &buff);

void GroupBy(SnippetStruct &snippet, BufferManager &buff);

void OrderBy(SnippetStruct &snippet, BufferManager &buff);

void GetWALManager(char *data, string TableName);

void DependencyLoopQuery();

void Having(SnippetStruct snippet, BufferManager &buff);

void Filtering(SnippetStruct snippet);

vectortype GetLV(lv lvdata, int index,
                 unordered_map<string, vector<vectortype>> tablelist);

vectortype GetRV(rv rvdata, int index,
                 unordered_map<string, vector<vectortype>> tablelist);

vectortype GetFilterValue(filtervalue filterdata, int index,
                          unordered_map<string, vector<vectortype>> tablelist);

bool LikeSubString_v2(string lv, string rv);

vector<string> split(string str, char Delimiter);

void get_data(int snippetnumber, BufferManager &bufma, string tablealias);

void get_data_and_filter(SnippetStruct snippet, BufferManager &bufma);
