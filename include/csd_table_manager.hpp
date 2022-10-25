#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <unordered_map>
#include <vector>

using namespace std;

#include "rocksdb/sst_file_reader.h"

class TableManager {
 public:
  TableManager() {}
  void InitCSDTableManager();  //임시로 데이터 넣어놓음
  void InitCSDTable();         //임시로 데이터 넣어놓음
  void InitTableInfo();        //임시로 데이터 넣어놓음
  vector<string> GetTableRep(string csd_name, string table_name);
  vector<string> GetSchemaList(string tableName);
  int GetSchemaLength(string tableName, string colName);
  int GetSchemaType(string tableName, string colName);

 private:
  struct KETIValue {
    int type;
    int length;
  };
  unordered_map<string, vector<string>> table_rep;
  unordered_map<string, vector<string>> schemaInfo;
  unordered_map<string, unordered_map<string, KETIValue>> tableInfo;
};