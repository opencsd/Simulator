
#pragma once
#include <unordered_map>
#include <vector>

#include "logger.hpp"
// #include "testmodule.h"

#define SE_TBL_GEN "GeneratedTable.json"
#define SE_TBL_ENV "SETBLPATH"

using namespace std;

class TableManager {
  // TableManager()
 public:
  struct ColumnSchema {
    std::string column_name;
    int type;
    int length;
    int offset;
  };

  struct SSTFile {
    std::string filename;
    int rowCount;
  };

  struct Table {
    std::string tablename;
    int tablesize;
    vector<struct ColumnSchema> Schema;
    vector<struct SSTFile> SSTList;
  };

  void init_TableManager();
  void print_TableManager();
  int get_table_schema(std::string tablename, vector<struct ColumnSchema> &dst);
  vector<string> get_sstlist(string tablename);

 private:
  unordered_map<std::string, struct Table> m_TableManager;
  Logger logger;
};