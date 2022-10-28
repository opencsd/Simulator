#include "TableManager.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "document.h"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

using namespace rapidjson;
using namespace std;

void TableManager::print_TableManager() {
  unordered_map<string, struct Table>::iterator it;

  for (it = m_TableManager.begin(); it != m_TableManager.end(); it++) {
    struct Table tmp = it->second;
    std::cout << "tablename : " << tmp.tablename << std::endl;
    std::cout << "Column name     Type  Length Offset" << std::endl;
    vector<struct ColumnSchema>::iterator itor = tmp.Schema.begin();

    for (; itor != tmp.Schema.end(); itor++) {
      std::cout << left << setw(15) << (*itor).column_name << " " << left
                << setw(5) << (*itor).type << " " << left << setw(6)
                << (*itor).length << " " << left << setw(5) << (*itor).offset
                << std::endl;
    }

    vector<struct SSTFile>::iterator itor2 = tmp.SSTList.begin();

    for (; itor2 != tmp.SSTList.end(); itor2++) {
      std::cout << "SST filename : " << (*itor2).filename << std::endl;
    }
  }
}

vector<string> TableManager::get_sstlist(string tablename) {
  vector<string> sstlist;
  for (int i = 0; i < m_TableManager[tablename].SSTList.size(); i++) {
    sstlist.push_back(m_TableManager[tablename].SSTList[i].filename);
  }
  return sstlist;
}

int TableManager::init_TableManager() {
  logger.info("init_TableManager");
  int json_fd;
  string json = "";
  char buf;
  const char *tmp = getenv("CSDTBLFILE");
  string filePath(tmp ? tmp : "");
  if (filePath.empty()) {
    cout << "[ERROR] No such variable found!" << endl;
    filePath = "/root/workspace/KETI-Pushdown-Process-Container/asset/";
  }
  filePath = filePath + "GeneratedTable.json";
  if ((json_fd = open(filePath.c_str(), O_RDONLY)) < 0) {
    logger.error("open error for GeneratedTable.json");
    exit(1);
  }
  int i = 0;
  int res;

  while (1) {
    if (read(json_fd, &buf, 1) > 0)
      json += buf;
    else
      break;
  }
  close(json_fd);

  // parse json
  Document document;
  document.Parse(json.c_str());

  Value &TableList = document["Table List"];
  // cout << 2 << endl;

  for (i = 0; i < TableList.Size(); i++) {
    // cout << i << endl;
    Value &TableObject = TableList[i];
    struct Table tbl;

    Value &tablenameObject = TableObject["tablename"];
    tbl.tablename = tablenameObject.GetString();

    Value &SchemaObject = TableObject["Schema"];
    int j;
    for (j = 0; j < SchemaObject.Size(); j++) {
      Value &ColumnObject = SchemaObject[j];
      struct ColumnSchema Column;

      Column.column_name = ColumnObject["column_name"].GetString();
      Column.type = ColumnObject["type"].GetInt();
      Column.length = ColumnObject["length"].GetInt();
      Column.offset = ColumnObject["offset"].GetInt();

      tbl.Schema.push_back(Column);
    }

    Value &SSTList = TableObject["SST List"];
    for (j = 0; j < SSTList.Size(); j++) {
      Value &SSTObject = SSTList[j];
      struct SSTFile SSTFile;

      SSTFile.filename = SSTObject["filename"].GetString();
      SSTFile.rowCount = SSTObject["rowCount"].GetInt();

      tbl.SSTList.push_back(SSTFile);
    }
    m_TableManager.insert({tbl.tablename, tbl});
  }

  return 0;
}

int TableManager::get_table_schema(std::string tablename,
                                   vector<struct ColumnSchema> &dst) {
  if (m_TableManager.find(tablename) == m_TableManager.end()) {
    std::cout << "Not Present" << std::endl;
    return -1;
  }

  struct Table tbl = m_TableManager[tablename];
  dst = tbl.Schema;
  return 0;
}
