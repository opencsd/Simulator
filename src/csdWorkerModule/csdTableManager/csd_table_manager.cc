#include "csd_table_manager.hpp"

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

using namespace std;
using namespace rapidjson;
void TableManager::InitCSDTable() {
  int json_fd;
  string json = "";
  char buf;
  const char* tmp = getenv("CSDTBLFILE");
  string filePath(tmp ? tmp : "");
  if (filePath.empty()) {
    cout << "[ERROR] No such variable found!" << endl;
    filePath = "/root/workspace/KETI-Pushdown-Process-Container/asset/";
  }
  string tblFilePath = filePath + "GeneratedCSDTableManager.json";
  cout << "File path Set done => " << tblFilePath << endl;
  if ((json_fd = open(tblFilePath.c_str(), O_RDONLY)) < 0) {
    fprintf(stderr, "open error for %s\n", "GeneratedCSDTableManager.json");
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
  cout << "File Read done! Start Parsing" << endl;
  Document document;
  document.Parse(json.c_str());

  auto tableRawData = document.GetArray();

  for (auto it = tableRawData.Begin(); it != tableRawData.End(); it++) {
    Value& tblinfo = *it;
    string tmpCSDNum = tblinfo["csdNum"].GetString();
    string tmpTableName = tblinfo["tableName"].GetString();
    string tmpFilePath = tblinfo["filePath"].GetString();
    cout << "{{" << tmpCSDNum << ", " << tmpTableName << "}, " << tmpFilePath
         << "} insert Map" << endl;
    string repKey = tmpCSDNum + "/" + tmpTableName;
    table_rep[repKey].push_back(tmpFilePath);
  }
}
void TableManager::InitTableInfo() {
  int infojson_fd;
  string infojson = "";
  char infobuf;
  const char* tmp = getenv("CSDTBLFILE");
  string filePath(tmp ? tmp : "");
  if (filePath.empty()) {
    cout << "[ERROR] No such variable found!" << endl;
    filePath = "/root/workspace/KETI-Pushdown-Process-Container/asset/";
  }
  string infoFilePath = filePath + "GeneratedTable.json";
  cout << "File path Set done => " << infoFilePath << endl;
  if ((infojson_fd = open(infoFilePath.c_str(), O_RDONLY)) < 0) {
    fprintf(stderr, "open error for %s\n", "GeneratedTable.json");
    exit(1);
  }
  int i = 0;
  int res;

  while (1) {
    if (read(infojson_fd, &infobuf, 1) > 0)
      infojson += infobuf;
    else
      break;
  }
  close(infojson_fd);
  cout << "File Read done! Start Parsing" << endl;
  Document document;
  document.Parse(infojson.c_str());

  auto tableInfoData = document["Table List"].GetArray();

  for (auto it = tableInfoData.Begin(); it != tableInfoData.End(); it++) {
    Value& tblinfo = *it;
    string tmpTableName = tblinfo["tablename"].GetString();
    auto schemaInfo = tblinfo["Schema"].GetArray();
    for (auto j = schemaInfo.Begin(); j != schemaInfo.End(); j++) {
      Value& schema = *j;
      KETIValue kValue;
      string colName = schema["column_name"].GetString();
      kValue.type = schema["type"].GetInt();
      kValue.length = schema["length"].GetInt();
      this->tableInfo[tmpTableName][colName] = kValue;
      this->schemaInfo[tmpTableName].emplace_back(colName);
    }
    auto sstInfo = tblinfo["SST List"].GetArray();
    for (auto j = sstInfo.Begin(); j != sstInfo.End(); j++) {
      Value& sst = *j;
      string sstName = sst["filename"].GetString();
      int rowCnt = sst["rowCount"].GetInt();
      this->sstRowCnt[tmpTableName][sstName] = rowCnt;
    }
  }
}
void TableManager::InitCSDTableManager() {
  // /dev/sda /dev/ngd-blk
}

vector<string> TableManager::GetTableRep(string csd_name, string table_name) {
  cout << "csd_table_mamager :: 66" << csd_name << endl;
  string repKey = csd_name + "/" + table_name;
  return table_rep[repKey];
}

vector<string> TableManager::GetSchemaList(string tableName) {
  return this->schemaInfo[tableName];
}
int TableManager::GetSchemaLength(string tableName, string colName) {
  return this->tableInfo[tableName][colName].length;
}
int TableManager::GetSchemaType(string tableName, string colName) {
  return this->tableInfo[tableName][colName].type;
}
int TableManager::GetTableRow(string sstName, string tableName) {
  return this->sstRowCnt[tableName][sstName];
}