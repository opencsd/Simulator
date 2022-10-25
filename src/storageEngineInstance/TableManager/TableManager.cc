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

int TableManager::generate_req_json(std::string tablename, char *dst) {
  Document document;
  document.SetObject();

  if (m_TableManager.find(tablename) == m_TableManager.end()) {
    std::cout << "Not Present" << std::endl;
    return -1;
  }

  struct Table tbl = m_TableManager[tablename];

  rapidjson::Document::AllocatorType &allocator = document.GetAllocator();

  Value REQ(kObjectType);

  Value str_val(kObjectType);
  str_val.SetString("Chunk | CSD", static_cast<SizeType>(strlen("Chunk | CSD")),
                    allocator);

  REQ.AddMember("Ordering", str_val, allocator);  // set req_json ordering

  Value Chunk_List(kArrayType);

  vector<struct SSTFile>::iterator itor = tbl.SSTList.begin();
  for (; itor != tbl.SSTList.end(); itor++) {
    Value Chunk(kObjectType);  // req chunk object

    str_val.SetString((*itor).filename.c_str(),
                      static_cast<SizeType>(strlen((*itor).filename.c_str())),
                      allocator);
    Chunk.AddMember("filename", str_val, allocator);  // set res_json ordering

    Value Offset_List(kArrayType);  // req offset list
    vector<struct DataBlockHandle>::iterator itor2 = (*itor).BlockList.begin();
    for (; itor2 != (*itor).BlockList.end(); itor2++) {
      Value Offset(kObjectType);  // req offset
      Offset.AddMember("Offset", (*itor2).Offset, allocator);
      Offset.AddMember("Length", (*itor2).Length, allocator);

      Offset_List.PushBack(Offset, allocator);
    }
    Chunk.AddMember("Chunks", Offset_List, allocator);
    Chunk_List.PushBack(Chunk, allocator);
  }
  REQ.AddMember("Chunk List", Chunk_List, allocator);  // set req_json ordering
  document.AddMember("REQ", REQ, allocator);

  rapidjson::StringBuffer strbuf;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
  document.Accept(writer);

  // std::cout << strbuf.GetString() << std::endl;
  strcpy(dst, strbuf.GetString());
  return 0;
}

int TableManager::generate_req_json(std::string tablename, std::string &dst) {
  cout << "Request JSON Generate" << endl;
  Document document;
  document.SetObject();

  if (m_TableManager.find(tablename) == m_TableManager.end()) {
    std::cout << "Not Present" << std::endl;
    return -1;
  }

  struct Table tbl = m_TableManager[tablename];

  rapidjson::Document::AllocatorType &allocator = document.GetAllocator();

  Value REQ(kObjectType);

  Value str_val(kObjectType);
  str_val.SetString("Chunk | CSD", static_cast<SizeType>(strlen("Chunk | CSD")),
                    allocator);

  REQ.AddMember("Ordering", str_val, allocator);  // set req_json ordering

  Value Chunk_List(kArrayType);

  vector<struct SSTFile>::iterator itor = tbl.SSTList.begin();
  for (; itor != tbl.SSTList.end(); itor++) {
    Value Chunk(kObjectType);  // req chunk object

    str_val.SetString((*itor).filename.c_str(),
                      static_cast<SizeType>(strlen((*itor).filename.c_str())),
                      allocator);
    Chunk.AddMember("filename", str_val, allocator);  // set res_json ordering

    Value Offset_List(kArrayType);  // req offset list
    vector<struct DataBlockHandle>::iterator itor2 = (*itor).BlockList.begin();
    for (; itor2 != (*itor).BlockList.end(); itor2++) {
      Value Offset(kObjectType);  // req offset
      Offset.AddMember("Offset", (*itor2).Offset, allocator);
      Offset.AddMember("Length", (*itor2).Length, allocator);

      Offset_List.PushBack(Offset, allocator);
    }
    Chunk.AddMember("Chunks", Offset_List, allocator);
    Chunk_List.PushBack(Chunk, allocator);
  }
  REQ.AddMember("Chunk List", Chunk_List, allocator);  // set req_json ordering
  document.AddMember("REQ", REQ, allocator);

  rapidjson::StringBuffer strbuf;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
  document.Accept(writer);

  // std::cout << strbuf.GetString() << std::endl;
  // strcpy(dst,strbuf.GetString());
  dst = std::string(strbuf.GetString());
  // cout << dst << endl;
  cout << "Request JSON Set Done" << endl;

  return 0;
}

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

      vector<struct DataBlockHandle>::iterator itor3 =
          (*itor2).BlockList.begin();
      std::cout << " StartOffset-Length" << endl;
      for (; itor3 != (*itor2).BlockList.end(); itor3++) {
        std::cout << "Block Handle : " << (*itor3).Offset << "-"
                  << (*itor3).Length << std::endl;
      }
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
  // read TableManager.json
  int json_fd;
  string json = "";
  char buf;

  // memset(json,0,sizeof(json));
  //  json_fd = open("TableManager.json",O_RDONLY);
  //  json_fd = open("NewTableManager.json",O_RDONLY);
  if ((json_fd = open("../../NewTableManager.json", O_RDONLY)) < 0) {
    fprintf(stderr, "open error for %s\n", "NewTableManager.json");
    exit(1);
  }
  int i = 0;
  int res;

  while (1) {
    // cout << 1;
    // res = read(json_fd,&buf,1);
    // if(res == 0){
    // 	break;
    // }
    // json += buf;
    if (read(json_fd, &buf, 1) > 0)
      json += buf;
    else
      break;
  }
  close(json_fd);

  // debug code check read json
  // std::cout << "json : " << json << std::endl;
  // cout << 1 << endl;
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

    // tbl.tablesize = TableObject["Size"].GetInt();

    // if(TableObject.HasMember("PK")){
    // 	for(int i = 0; i < TableObject["PK"].Size(); i++){
    // 		tbl.PK.push_back(TableObject["PK"][i].GetString());
    // 	}
    // }
    // if(TableObject.HasMember("Index")){
    // 	for(int i = 0; i < TableObject["Index"].Size(); i++){
    // 		vector<string> index;
    // 		for(int j = 0; j < TableObject["Index"][i].Size(); j++){
    // 			index.push_back(TableObject["Index"][i][j].GetString());
    // 		}
    // 		tbl.IndexList.push_back(index);
    // 	}
    // }
    // debug code check filename
    // std::cout << "tablename : " << tbl.tablename << std::endl;

    Value &SchemaObject = TableObject["Schema"];
    int j;
    for (j = 0; j < SchemaObject.Size(); j++) {
      // cout << i << " " << j << endl;
      Value &ColumnObject = SchemaObject[j];
      struct ColumnSchema Column;

      Column.column_name = ColumnObject["column_name"].GetString();
      Column.type = ColumnObject["type"].GetInt();
      Column.length = ColumnObject["length"].GetInt();
      Column.offset = ColumnObject["offset"].GetInt();

      tbl.Schema.push_back(Column);
      // debug code check Schema
      // std::cout << "Schema : " << tbl.Schema[j].column_name << std::endl;
    }

    Value &SSTList = TableObject["SST List"];
    for (j = 0; j < SSTList.Size(); j++) {
      Value &SSTObject = SSTList[j];
      struct SSTFile SSTFile;

      SSTFile.filename = SSTObject["filename"].GetString();

      Value &BlockList = SSTObject["Block List"];

      for (int k = 0; k < BlockList.Size(); k++) {
        Value &BlockHandleObject = BlockList[k];
        struct DataBlockHandle DataBlockHandle;

        DataBlockHandle.Offset = BlockHandleObject["Offset"].GetInt();
        DataBlockHandle.Length = BlockHandleObject["Length"].GetInt();

        SSTFile.BlockList.push_back(DataBlockHandle);
      }

      tbl.SSTList.push_back(SSTFile);
      // debug code check sst filename
      // std::cout << "SST : " << SSTObject["filename"].GetString() <<
      // std::endl;
    }
    m_TableManager.insert({tbl.tablename, tbl});
  }

  return 0;
}

int TableManager::init_TableManager(string filePath) {
  cout << "init_TableManager" << endl;
  int json_fd;
  string json = "";
  char buf;

  if ((json_fd = open(filePath.c_str(), O_RDONLY)) < 0) {
    fprintf(stderr, "open error for %s\n", "NewTableManager.json");
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

      Value &BlockList = SSTObject["Block List"];

      for (int k = 0; k < BlockList.Size(); k++) {
        Value &BlockHandleObject = BlockList[k];
        struct DataBlockHandle DataBlockHandle;

        DataBlockHandle.Offset = BlockHandleObject["Offset"].GetInt();
        DataBlockHandle.Length = BlockHandleObject["Length"].GetInt();

        SSTFile.BlockList.push_back(DataBlockHandle);
      }

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

vector<string> TableManager::get_ordered_table_by_size(
    vector<string> tablenames) {
  vector<pair<string, int>> tablesize;
  for (int i = 0; i < tablenames.size(); i++) {
    struct Table tbl = m_TableManager[tablenames[i]];
    tablesize.push_back(make_pair(tbl.tablename, tbl.tablesize));
  }
  sort(tablesize.begin(), tablesize.end(),
       [](pair<string, int> a, pair<string, int> b) {
         return a.second < b.second;
       });
  vector<string> ret;
  for (int i = 0; i < tablesize.size(); i++) {
    ret.push_back(tablesize[i].first);
  }
  return ret;
}

int TableManager::get_IndexList(string tablename, vector<vector<string>> &dst) {
  if (m_TableManager.find(tablename) == m_TableManager.end()) {
    std::cout << "Not Present" << std::endl;
    return -1;
  }
  struct Table tbl = m_TableManager[tablename];
  dst = tbl.IndexList;
  return 0;
}
