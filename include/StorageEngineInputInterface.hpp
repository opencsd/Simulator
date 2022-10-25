#pragma once
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <iostream>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

// Include boost thread
#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "grpc/snippet_sample.grpc.pb.h"

// Include rapidjson
#include "document.h"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

//#include "TableManager.h"
#include "CSDScheduler.hpp"
#include "SnippetManager.hpp"
// #include "SnippetManager.h"
// #include "snippet_sample.pb.h"
// #include "snippet_sample.grpc.pb.h"
#include "WalManager.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using snippetsample::Request;
using snippetsample::Result;
using snippetsample::Snippet;
using snippetsample::SnippetSample;

#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace rapidjson;
using namespace std;

class EngineModule {
 public:
  TableManager tblManager;
  CSDManager csdmanager;
  Scheduler csdscheduler;
  BufferManager bufma;
  // SnippetManager snippetmanager(tblManager,csdscheduler,csdmanager);
  SnippetManager snippetmanager;
  EngineModule() {
    csdscheduler.init_scheduler(csdmanager);
    bufma.InitBufferManager(csdscheduler);
  }
  void accept_connection(int server_fd);
  // int my_LBA2PBA(char* req,char* res);
  int my_LBA2PBA(std::string &req_json, std::string &res_json);

  void AppendProjection(SnippetStruct &snippetdata, Value &Projectiondata);
  void AppendProjection(SnippetStruct &snippetdata,
                        snippetsample::Snippet &snippet);
  void AppendDependencyProjection(SnippetStruct &snippetdata,
                                  Value &Projectiondata);
  void AppendDependencyProjection(
      SnippetStruct &snippetdata,
      snippetsample::Snippet_Dependency &dependency);
  void AppendTableFilter(SnippetStruct &snippetdata, Value &filterdata);
  void AppendTableFilter(SnippetStruct &snippetdata,
                         snippetsample::Snippet &snippet);
  void AppendDependencyFilter(SnippetStruct &snippetdata, Value &filterdata);
  void AppendDependencyFilter(SnippetStruct &snippetdata,
                              snippetsample::Snippet_Dependency &dependency);
  void testrun(string dirname);
  void testrun(string dirname, string jsonPath);
  void RunServer();
  void RunServer(string jsonPath);
};

// void testsetdata();

// void testjoin();

// void testaggregation();
class SnippetSampleServiceImpl final : public SnippetSample::Service {
  Status SetSnippet(
      ServerContext *context,
      ServerReaderWriter<Result, SnippetRequest> *stream) override {
    SnippetRequest snippetrequest;
    // Snippet snippet;
    queue<SnippetStruct> snippetqueue;
    EngineModule em;
    st = time(0);
    keti_log("Storage Engine Instance",
             "\t------------:: STEP 2 ::------------");
    while (stream->Read(&snippetrequest)) {
      // gRPC -> custom struct
      {
        SnippetStruct snippetdata;

        snippetdata.snippetType = snippetrequest.type();
        auto snippet = snippetrequest.snippet();

        em.AppendTableFilter(snippetdata, snippet);

        if (snippetdata.snippetType ==
            snippetsample::SnippetRequest::DEPENDENCY_OPER_SNIPPET) {
          if (snippet.has_dependency()) {
            auto dependency = snippet.dependency();
            em.AppendDependencyProjection(snippetdata, dependency);
            em.AppendDependencyFilter(snippetdata, dependency);
          }
        }

        for (int i = 0; i < snippet.table_name_size(); i++) {
          snippetdata.tablename.push_back(snippet.table_name(i));
        }

        for (int i = 0; i < snippet.column_alias_size(); i++) {
          snippetdata.column_alias.push_back(snippet.column_alias(i));
        }

        for (int i = 0; i < snippet.column_filtering_size(); i++) {
          snippetdata.columnFiltering.push_back(snippet.column_filtering(i));
        }

        if (snippet.has_order_by()) {
          auto order_by = snippet.order_by();
          for (int j = 0; j < order_by.ascending_size(); j++) {
            snippetdata.orderBy.push_back(order_by.column_name(j));
            snippetdata.orderType.push_back(order_by.ascending(j));
          }
        }

        for (int i = 0; i < snippet.group_by_size(); i++) {
          snippetdata.groupBy.push_back(snippet.group_by(i));
        }

        for (int i = 0; i < snippet.table_col_size(); i++) {
          snippetdata.table_col.push_back(snippet.table_col(i));
        }

        for (int i = 0; i < snippet.table_offset_size(); i++) {
          snippetdata.table_offset.push_back(snippet.table_offset(i));
        }

        for (int i = 0; i < snippet.table_offlen_size(); i++) {
          snippetdata.table_offlen.push_back(snippet.table_offlen(i));
        }

        for (int i = 0; i < snippet.table_datatype_size(); i++) {
          snippetdata.table_datatype.push_back(snippet.table_datatype(i));
        }

        snippetdata.tableAlias = snippet.table_alias();
        snippetdata.query_id = snippet.query_id();
        snippetdata.work_id = snippet.work_id();
        em.AppendProjection(snippetdata, snippet);
        snippetqueue.push(snippetdata);

        string wal_json;
      }
      string wal_json;

      // if(snippetrequest.type() == 0) {
      //   WalManager test(snippetrequest.snippet());
      //   test.run(wal_json);
      //   snippetdata.wal_json = wal_json;
      // }
    }
    LQNAME = snippetqueue.back().tableAlias;
    Queryid = snippetqueue.back().query_id;
    em.snippetmanager.NewQuery(snippetqueue, em.bufma, em.tblManager,
                               em.csdscheduler, em.csdmanager);

    unordered_map<string, vector<vectortype>> RetTable =
        em.bufma.GetTableData(Queryid, LQNAME).table_data;

    for (auto i = RetTable.begin(); i != RetTable.end(); i++) {
      pair<string, vector<vectortype>> tmppair = *i;
    }
    return Status::OK;
  }
  Status Run(ServerContext *context, const Request *request,
             Result *result) override {
    query_result = "\n";
    EngineModule em;
    {
      unordered_map<string, vector<vectortype>> RetTable =
          em.bufma.GetTableData(Queryid, LQNAME).table_data;
      query_result += "+--------------------------------+\n ";
      for (auto i = RetTable.begin(); i != RetTable.end(); i++) {
        pair<string, vector<vectortype>> tmppair = *i;
        query_result += tmppair.first + "  ";
      }
      query_result += "\n";
      query_result += "+--------------------------------+\n";

      auto tmppair = *RetTable.begin();
      int ColumnCount = tmppair.second.size();
      for (int i = 0; i < ColumnCount; i++) {
        string tmpstring = " ";
        for (auto j = RetTable.begin(); j != RetTable.end(); j++) {
          pair<string, vector<vectortype>> tmppair = *j;
          if (tmppair.second[i].type == 1) {
            if (tmppair.second[i].intvec == 103) {
              tmpstring += to_string(tmppair.second[i].intvec) + "           ";
            } else {
              tmpstring += to_string(tmppair.second[i].intvec) + "            ";
            }
          } else if (tmppair.second[i].type == 2) {
            tmpstring += to_string(tmppair.second[i].floatvec) + "    ";
          } else {
            tmpstring += tmppair.second[i].strvec + "            ";
          }
        }
        query_result += tmpstring + "\n";
      }
      query_result += "+--------------------------------+";
      fi = time(0);
    }

    result->set_value(query_result);

    query_result = "";
    em.bufma.EndQuery(Queryid);
    return Status::OK;
  }

 private:
  std::unordered_map<int, std::vector<std::string>> map;
  std::string query_result = "";
  time_t fi, st;
  string LQNAME;
  int Queryid;
};