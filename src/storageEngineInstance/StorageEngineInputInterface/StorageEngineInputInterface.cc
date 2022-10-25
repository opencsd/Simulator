#include "StorageEngineInputInterface.hpp"

#include "keti_util.hpp"

// Include C++ Header
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// Include gRPC Header
#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using snippetsample::Request;
using snippetsample::Result;
using snippetsample::Snippet;
using snippetsample::SnippetRequest;
using snippetsample::SnippetSample;

using namespace std;

// fill SnippetStruct TableFilter with json
void EngineModule::AppendTableFilter(SnippetStruct &snippetdata,
                                     Value &filterdata) {
  for (int i = 0; i < filterdata.Size(); i++) {
    filterstruct tmpfilterst;
    if (filterdata[i].HasMember("LV")) {
      for (int j = 0; j < filterdata[i]["LV"]["type"].Size(); j++) {
        lv tmplv;
        tmplv.type.push_back(filterdata[i]["LV"]["type"][j].GetInt());
        tmplv.value.push_back(filterdata[i]["LV"]["value"][j].GetString());
        tmpfilterst.LV = tmplv;
      }
    }
    tmpfilterst.filteroper = filterdata[i]["Operator"].GetInt();
    if (filterdata[i].HasMember("RV")) {
      rv tmprv;
      for (int j = 0; j < filterdata[i]["RV"]["type"].Size(); j++) {
        tmprv.type.push_back(filterdata[i]["RV"]["type"][j].GetInt());
        tmprv.value.push_back(filterdata[i]["RV"]["value"][j].GetString());
      }
      tmpfilterst.RV = tmprv;
    }
    snippetdata.table_filter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct TableFilter with gRPC message
void EngineModule::AppendTableFilter(SnippetStruct &snippetdata,
                                     snippetsample::Snippet &snippet) {
  for (int i = 0; i < snippet.table_filter_size(); i++) {
    auto filter = snippet.table_filter(i);
    filterstruct tmpfilterst;
    if (filter.has_lv()) {
      lv tmplv;
      auto lv = filter.lv();
      for (int j = 0; j < lv.type_size(); j++) {
        tmplv.type.push_back(lv.type(j));
        tmplv.value.push_back(lv.value(j));
      }
      tmpfilterst.LV = tmplv;
    }
    tmpfilterst.filteroper = filter.operator_();
    if (filter.has_rv()) {
      rv tmprv;
      auto rv = filter.rv();
      for (int j = 0; j < rv.type_size(); j++) {
        tmprv.type.push_back(rv.type(j));
        tmprv.value.push_back(rv.value(j));
      }
      tmpfilterst.RV = tmprv;
    }
    snippetdata.table_filter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct DependencyFilter with json
void EngineModule::AppendDependencyFilter(SnippetStruct &snippetdata,
                                          Value &filterdata) {
  for (int i = 0; i < filterdata.Size(); i++) {
    filterstruct tmpfilterst;
    if (filterdata[i].HasMember("LV")) {
      for (int j = 0; j < filterdata[i]["LV"]["type"].Size(); j++) {
        lv tmplv;
        tmplv.type.push_back(filterdata[i]["LV"]["type"][j].GetInt());
        tmplv.value.push_back(filterdata[i]["LV"]["value"][j].GetString());
        tmpfilterst.LV = tmplv;
      }
    }
    tmpfilterst.filteroper = filterdata[i]["Operator"].GetInt();
    if (filterdata[i].HasMember("RV")) {
      for (int j = 0; j < filterdata[i]["RV"]["type"].Size(); j++) {
        rv tmprv;
        tmprv.type.push_back(filterdata[i]["RV"]["type"][j].GetInt());
        tmprv.value.push_back(filterdata[i]["RV"]["value"][j].GetString());
        tmpfilterst.RV = tmprv;
      }
    }
    snippetdata.dependencyFilter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct DependencyFilter with gRPC message
void EngineModule::AppendDependencyFilter(
    SnippetStruct &snippetdata, snippetsample::Snippet_Dependency &dependency) {
  for (int i = 0; i < dependency.dependency_filter_size(); i++) {
    auto filter = dependency.dependency_filter(i);
    filterstruct tmpfilterst;
    if (filter.has_lv()) {
      lv tmplv;
      auto lv = filter.lv();
      for (int j = 0; j < lv.type_size(); j++) {
        tmplv.type.push_back(lv.type(j));
        tmplv.value.push_back(lv.value(j));
      }
      tmpfilterst.LV = tmplv;
    }
    tmpfilterst.filteroper = filter.operator_();
    if (filter.has_rv()) {
      rv tmprv;
      auto rv = filter.rv();
      for (int j = 0; j < rv.type_size(); j++) {
        tmprv.type.push_back(rv.type(j));
        tmprv.value.push_back(rv.value(j));
      }
      tmpfilterst.RV = tmprv;
    }
    snippetdata.dependencyFilter.push_back(tmpfilterst);
  }
}

// fill SnippetStruct Projection with json
void EngineModule::AppendProjection(SnippetStruct &snippetdata,
                                    Value &Projectiondata) {
  for (int i = 0; i < Projectiondata.Size(); i++) {
    vector<Projection> tmpVec;
    if (Projectiondata[i]["selectType"] ==
        static_cast<int>(KETI_Aggregation_Type::KETI_COUNTALL)) {
      Projection tmpPro;
      tmpPro.value = "4";
      tmpVec.push_back(tmpPro);
    } else {
      for (int j = 0; j < Projectiondata[i]["value"].Size(); j++) {
        if (j == 0) {
          Projection tmpPro;
          tmpPro.value = to_string(Projectiondata[i]["selectType"].GetInt());
          tmpVec.push_back(tmpPro);
        }

        Projection tmpPro;
        tmpPro.type = Projectiondata[i]["valueType"][j].GetInt();
        tmpPro.value = Projectiondata[i]["value"][j].GetString();
        tmpVec.push_back(tmpPro);
      }
    }
    snippetdata.columnProjection.push_back(tmpVec);
  }
}

// fill SnippetStruct Projection with gRPC message
void EngineModule::AppendProjection(SnippetStruct &snippetdata,
                                    snippetsample::Snippet &snippet) {
  for (int i = 0; i < snippet.column_projection_size(); i++) {
    auto projection = snippet.column_projection(i);
    vector<Projection> tmpVec;
    if (projection.select_type() ==
        snippetsample::Snippet_Projection::COUNTSTAR) {
      Projection tmpPro;
      tmpPro.value = to_string(projection.select_type());
      ;
      tmpVec.push_back(tmpPro);
    } else {
      for (int j = 0; j < projection.value_type_size(); j++) {
        if (j == 0) {
          Projection tmpPro;
          tmpPro.value = to_string(projection.select_type());
          tmpVec.push_back(tmpPro);
        }
        Projection tmpPro;
        tmpPro.type = projection.value_type(j);
        tmpPro.value = projection.value(j);
        tmpVec.push_back(tmpPro);
      }
    }
    snippetdata.columnProjection.push_back(tmpVec);
  }
}

// fill SnippetStruct DependencyProjection with json
void EngineModule::AppendDependencyProjection(SnippetStruct &snippetdata,
                                              Value &Projectiondata) {
  for (int i = 0; i < Projectiondata.Size(); i++) {
    vector<Projection> tmpVec;
    if (Projectiondata[i]["selectType"] ==
        static_cast<int>(KETI_Aggregation_Type::KETI_COUNTALL)) {
      Projection tmpPro;
      tmpPro.value = "4";
      tmpVec.push_back(tmpPro);
    } else {
      for (int j = 0; j < Projectiondata[i]["value"].Size(); j++) {
        if (j == 0) {
          Projection tmpPro;
          tmpPro.value = to_string(Projectiondata[i]["selectType"].GetInt());
          tmpVec.push_back(tmpPro);
        }

        Projection tmpPro;
        tmpPro.type = Projectiondata[i]["valueType"][j].GetInt();
        tmpPro.value = Projectiondata[i]["value"][j].GetString();
        tmpVec.push_back(tmpPro);
      }
    }
    snippetdata.dependencyProjection.push_back(tmpVec);
  }
}

// fill SnippetStruct DependencyProjection with gRPC message
void EngineModule::AppendDependencyProjection(
    SnippetStruct &snippetdata, snippetsample::Snippet_Dependency &dependency) {
  for (int i = 0; i < dependency.dependency_projection_size(); i++) {
    auto projection = dependency.dependency_projection(i);
    // cout << i << endl;
    vector<Projection> tmpVec;
    if (projection.select_type() ==
        snippetsample::Snippet_Projection::COUNTSTAR) {
      Projection tmpPro;
      tmpPro.value = to_string(projection.select_type());
      ;
      tmpVec.push_back(tmpPro);
    } else {
      for (int j = 0; j < projection.value_type_size(); j++) {
        if (j == 0) {
          Projection tmpPro;
          tmpPro.value = to_string(projection.select_type());
          tmpVec.push_back(tmpPro);
        }
        Projection tmpPro;
        tmpPro.type = projection.value_type(j);
        tmpPro.value = projection.value(j);
        tmpVec.push_back(tmpPro);
      }
    }
    snippetdata.columnProjection.push_back(tmpVec);
  }
}

void EngineModule::testrun(string dirname, string jsonPath) {
  cout << "Run Engine Module" << endl;
  tblManager.init_TableManager(jsonPath);
  cout << "TableManager Init Done" << endl;
  time_t st = time(0);
  queue<SnippetStruct> snippetqueue;
  string tmpstring = "";
  cout << dirname[dirname.size() - 1] << endl;
  if (dirname[dirname.size() - 1] == '/') {
    cout << "dirname end error" << endl;
    string tmpdirName = "";
    for (int i = 0; i < dirname.size() - 1; i++) {
      tmpdirName = tmpdirName + dirname[i];
    }
    dirname = tmpdirName;
  }
  cout << dirname << endl;

  cout << dirname[dirname.size() - 1] << "  "
       << int(dirname[dirname.size() - 1]) << endl;
  cout << dirname[dirname.size() - 2] << "  "
       << int(dirname[dirname.size() - 2]) << endl;

  if (int(dirname[dirname.size() - 2]) > 47 &&
      int(dirname[dirname.size() - 2]) < 58) {
    cout << "dirname number Setting case 1" << endl;
    cout << dirname << endl;
    tmpstring = dirname[dirname.size() - 2] + dirname[dirname.size() - 1];
  } else {
    string zero_str = "0";
    cout << "dirname number Setting case 2" << endl;
    cout << dirname << endl;
    cout << dirname[dirname.size() - 1] << "  "
         << int(dirname[dirname.size() - 1]) << endl;
    cout << "Will Be " << zero_str + dirname[dirname.size() - 1] << endl;
    tmpstring = zero_str + dirname[dirname.size() - 1];
  }

  cout << tmpstring << endl;

  int filecount = 0;
  for (const auto &file : filesystem::directory_iterator(dirname)) {
    filecount++;
  }

  for (int i = 0; i < filecount; i++) {
    string filename =
        dirname + "/tpch" + tmpstring + "-" + to_string(i) + ".json";
    cout << "read " << filename << endl;
    int json_fd;
    string json = "";
    json_fd = open(filename.c_str(), O_RDONLY);
    int res;
    char buf;

    while (1) {
      res = read(json_fd, &buf, 1);
      if (res == 0) {
        break;
      }
      json += buf;
    }
    close(json_fd);
    Document doc;
    doc.Parse(json.c_str());
    Value &document = doc["snippet"];
    SnippetStruct snippetdata;
    snippetdata.snippetType = doc["type"].GetInt();
    AppendTableFilter(snippetdata, document["tableFilter"]);

    if (snippetdata.snippetType == 6) {
      if (document.HasMember("dependency")) {
        if (document["dependency"].HasMember("dependencyProjection")) {
          AppendDependencyProjection(
              snippetdata, document["dependency"]["dependencyProjection"]);
        }
        if (document["dependency"].HasMember("dependencyFilter")) {
          AppendDependencyFilter(snippetdata,
                                 document["dependency"]["dependencyFilter"]);
        }
      }
    }

    for (int i = 0; i < document["tableName"].Size(); i++) {
      snippetdata.tablename.push_back(document["tableName"][i].GetString());
    }

    for (int i = 0; i < document["columnAlias"].Size(); i++) {
      snippetdata.column_alias.push_back(
          document["columnAlias"][i].GetString());
    }

    for (int i = 0; i < document["columnFiltering"].Size(); i++) {
      snippetdata.columnFiltering.push_back(
          document["columnFiltering"][i].GetString());
    }

    if (document.HasMember("orderBy")) {
      for (int j = 0; j < document["orderBy"]["columnName"].Size(); j++) {
        snippetdata.orderBy.push_back(
            document["orderBy"]["columnName"][j].GetString());
        snippetdata.orderType.push_back(
            document["orderBy"]["ascending"][j].GetInt());
      }
    }

    for (int i = 0; i < document["groupBy"].Size(); i++) {
      snippetdata.groupBy.push_back(document["groupBy"][i].GetString());
    }
    for (int i = 0; i < document["tableCol"].Size(); i++) {
      snippetdata.table_col.push_back(document["tableCol"][i].GetString());
    }
    for (int i = 0; i < document["tableOffset"].Size(); i++) {
      snippetdata.table_offset.push_back(document["tableOffset"][i].GetInt());
    }
    for (int i = 0; i < document["tableOfflen"].Size(); i++) {
      snippetdata.table_offlen.push_back(document["tableOfflen"][i].GetInt());
    }
    for (int i = 0; i < document["tableDatatype"].Size(); i++) {
      snippetdata.table_datatype.push_back(
          document["tableDatatype"][i].GetInt());
    }

    snippetdata.tableAlias = document["tableAlias"].GetString();

    snippetdata.query_id = document["queryID"].GetInt();

    snippetdata.work_id = document["workID"].GetInt();

    AppendProjection(snippetdata, document["columnProjection"]);

    snippetqueue.push(snippetdata);
  }
  cout << "Read Snippet Done" << endl;
  string LQNAME = snippetqueue.back().tableAlias;
  int Queryid = snippetqueue.back().query_id;

  cout << "Query to CSD" << endl;
  snippetmanager.NewQuery(snippetqueue, bufma, tblManager, csdscheduler,
                          csdmanager);
  cout << "GET DATA to CSD" << endl;
  unordered_map<string, vector<vectortype>> RetTable =
      bufma.GetTableData(Queryid, LQNAME).table_data;

  // keti_log("DB Connector Instance", "\n\t------------:: STEP 4
  // ::------------");
  // cout << "GET DATA to CSD" << endl;
  std::vector<int> len;
  cout << "+--------------------------------------+" << endl;
  cout << " ";
  for (auto i = RetTable.begin(); i != RetTable.end(); i++) {
    pair<string, vector<vectortype>> tmppair = *i;
    cout << tmppair.first;
    len.push_back(tmppair.first.length());
    cout << " \t";
  }
  cout << endl;
  cout << "+--------------------------------------+" << endl;

  auto tmppair = *RetTable.begin();
  int ColumnCount = tmppair.second.size();
  for (int i = 0; i < ColumnCount; i++) {
    string tmpstring = " ";
    int col_idx = 0;
    for (auto j = RetTable.begin(); j != RetTable.end(); j++) {
      pair<string, vector<vectortype>> tmppair = *j;

      if (tmppair.second[i].type == 1) {
        if (tmppair.second[i].intvec == 103) {
          tmpstring += to_string(tmppair.second[i].intvec);
        } else {
          tmpstring += to_string(tmppair.second[i].intvec);
        }
        // std::string space(
        //     len[col_idx] - to_string(tmppair.second[i].intvec).length(), '
        //     ');
        // tmpstring += space;
        tmpstring += " \t";
      } else if (tmppair.second[i].type == 2) {
        tmpstring += to_string(tmppair.second[i].floatvec);
        // std::string space(
        //     len[col_idx] - to_string(tmppair.second[i].floatvec).length(), '
        //     ');
        // tmpstring += space;
        tmpstring += " \t";

      } else {
        tmpstring += tmppair.second[i].strvec;
        // std::string space(len[col_idx] - tmppair.second[i].strvec.length(),
        //                   ' ');
        // tmpstring += space;
        tmpstring += " \t";
      }
    }
    cout << tmpstring << endl;
    ;
    col_idx++;
  }
  cout << "+--------------------------------------+" << endl;
  ;
  time_t fi = time(0);

  sleep(100);
}
void EngineModule::RunServer(string jsonPath) {
  tblManager.init_TableManager(jsonPath);
  std::string server_address("0.0.0.0:50051");
  SnippetSampleServiceImpl service;

  ServerBuilder builder;

  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  server->Wait();
}
