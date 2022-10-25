#include "SnippetManager.hpp"
using namespace std;
void SnippetManager::NewQuery(queue<SnippetStruct> newqueue,
                              BufferManager &buff, TableManager &tableManager_,
                              Scheduler &scheduler_, CSDManager &csdManager_) {
  //스레드 생성
  // while(!newqueue.empty()){
  //     SnippetStruct a = newqueue.front();
  //     cout << a.table_filter.Size() << endl;
  //     newqueue.pop();
  // }
  SavedRet tmpquery;
  tmpquery.NewQuery(newqueue);

  // thread t1 = thread(NewQueryMain,tmpquery);
  // t1.join(); //고민필요
  NewQueryMain(tmpquery, buff, tableManager_, scheduler_, csdManager_);
}

unordered_map<string, vector<vectortype>> SnippetManager::ReturnResult(
    int queryid) {
  //스레드 생성
  unordered_map<string, vector<vectortype>> ret;
  SavedResult[queryid].lockmutex();
  ret = SavedResult[queryid].ReturnResult();
  SavedResult[queryid].unlockmutex();
  return ret;
}

void SavedRet::NewQuery(queue<SnippetStruct> newqueue) {
  QueryQueue = newqueue;
}

unordered_map<string, vector<vectortype>> SavedRet::ReturnResult() {
  return result_;
}

void SavedRet::lockmutex() { mutex_.lock(); }

void SavedRet::unlockmutex() { mutex_.unlock(); }

SnippetStruct SavedRet::Dequeue() {
  SnippetStruct tmp = QueryQueue.front();
  QueryQueue.pop();
  return tmp;
}

int SavedRet::GetSnippetSize() { return QueryQueue.size(); }

void SavedRet::SetResult(unordered_map<string, vector<vectortype>> result) {
  result_ = result;
}

void SnippetManager::NewQueryMain(SavedRet &snippetdata, BufferManager &buff,
                                  TableManager &tblM, Scheduler &scheduler_,
                                  CSDManager &csdmanager) {
  cout << "New Query Main" << endl;
  int LQID = 0;   // last query id
  string LTName;  // last table name
  int snippetsize = snippetdata.GetSnippetSize();

  for (int i = 0; i < snippetsize; i++) {
    SnippetStruct snippet = snippetdata.Dequeue();

    SnippetRun(snippet, buff, tblM, scheduler_, csdmanager);
    LQID = snippet.query_id;
    LTName = snippet.tableAlias;
  }
  snippetdata.SetResult(buff.GetTableData(LQID, LTName).table_data);
  keti_log("Snippet Manager", "End Query");
  keti_log("Snippet Manager", "Send Query Result To DB Connector Instance");
}

void SnippetManager::SnippetRun(SnippetStruct &snippet, BufferManager &buff,
                                TableManager &tableManager_,
                                Scheduler &scheduler_, CSDManager &csdmanager) {
  cout << "SnippetRUN()" << endl;
  cout << "Snippet Size :: " << snippet.tablename.size() << endl;
  vector<string> emptyVector;
  //비트 마스킹으로 교체 예정
  //여기서 각 작업별 수행 위치 선정
  //테이블 수에 따라서 일단 작업 선정
  if (snippet.tablename.size() > 1) {
    buff.InitWork(snippet.query_id, snippet.work_id, snippet.tableAlias,
                  snippet.column_alias, snippet.return_datatype,
                  snippet.table_offlen, 0, emptyVector);
    cout << "Snippet Type :: " << snippet.snippetType << endl;
    switch (snippet.snippetType) {
      case snippetsample::SnippetRequest::BASIC_SNIPPET: {  //변수 삽입하는
                                                            //스캔부분
        sendToSnippetScheduler(snippet, buff, scheduler_, tableManager_,
                               csdmanager);
      } break;
      case snippetsample::SnippetRequest::AGGREGATION_SNIPPET: {  // having
        Storage_Filter(snippet, buff);
      } break;
      case snippetsample::SnippetRequest::JOIN_SNIPPET: {
        JoinTable(snippet, buff);  // join 호출
        // JoinThread(snippet, buff);
        Aggregation(snippet, buff, 0);
      } break;
      case snippetsample::SnippetRequest::SUBQUERY_SNIPPET: {
      } break;
      case snippetsample::SnippetRequest::
          DEPENDENCY_EXIST_SNIPPET: {  // dependency
                                       // exist
        DependencyExist(snippet, buff);
      } break;
      case snippetsample::SnippetRequest::DEPENDENCY_NOT_EXIST_SNIPPET: {
        // dependency not exist
      } break;
      case snippetsample::SnippetRequest::
          DEPENDENCY_OPER_SNIPPET: {  // dependency
                                      // =
        DependencyOPER(snippet, buff);
        Aggregation(snippet, buff, 0);
      } break;
      case snippetsample::SnippetRequest::DEPENDENCY_IN_SNIPPET: {
      } break;
      case snippetsample::SnippetRequest::HAVING_SNIPPET: {  //그룹바이 이후
                                                             // having 부분
        Storage_Filter(snippet, buff);
      } break;
      case snippetsample::SnippetRequest::LEFT_OUTER_JOIN_SNIPPET: {  // having
        LOJoin(snippet, buff);
        Aggregation(snippet, buff, 0);
      } break;
    }
  } else {
    cout << "Single Table..." << endl;
    cout << "CheckTableStatus :: "
         << buff.CheckTableStatus(snippet.query_id, snippet.tablename[0])
         << endl;
    if (buff.CheckTableStatus(snippet.query_id, snippet.tablename[0]) == 3) {
      //단일 테이블인데 se작업
      // unordered_map<string,vector<vectortype>> RetTable =
      // buff.GetTableData(4,"snippet4-11").table_data;
      //   unordered_map<string,vector<vectortype>> RetTable =
      //   snippetmanager.ReturnResult(4);
      // for(auto i = RetTable.begin(); i != RetTable.end(); i++){
      //     pair<string,vector<vectortype>> tmppair = *i;
      //     cout << tmppair.first << endl;
      //     for(int j = 0; j < tmppair.second.size(); j++){
      //         if(tmppair.second[j].type == 1){
      //             cout << tmppair.second[j].intvec << endl;;
      //         }else if(tmppair.second[j].type == 2){
      //             cout << tmppair.second[j].floatvec << endl;;
      //         }else{
      //             cout << tmppair.second[j].strvec << endl;;
      //         }
      //         // cout << endl;
      //     }
      // }

      // auto tmppair = *RetTable.begin();
      //     int ColumnCount = tmppair.second.size();
      //     for(int i = 0; i < ColumnCount; i++){
      //         for(auto j = RetTable.begin(); j != RetTable.end(); j++){
      //             pair<string,vector<vectortype>> tmppair = *j;
      //             // cout << "type : " << tmppair.second[i].type << endl;
      //             // cout << tmppair.second[j] << " ";
      //             if(tmppair.second[i].type == 1){
      //             cout << tmppair.second[i].intvec << " ";
      //             }else if(tmppair.second[i].type == 2){
      //             cout <<" "<< tmppair.second[i].floatvec << " ";
      //             }else{
      //             cout << tmppair.second[i].strvec << " ";
      //             }
      //         }
      //         cout << endl;
      //     }
      buff.InitWork(snippet.query_id, snippet.work_id, snippet.tableAlias,
                    snippet.column_alias, snippet.return_datatype,
                    snippet.table_offlen, 0, emptyVector);
      if (snippet.groupBy.size() > 0) {
        //그룹바이 호출
        GroupBy(snippet, buff);
      } else if (snippet.columnProjection.size() > 0) {
        //어그리게이션 호출
        Aggregation(snippet, buff, 1);
      }
      if (snippet.orderBy.size() > 0) {
        //오더바이 호출
        OrderBy(snippet, buff);
      }
      // if(snippet.columnProjection.size() > 0){
      //     //어그리게이션 호출
      //     buff.InitWork(snippet.query_id,snippet.work_id,snippet.tableAlias,snippet.column_alias,snippet.return_datatype,snippet.table_offlen,0);
      // }
    } else {
      cout << "Make Request... Snippet Type :: " << snippet.snippetType << endl;
      // cout << snippet.snippetType << endl;
      if (snippet.snippetType == snippetsample::SnippetRequest::BASIC_SNIPPET) {
        cout << "Basic Snippet" << endl;
        ReturnColumnType(snippet, buff);
        // vector<int> returncolumnlen;

        // if(GetWALTable(snippet)){
        //     //머지쿼리에 데이터 필터 요청
        //     // Filtering(snippet); //wal 데이터 필터링
        // }
        //여기는 csd로 내리는 쪽으로
        //리턴타입 봐야함
        // lba2pba도 해야함
        cout << "ReturnColumnType Set done" << endl;
        string req_json;
        // string res_json;
        tableManager_.generate_req_json(snippet.tablename[0], req_json);
        // my_LBA2PBA(req_json, res_json);
        //  Document resdoc;
        Document reqdoc;
        reqdoc.Parse(req_json.c_str());
        // resdoc.Parse(res_json.c_str());
        // scheduler_.snippetdata.block_info_list = resdoc["RES"]["Chunk List"];
        vector<string> sstfilename;
        // cout << 111 << endl;
        for (int i = 0; i < reqdoc["REQ"]["Chunk List"].Size(); i++) {
          sstfilename.push_back(
              reqdoc["REQ"]["Chunk List"][i]["filename"].GetString());
          // cout << reqdoc["REQ"]["Chunk List"][i]["filename"].GetString() <<
          // endl;
        }
        int count = 1;
        // for (int i = 0; i < scheduler_.snippetdata.block_info_list.Size();
        //      i++) {
        //   scheduler_.threadblocknum.push_back(count);
        //   for (int j = 0; j < scheduler_.snippetdata
        //                           .block_info_list[i][sstfilename[i].c_str()]
        //                           .Size();
        //        j++) {
        //     scheduler_.blockvec.push_back(count);
        //     // cout << count << endl;
        //     count++;
        //   }
        // }
        buff.InitWork(snippet.query_id, snippet.work_id, snippet.tableAlias,
                      snippet.column_alias, snippet.return_datatype,
                      snippet.return_offlen, 0, sstfilename);
        // cout << "[Snippet Manager] Recived Snippet" << snippet.query_id <<
        // "-" << snippet.work_id << endl; cout << "[Snippet Manager] Updating
        // Snippet info ..." << endl; cout << "[Snippet Manager] Get Table Data
        // info from Table Data Manager" << endl; cout << "[Snippet Manager] Get
        // Data Block Address from LBA2PBA Query Agent" << endl;
        //  keti_log("Snippet Manager","Recived Snippet" +
        //  std::to_string(snippet.work_id));
        cout << "Snippet Manager "
             << "Updating Snippet info ..." << endl;
        cout << "Snippet Manager "
             << "Get Table Data info from Table Data Manager" << endl;
        cout << "Snippet Manager"
             << "Get Data Block Address from LBA2PBA Query Agent" << endl;

        // cout << "[Snippet Manager] File SST Size : " <<
        // scheduler_.snippetdata.block_info_list.Size() << endl;
        // cout << "[Snippet Manager] File SST Size : 8"  << endl;
        //이부분 수정 필요
        // vector<string> tmpsstfilelist
        scheduler_.snippetdata.wal_json = snippet.wal_json;
        scheduler_.snippetdata.query_id = snippet.query_id;
        scheduler_.snippetdata.work_id = snippet.work_id;
        scheduler_.snippetdata.table_offset = snippet.table_offset;
        scheduler_.snippetdata.table_offlen = snippet.table_offlen;
        scheduler_.snippetdata.table_filter = snippet.table_filter;
        scheduler_.snippetdata.table_datatype = snippet.table_datatype;
        scheduler_.snippetdata.sstfilelist = sstfilename;
        scheduler_.snippetdata.table_col = snippet.table_col;
        scheduler_.snippetdata.column_filtering = snippet.columnFiltering;
        scheduler_.snippetdata.column_projection = snippet.columnProjection;
        // scheduler_.snippetdata.block_info_list = snippet.block_info_list;
        scheduler_.snippetdata.tablename = snippet.tablename[0];
        scheduler_.snippetdata.returnType = snippet.return_datatype;
        scheduler_.snippetdata.Group_By = snippet.groupBy;
        scheduler_.snippetdata.Order_By = snippet.orderBy;
        // cout << 1 << endl;
        // buff.InitWork(snippet.query_id,snippet.work_id,snippet.tableAlias,snippet.column_alias,snippet.return_datatype,snippet.return_offlen,count);
        // buff.InitWork(snippet.query_id,snippet.work_id,snippet.tableAlias,snippet.column_alias,snippet.return_datatype,snippet.return_offlen,0);
        // cout << 2 << endl;
        boost::thread_group tg;
        // cout << "[Snippet Manager] Send Snippet to Snippet Scheduler" <<
        // endl;
        keti_log("Snippet Manager", "Send Snippet to Snippet Scheduler");

        if (snippet.work_id != 0) {
          buff.GetTableInfo(snippet.query_id, scheduler_.snippetdata.bfalias);
        }

        // cout << snippet.query_id << " " << snippet.work_id << endl;
        // if (snippet.query_id == 3 && snippet.work_id == 0) {
        //   keti_log("Snippet Scheduler", "Scheduling BestCSD ...");
        //   keti_log(
        //       "Snippet Scheduler",
        //       "Scheduling CSD List :  CSD ID : 1, CSD ID : 9, CSD ID : 17,");
        //   keti_log("Snippet Scheduler", " => BestCSD : CSD1");
        //   std::string line;
        //   std::getline(std::cin, line);
        // }

        scheduler_.snippetdata.bfalias = snippet.tableAlias;
        cout << "Send to Snippet.... Thread Size :: " << sstfilename.size()
             << endl;
        scheduler_.SSTFileSize = sstfilename.size();
        for (int i = 0; i < sstfilename.size(); i++) {
          // for (int i = 0; i < 10; i++) {
          // cout << "Send Snippet to " <<
          // csdmanager.getsstincsd(sstfilename[i])
          //      << " -> (FileName :: " << sstfilename[i] << ")" << endl;
          tg.create_thread(
              boost::bind(&Scheduler::sched, &scheduler_, i, csdmanager));
        }
        // for(int i = 0; i < 8; i++){
        //     tg.create_thread(boost::bind(&Scheduler::sched,&scheduler_,i,csdmanager));
        // }
        // std::string line;
        // std::getline(std::cin, line);
        if (snippet.work_id == 0) {
          keti_log("Buffer Manager",
                   "\t\t------------:: STEP 3 ::------------");
        }
        tg.join_all();
        scheduler_.threadblocknum.clear();
        scheduler_.blockvec.clear();

        // get_data_and_filter(snippet,buff);

        // if(snippet.columnProjection.size() > 0){
        //     //어그리게이션 호출
        //     // cout << "start agg" << endl;
        //     Aggregation(snippet,buff,0);
        //     // cout << "end agg" << endl;
        // }
      } else if (snippet.snippetType ==
                 snippetsample::SnippetRequest::AGGREGATION_SNIPPET) {
        cout << "Make Request... Snippet Type :: " << snippet.snippetType
             << endl;
        cout << "Group By Size :: " << snippet.groupBy.size() << endl;
        cout << "Aggregation Size :: " << snippet.columnProjection.size()
             << endl;
        cout << "OrderBy Size :: " << snippet.orderBy.size() << endl;
        if (snippet.groupBy.size() > 0) {
          GroupBy(snippet, buff);
        } else if (snippet.columnProjection.size() > 0) {
          Aggregation(snippet, buff, 1);
        }
        if (snippet.orderBy.size() > 0) {
          OrderBy(snippet, buff);
        }
      }
    }
  }
}

void SnippetManager::ReturnColumnType(SnippetStruct &snippet,
                                      BufferManager &buff) {
  cout << "ReturnColumnType..." << endl;
  unordered_map<string, int> columntype;
  unordered_map<string, int> columnofflen;
  vector<int> return_datatype;
  vector<int> return_offlen;
  bool bufferflag = false;

  cout << "[Snippet Manager] "
       << "Start Get Return Column Type..." << endl;
  cout << "table Count -> " << snippet.tablename.size() << endl;
  if (snippet.tablename.size() > 1) {
    bufferflag = true;
    for (int i = 0; i < snippet.tablename.size(); i++) {
      TableInfo tmpinfo =
          buff.GetTableInfo(snippet.query_id, snippet.tablename[i]);
      for (int j = 0; j < tmpinfo.table_column.size(); j++) {
        columntype.insert(
            make_pair(tmpinfo.table_column[j], tmpinfo.table_datatype[j]));
        columnofflen.insert(
            make_pair(tmpinfo.table_column[j], tmpinfo.table_offlen[j]));
      }
    }
  } else {
    cout << "table Count is " << snippet.tablename.size()
         << " start table check" << endl;
    cout << "CheckTableStatus -> "
         << buff.CheckTableStatus(snippet.query_id, snippet.tablename[0])
         << endl;
    if (buff.CheckTableStatus(snippet.query_id, snippet.tablename[0]) == 3) {
      bufferflag = true;
      TableInfo tmpinfo =
          buff.GetTableInfo(snippet.query_id, snippet.tablename[0]);
      for (int j = 0; j < tmpinfo.table_column.size(); j++) {
        columntype.insert(
            make_pair(tmpinfo.table_column[j], tmpinfo.table_datatype[j]));
        columnofflen.insert(
            make_pair(tmpinfo.table_column[j], tmpinfo.table_offlen[j]));
      }
    } else {
      cout << "table Count is "
           << buff.CheckTableStatus(snippet.query_id, snippet.tablename[0])
           << " start set buffer" << endl;
      for (int i = 0; i < snippet.table_col.size(); i++) {
        columntype.insert(
            make_pair(snippet.table_col[i], snippet.table_datatype[i]));
        columnofflen.insert(
            make_pair(snippet.table_col[i], snippet.table_offlen[i]));
      }
      cout << "set colbuffer Done" << endl;
    }
  }
  cout << "Projection Size " << snippet.columnProjection.size() << endl;
  for (int i = 0; i < snippet.columnProjection.size(); i++) {
    cout << "Real Projection Size " << snippet.columnProjection[i].size()
         << endl;
    for (int j = 1; j < snippet.columnProjection[i].size(); j++) {
      if (snippet.columnProjection[i][0].value == "3" ||
          snippet.columnProjection[i][0].value == "4") {
        return_datatype.push_back(
            static_cast<int>(MySQL_DataType::MySQL_INT32));
        return_offlen.push_back(4);
        break;
      }
      if (snippet.columnProjection[i][j].type == 10) {
        return_datatype.push_back(
            columntype[snippet.columnProjection[i][j].value]);
        return_offlen.push_back(
            columnofflen[snippet.columnProjection[i][j].value]);
        break;
      } else {
        continue;
      }
    }
  }
  snippet.return_datatype = return_datatype;
  snippet.return_offlen = return_offlen;
}

void SnippetManager::GetIndexNumber(string TableName,
                                    vector<int> &IndexNumber) {
  //메타 매니저에 테이블 이름을 주면 인덱스 넘버 리턴
}

bool SnippetManager::GetWALTable(SnippetStruct &snippet) {
  // WAL 매니저에 인덱스 번호를 주면 데이터를 리턴 없으면 false
  vector<int> indexnumber;
  // GetIndexNumber(TableName,indexnumber);
  if (snippet.tablename.size() > 1) {
    return false;
  }
  GetIndexNumber(snippet.tablename[0], indexnumber);
  // wal manager에 인덱스 넘버 데이터 있는지 확인
  return false;
}

int my_LBA2PBA(std::string &req_json, std::string &res_json) {
  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  char buffer[BUFF_SIZE] = {0};
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Socket creation error \n");
    return -1;
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(LBA2PBAPORT);

  // Convert IPv4 and IPv6 addresses from text to binary form
  if (inet_pton(AF_INET, "10.0.5.120", &serv_addr.sin_addr) <=
      0)  // csd 정보를 통해 ip 입력(std::string 타입)
  {
    printf("\nInvalid address/ Address not supported \n");
    return -1;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    printf("\nConnection Failed %s\n", strerror(errno));
    // sql_print_information("connect error %s", strerror(errno));
    return -1;
  }

  // send json
  size_t len = strlen(req_json.c_str());
  send(sock, &len, sizeof(len), 0);
  send(sock, req_json.c_str(), strlen(req_json.c_str()), 0);

  // read(sock,res_json,BUFF_SIZE);

  size_t length;
  read(sock, &length, sizeof(length));

  int numread;
  while (1) {
    if ((numread = read(sock, buffer, BUFF_SIZE - 1)) == -1) {
      perror("read");
      exit(1);
    }
    length -= numread;
    buffer[numread] = '\0';
    res_json += buffer;

    if (length == 0) break;
  }

  ::close(sock);

  return 0;
}

void SnippetStructQueue::enqueue(SnippetStruct tmpsnippet) {
  tmpvec.push_back(tmpsnippet);
}

// SnippetStruct SnippetStructQueue::dequeue(){
//     SnippetStruct ret = tmpvec[queuecount];
//     queuecount++;
//     return ret;
// }

void SnippetStructQueue::initqueue() { queuecount = 0; }

void sendToSnippetScheduler(SnippetStruct &snippet, BufferManager &buff,
                            Scheduler &scheduler_, TableManager &tableManager_,
                            CSDManager &csdManager) {
  //여기는 csd로 내리는 쪽으로
  //여기는 필터값 수정하는 내리는곳 일반적인 것은 위쪽에 있음
  //리턴타입 봐야함
  // lba2pba도 해야함
  cout << "Ready to Send Snippet" << endl;
  string req_json;
  string res_json;
  tableManager_.generate_req_json(snippet.tablename[0], req_json);
  my_LBA2PBA(req_json, res_json);
  Document resdoc;
  Document reqdoc;
  reqdoc.Parse(req_json.c_str());
  resdoc.Parse(res_json.c_str());
  scheduler_.snippetdata.block_info_list = resdoc["RES"]["Chunk List"];

  vector<string> sstfilename;
  for (int i = 0; i < reqdoc["REQ"]["Chunk List"].Size(); i++) {
    sstfilename.push_back(
        reqdoc["REQ"]["Chunk List"][i]["filename"].GetString());
    // cout << reqdoc["REQ"]["Chunk List"][i]["filename"].GetString() << endl;
  }
  int count = 0;
  for (int i = 0; i < scheduler_.snippetdata.block_info_list.Size(); i++) {
    scheduler_.threadblocknum.push_back(count);
    for (int j = 0;
         j < scheduler_.snippetdata.block_info_list[i][sstfilename[i].c_str()]
                 .Size();
         j++) {
      scheduler_.blockvec.push_back(count);
      // cout << count << endl;
    }
  }

  unordered_map<string, vector<vectortype>> tmpdata =
      buff.GetTableData(snippet.query_id, snippet.tablename[1]).table_data;

  //이부분 수정 필요
  scheduler_.snippetdata.query_id = snippet.query_id;
  scheduler_.snippetdata.work_id = snippet.work_id;
  scheduler_.snippetdata.table_offset = snippet.table_offset;
  scheduler_.snippetdata.table_offlen = snippet.table_offlen;

  //필터 수정 해야함
  for (int i = 0; i < snippet.table_filter.size(); i++) {
    if (snippet.table_filter[i].LV.value.size() == 0) {
      continue;
    }
    for (int j = 0; j < snippet.table_filter[i].RV.type.size(); j++) {
      if (snippet.table_filter[i].RV.type[j] != 10) {
        continue;
      }
      if (snippet.table_filter[i].RV.value[j].substr(
              0, snippet.tablename[1].size()) == snippet.tablename[1]) {
        string tmpstring;
        int tmptype;
        if (tmpdata[snippet.table_filter[i].RV.value[j].substr(
                snippet.tablename[1].size() + 1)][0]
                .type == 0) {
          tmpstring = tmpdata[snippet.table_filter[i].RV.value[j].substr(
              snippet.tablename[1].size() + 1)][0]
                          .strvec;
          tmptype = 9;
        } else if (tmpdata[snippet.table_filter[i].RV.value[j].substr(
                       snippet.tablename[1].size() + 1)][0]
                       .type == 1) {
          tmpstring =
              to_string(tmpdata[snippet.table_filter[i].RV.value[j].substr(
                  snippet.tablename[1].size() + 1)][0]
                            .intvec);
          tmptype = 3;
        } else {
          tmpstring =
              to_string(tmpdata[snippet.table_filter[i].RV.value[j].substr(
                  snippet.tablename[1].size() + 1)][0]
                            .floatvec);
          tmptype = 5;
        }
        snippet.table_filter[i].RV.type[j] = tmptype;
        snippet.table_filter[i].RV.value[j] = tmpstring;
        // snippet.table_filter[i].RV.value[j] =
        // tmpdata[snippet.table_filter[i].RV.value[j].substr(snippet.tablename[1].size()
        // + 1)];
      }
    }
  }
  scheduler_.snippetdata.table_filter = snippet.table_filter;

  scheduler_.snippetdata.wal_json = snippet.wal_json;
  scheduler_.snippetdata.table_datatype = snippet.table_datatype;
  scheduler_.snippetdata.sstfilelist = sstfilename;
  scheduler_.snippetdata.table_col = snippet.table_col;
  scheduler_.snippetdata.column_filtering = snippet.columnFiltering;
  scheduler_.snippetdata.column_projection = snippet.columnProjection;
  // scheduler_.snippetdata.block_info_list = snippet.block_info_list;
  scheduler_.snippetdata.tablename = snippet.tablename[0];
  scheduler_.snippetdata.returnType = snippet.return_datatype;
  scheduler_.snippetdata.Group_By = snippet.groupBy;
  scheduler_.snippetdata.Order_By = snippet.orderBy;
  // cout << 1 << endl;
  buff.InitWork(snippet.query_id, snippet.work_id, snippet.tableAlias,
                snippet.column_alias, snippet.return_datatype,
                snippet.return_offlen, count, sstfilename);
  // cout << 2 << endl;
  boost::thread_group tg;
  for (int i = 0; i < sstfilename.size(); i++) {
    tg.create_thread(
        boost::bind(&Scheduler::sched, &scheduler_, i, csdManager));
  }
  tg.join_all();
  scheduler_.threadblocknum.clear();
  scheduler_.blockvec.clear();
  // scheduler_.sched(1);
}