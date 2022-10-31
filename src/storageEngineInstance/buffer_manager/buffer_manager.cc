#include "buffer_manager.hpp"

void getColOffset(const char *row_data, int *col_offset_list,
                  vector<int> return_datatype, vector<int> table_offlen);

int BufferManager::InitBufferManager(Scheduler &scheduler) {
  BufferManager_Input_Thread =
      thread([&]() { BufferManager::BlockBufferInput(); });
  BufferManager_Thread =
      thread([&]() { BufferManager::BufferRunning(scheduler); });
  return 0;
}

int BufferManager::Join() {
  BufferManager_Input_Thread.join();
  BufferManager_Thread.join();
  return 0;
}

void BufferManager::BlockBufferInput() {
  InitSocket(UNIX_SOCKET);
  BindSocket("/data/blockBuffer.sock");
  ListenSocket();
  int server_fd, client_fd;
  int opt = 1;
  struct sockaddr_in serv_addr, client_addr;
  socklen_t addrlen = sizeof(client_addr);
  static char cMsg[] = "ok";

  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }

  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(PORT_BUF);

  if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("bind");
    exit(EXIT_FAILURE);
  }

  if (listen(server_fd, NCONNECTION) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  while (1) {
    if ((client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
                            (socklen_t *)&addrlen)) < 0) {
      perror("accept");
      exit(EXIT_FAILURE);
    }

    std::string json = "";
    int njson;
    size_t ljson;

    recv(client_fd, &ljson, sizeof(ljson), 0);

    char buffer[ljson] = {0};

    while (1) {
      if ((njson = recv(client_fd, buffer, BUFF_SIZE - 1, 0)) == -1) {
        perror("read");
        exit(1);
      }
      ljson -= njson;
      buffer[njson] = '\0';
      json += buffer;

      if (ljson == 0) break;
    }

    BlockResultQueue.push_work(BlockResult(json.c_str()));

    close(client_fd);
  }
  close(server_fd);
}

void BufferManager::BufferRunning(Scheduler &scheduler) {
  while (1) {
    BlockResult blockResult = BlockResultQueue.wait_and_pop();

    // keti_log("Buffer Manager", "Receive Data from CSD Return Interface {ID :
    // " +
    //                              std::to_string(blockResult.work_id) +
    //                            ", SST: " + blockResult.sst_name + "}");

    if ((m_BufferManager.find(blockResult.query_id) == m_BufferManager.end()) ||
        (m_BufferManager[blockResult.query_id]->work_buffer_list.find(
             blockResult.work_id) ==
         m_BufferManager[blockResult.query_id]->work_buffer_list.end())) {
      cout << "<error> Work(" << blockResult.query_id << "-"
           << blockResult.work_id << ") Initialize First!" << endl;
    }

    MergeBlock(blockResult, scheduler);
  }
}

void BufferManager::MergeBlock(BlockResult result, Scheduler &scheduler) {
  int qid = result.query_id;
  int wid = result.work_id;

  Work_Buffer *myWorkBuffer = m_BufferManager[qid]->work_buffer_list[wid];

  unique_lock<mutex> lock(myWorkBuffer->mu);

  if (myWorkBuffer->all_csd_done) {
    cout << "<error> Work(" << qid << "-" << wid << ") Is Already Done!"
         << endl;
    return;
  }
  myWorkBuffer->csd_sst_map[result.csd_name].assign(result.sstList.begin(),
                                                    result.sstList.end());

  for (int i = 0; i < myWorkBuffer->table_column.size(); i++) {
    string colName = myWorkBuffer->table_column[i];
    myWorkBuffer->table_data[colName].insert(
        myWorkBuffer->table_data[colName].end(),
        result.csd_table_data[colName].begin(),
        result.csd_table_data[colName].end());
  }

  myWorkBuffer->left_block_count -= result.sstList.size();
  myWorkBuffer->csdCNT++;
  string tmpstring = "Merging Data {ID : " + std::to_string(wid) +
                     "} Done (CSD : " + result.csd_name +
                     ", Status : " + std::to_string(myWorkBuffer->csdCNT) +
                     " / 8)";
  keti_log("Buffer Manager", tmpstring);
  if (myWorkBuffer->left_block_count == 0 && myWorkBuffer->csdCNT == 8) {
    string tmpstring = "Merging Data Done{ID : " + std::to_string(wid) +
                       "} Done (File : " + result.csd_name +
                       ", Status : " + std::to_string(myWorkBuffer->csdCNT) +
                       " / 8)";
    keti_log("Buffer Manager", tmpstring);
    for (int j = 0; j < myWorkBuffer->table_column.size(); j++) {
      string colName = myWorkBuffer->table_column[j];

      cout << "[Buffer Manager] "
           << "(" << colName << "/" << myWorkBuffer->table_data[colName].size()
           << ")" << endl;
    }
    myWorkBuffer->all_csd_done = true;
    m_BufferManager[qid]->table_status[myWorkBuffer->table_alias].second = true;
    myWorkBuffer->cond.notify_all();
  }
}

int BufferManager::InitWork(int qid, int wid, string table_alias,
                            vector<string> table_column_,
                            vector<int> return_datatype,
                            vector<int> table_offlen_, int total_block_cnt_,
                            vector<string> sstList) {
  if (sstList.size() > 0) {
    // SendCSDReturn(qid, wid, table_alias, table_column_, return_datatype,
    //               table_offlen_, total_block_cnt_);
  }
  if (m_BufferManager.find(qid) == m_BufferManager.end()) {
    InitQuery(qid);
  } else if (m_BufferManager[qid]->work_buffer_list.find(wid) !=
             m_BufferManager[qid]->work_buffer_list.end()) {
    cout << "<error> Work ID Duplicate Error" << endl;
    return -1;
  }

  Work_Buffer *workBuffer =
      new Work_Buffer(table_alias, table_column_, return_datatype,
                      table_offlen_, total_block_cnt_, sstList);

  m_BufferManager[qid]->work_cnt++;
  m_BufferManager[qid]->work_buffer_list[wid] = workBuffer;
  m_BufferManager[qid]->table_status[table_alias] = make_pair(wid, false);

  return 1;
}
void BufferManager::SendCSDReturn(int qid, int wid, string table_alias,
                                  vector<string> table_column_,
                                  vector<int> return_datatype,
                                  vector<int> table_offlen_,
                                  int total_block_cnt_) {
  // int csdPorts[8] = {28080, 28081, 28082, 28083, 28084, 28085, 28086, 28087};
  int csdPorts[8] = {28080, 28080, 28080, 28080, 28080, 28080, 28080, 28080};
  string csdIP = "10.0.5.123";
  StringBuffer snippetbuf;
  snippetbuf.Clear();
  Writer<StringBuffer> writer(snippetbuf);
  writer.StartObject();
  writer.Key("queryID");
  writer.Int(qid);
  writer.Key("workID");
  writer.Int(wid);
  writer.Key("tableAlias");
  writer.String(table_alias.c_str());
  writer.Key("tableCol");
  writer.StartArray();
  for (int i = 0; i < table_column_.size(); i++) {
    writer.String(table_column_[i].c_str());
  }
  writer.EndArray();
  writer.Key("returnType");
  writer.StartArray();
  for (int i = 0; i < return_datatype.size(); i++) {
    writer.Int(return_datatype[i]);
  }
  writer.EndArray();
  writer.Key("tableOfflen");
  writer.StartArray();
  for (int i = 0; i < table_offlen_.size(); i++) {
    writer.Int(table_offlen_[i]);
  }
  writer.EndArray();
  writer.Key("totalBlockCount");
  writer.Int(total_block_cnt_);
  writer.EndObject();
  string returnBuffer = snippetbuf.GetString();
  for (int i = 0; i < 8; i++) {
    if (i > 0) continue;
    int sock;
    struct sockaddr_in serv_addr;
    sock = socket(PF_INET, SOCK_STREAM, 0);
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr(csdIP.c_str());

    serv_addr.sin_port = htons(csdPorts[i]);

    connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

    size_t len = strlen(returnBuffer.c_str());
    send(sock, &len, sizeof(len), 0);
    send(sock, (char *)returnBuffer.c_str(), strlen(returnBuffer.c_str()), 0);
    close(sock);
  }
}

int BufferManager::EndQuery(int qid) {
  if (m_BufferManager.find(qid) == m_BufferManager.end()) {
    cout << "<error> There's No Query ID {" << qid << "}" << endl;
    return 0;
  }
  m_BufferManager.erase(qid);
  return 1;
}

void BufferManager::InitQuery(int qid) {
  Query_Buffer *queryBuffer = new Query_Buffer(qid);
  m_BufferManager.insert(pair<int, Query_Buffer *>(qid, queryBuffer));
}

int BufferManager::CheckTableStatus(int qid, string tname) {
  if (m_BufferManager.find(qid) == m_BufferManager.end()) {
    return static_cast<int>(Work_Status_Type::QueryIDError);
  } else if (m_BufferManager[qid]->table_status.find(tname) ==
             m_BufferManager[qid]->table_status.end()) {
    return static_cast<int>(Work_Status_Type::NonInitTable);
  } else if (!m_BufferManager[qid]->table_status[tname].second) {
    return static_cast<int>(Work_Status_Type::NotFinished);
  } else {
    return static_cast<int>(Work_Status_Type::WorkDone);
  }
}

TableInfo BufferManager::GetTableInfo(int qid, string tname) {
  int status = CheckTableStatus(qid, tname);

  TableInfo tableInfo;

  if (status != static_cast<int>(Work_Status_Type::WorkDone)) {
    int wid = m_BufferManager[qid]->table_status[tname].first;
    Work_Buffer *workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
    unique_lock<mutex> lock(workBuffer->mu);
    workBuffer->cond.wait(lock);
  }

  int wid = m_BufferManager[qid]->table_status[tname].first;
  tableInfo.table_column =
      m_BufferManager[qid]->work_buffer_list[wid]->table_column;
  tableInfo.table_datatype =
      m_BufferManager[qid]->work_buffer_list[wid]->table_datatype;
  tableInfo.table_offlen =
      m_BufferManager[qid]->work_buffer_list[wid]->table_offlen;

  return tableInfo;
}

TableData BufferManager::GetTableData(int qid, string tname) {
  cout << "CheckTableStatus... :: " << CheckTableStatus(qid, tname) << endl;
  int status = CheckTableStatus(qid, tname);
  TableData tableData;

  switch (status) {
    case static_cast<int>(Work_Status_Type::QueryIDError):
    case static_cast<int>(Work_Status_Type::NonInitTable): {
      tableData.valid = false;
      break;
    }
    case static_cast<int>(Work_Status_Type::NotFinished): {
      int wid = m_BufferManager[qid]->table_status[tname].first;
      Work_Buffer *workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
      unique_lock<mutex> lock(workBuffer->mu);
      workBuffer->cond.wait(lock);
      // cout << "here55" << endl;
      tableData.table_data = workBuffer->table_data;
      break;
    }
    case static_cast<int>(Work_Status_Type::WorkDone): {
      int wid = m_BufferManager[qid]->table_status[tname].first;
      Work_Buffer *workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
      unique_lock<mutex> lock(workBuffer->mu);
      tableData.table_data = workBuffer->table_data;
      break;
    }
  }

  return tableData;
}

int BufferManager::SaveTableData(
    int qid, string tname,
    unordered_map<string, vector<vectortype>> table_data_) {
  keti_log("Buffer Manager", "Saved Table, Table Name : " + tname);

  int wid = m_BufferManager[qid]->table_status[tname].first;
  Work_Buffer *workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
  unique_lock<mutex> lock(workBuffer->mu);

  m_BufferManager[qid]->work_buffer_list[wid]->table_data = table_data_;
  m_BufferManager[qid]->work_buffer_list[wid]->all_csd_done = true;
  m_BufferManager[qid]->table_status[tname].second = true;

  return 1;
}

int BufferManager::DeleteTableData(int qid, string tname) {
  int status = CheckTableStatus(qid, tname);
  switch (status) {
    case static_cast<int>(Work_Status_Type::QueryIDError):
    case static_cast<int>(Work_Status_Type::NonInitTable): {
      return -1;
    }
    case static_cast<int>(Work_Status_Type::NotFinished): {
      int wid = m_BufferManager[qid]->table_status[tname].first;
      Work_Buffer *workBuffer = m_BufferManager[qid]->work_buffer_list[wid];

      unique_lock<mutex> lock(workBuffer->mu);
      workBuffer->cond.wait(lock);

      for (auto i : workBuffer->table_data) {
        i.second.clear();
      }
    }
    case static_cast<int>(Work_Status_Type::WorkDone): {
      int wid = m_BufferManager[qid]->table_status[tname].first;
      Work_Buffer *workBuffer = m_BufferManager[qid]->work_buffer_list[wid];
      unique_lock<mutex> lock(workBuffer->mu);

      for (auto i : workBuffer->table_data) {
        i.second.clear();
      }
      return 0;
    }
  }

  return 1;
}

void getColOffset(const char *row_data, int *col_offset_list,
                  vector<int> return_datatype, vector<int> table_offlen) {
  int col_type = 0, col_len = 0, col_offset = 0, new_col_offset = 0, tune = 0;
  int col_count = return_datatype.size();

  for (int i = 0; i < col_count; i++) {
    col_type = return_datatype[i];
    col_len = table_offlen[i];
    new_col_offset = col_offset + tune;
    col_offset += col_len;
    if (col_type == static_cast<int>(MySQL_DataType::MySQL_VARSTRING)) {
      if (col_len < 256) {  // 0~255
        char var_len[1];
        var_len[0] = row_data[new_col_offset];
        uint8_t var_len_ = *((uint8_t *)var_len);
        tune += var_len_ + 1 - col_len;
      } else {  // 0~65535
        char var_len[2];
        var_len[0] = row_data[new_col_offset];
        int new_col_offset_ = new_col_offset + 1;
        var_len[1] = row_data[new_col_offset_];
        uint16_t var_len_ = *((uint16_t *)var_len);
        tune += var_len_ + 2 - col_len;
      }
    }

    col_offset_list[i] = new_col_offset;
  }
}