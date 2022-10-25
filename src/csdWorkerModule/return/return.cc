#include "return.hpp"

void Return::ReturnResult() {
  // cout << "<-----------  Return Layer Running...  ----------->\n";
  while (1) {
    MergeResult mergeResult = ReturnQueue.wait_and_pop();
    MergeCast(mergeResult);
  }
}
void Return::getColOffset(const char *row_data, int *col_offset_list,
                          vector<int> return_datatype,
                          vector<int> table_offlen) {
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
void Return::MergeCast(MergeResult work) {
  cout << "[CSD Return] Start Cast... (FileName : "
       << returnBuffer.result.sst_name << ")" << endl;
  if (returnBuffer.justInit) {
    vector<string> sstList_ =
        tManager.GetTableRep(work.csd_name, work.tableName);
    returnBuffer.sstList.assign(sstList_.begin(), sstList_.end());
    returnBuffer.justInit = false;
    for (int i = 0; i < returnBuffer.sstList.size(); i++) {
      returnBuffer.left_block_count[returnBuffer.sstList[i]] = -1;
      returnBuffer.is_done[returnBuffer.sstList[i]] = false;
    }
  }
  returnBuffer.result = work;
  if (returnBuffer.result.length != 0) {
    int col_type, col_offset, col_len, origin_row_len, new_row_len,
        col_count = 0;
    string col_name;
    vector<int> new_row_offset;
    new_row_offset.assign(returnBuffer.result.row_offset.begin(),
                          returnBuffer.result.row_offset.end());
    new_row_offset.emplace_back(returnBuffer.result.length);

    for (int i = 0; i < returnBuffer.result.row_count; i++) {
      cout << "[CSD Return] Casting Data... (RowCNT : " << i << ")" << endl;
      origin_row_len = new_row_offset[i + 1] - new_row_offset[i];
      char row_data[origin_row_len];
      memcpy(
          row_data,
          returnBuffer.result.data.c_str() + returnBuffer.result.row_offset[i],
          origin_row_len);

      new_row_len = 0;
      col_count = returnBuffer.table_column.size();
      int col_offset_list[col_count + 1];

      getColOffset(row_data, col_offset_list, returnBuffer.return_datatype,
                   returnBuffer.table_offlen);
      col_offset_list[col_count] = origin_row_len;

      for (int j = 0; j < returnBuffer.table_column.size(); j++) {
        col_name = returnBuffer.table_column[j];
        col_offset = col_offset_list[j];
        col_len = col_offset_list[j + 1] - col_offset_list[j];
        col_type = returnBuffer.return_datatype[j];

        vectortype_r tmpvect;
        switch (col_type) {
          case static_cast<int>(MySQL_DataType::MySQL_BYTE): {
            char tempbuf[col_len];
            memcpy(tempbuf, row_data + col_offset, col_len);
            int64_t my_value = *((int8_t *)tempbuf);
            tmpvect.type = 1;
            tmpvect.intvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);

            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_INT16): {
            char tempbuf[col_len];
            memcpy(tempbuf, row_data + col_offset, col_len);
            int64_t my_value = *((int16_t *)tempbuf);
            tmpvect.type = 1;
            tmpvect.intvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_INT32): {
            char tempbuf[col_len];
            memcpy(tempbuf, row_data + col_offset, col_len);
            int64_t my_value = *((int32_t *)tempbuf);
            tmpvect.type = 1;
            tmpvect.intvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_INT64): {
            char tempbuf[col_len];
            memcpy(tempbuf, row_data + col_offset, col_len);
            int64_t my_value = *((int64_t *)tempbuf);
            tmpvect.type = 1;
            tmpvect.intvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_FLOAT32): {
            //아직 처리X
            char tempbuf[col_len];  // col_len = 4
            memcpy(tempbuf, row_data + col_offset, col_len);
            double my_value = *((float *)tempbuf);
            tmpvect.type = 2;
            tmpvect.floatvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_DOUBLE): {
            //아직 처리X
            char tempbuf[col_len];  // col_len = 8
            memcpy(tempbuf, row_data + col_offset, col_len);
            double my_value = *((double *)tempbuf);
            tmpvect.type = 2;
            tmpvect.floatvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_NEWDECIMAL): {
            // decimal(15,2)만 고려한 상황 -> col_len = 7 (integer:6/real:1)
            char tempbuf[col_len];  // col_len = 7
            memcpy(tempbuf, row_data + col_offset, col_len);
            bool is_negative = false;
            if (std::bitset<8>(tempbuf[0])[7] == 0) {
              is_negative = true;
              for (int i = 0; i < 7; i++) {
                tempbuf[i] = ~tempbuf[i];
              }
            }
            char integer[8];
            char real[1];
            memset(&integer, 0, 8);
            for (int k = 0; k < 4; k++) {
              integer[k] = tempbuf[5 - k];
            }
            real[0] = tempbuf[6];
            int64_t ivalue = *((int64_t *)integer);
            double rvalue = *((int8_t *)real);
            rvalue *= 0.01;
            double my_value = ivalue + rvalue;
            if (is_negative) {
              my_value *= -1;
            }
            tmpvect.type = 2;
            tmpvect.floatvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_DATE): {
            char tempbuf[col_len + 1];
            memcpy(tempbuf, row_data + col_offset, col_len);
            tempbuf[3] = 0x00;
            int64_t my_value = *((int *)tempbuf);
            tmpvect.type = 1;
            tmpvect.intvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_TIMESTAMP): {
            //아직 처리X
            char tempbuf[col_len];
            memcpy(tempbuf, row_data + col_offset, col_len);
            int my_value = *((int *)tempbuf);
            tmpvect.type = 1;
            tmpvect.intvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          case static_cast<int>(MySQL_DataType::MySQL_STRING):
          case static_cast<int>(MySQL_DataType::MySQL_VARSTRING): {
            char tempbuf[col_len + 1];
            memcpy(tempbuf, row_data + col_offset, col_len);
            tempbuf[col_len] = '\0';
            string my_value(tempbuf);
            tmpvect.type = 0;
            tmpvect.strvec = my_value;
            returnBuffer.table_data[col_name].emplace_back(tmpvect);
            break;
          }
          default: {
            cout << "<error> Type:" << col_type << " Is Not Defined!!" << endl;
          }
        }
      }
    }
  }
  returnBuffer.fileCNT++;
  string tmpstring =
      "Merging Data {ID : " + std::to_string(returnBuffer.result.work_id) +
      "} Done (File : " + returnBuffer.result.sst_name +
      ", Status : " + std::to_string(returnBuffer.fileCNT) + " / " +
      std::to_string(returnBuffer.sstList.size()) + ")";
  cout << "[Return Manager] " << tmpstring << endl;

  returnBuffer.is_done[returnBuffer.result.sst_name] = true;
  if (returnBuffer.sstList.size() == returnBuffer.fileCNT) {
    string tmpstring = "Merging Data Done{ID : " +
                       std::to_string(returnBuffer.result.work_id) +
                       "} Done (File : " + returnBuffer.result.sst_name +
                       ", Status : " + std::to_string(returnBuffer.fileCNT) +
                       " / " + std::to_string(returnBuffer.sstList.size()) +
                       ")";
    cout << "[Return Manager] " << tmpstring << endl;
    for (int j = 0; j < returnBuffer.table_column.size(); j++) {
      string colName = returnBuffer.table_column[j];

      cout << "[Buffer Manager] "
           << "(" << colName << "/" << returnBuffer.table_data[colName].size()
           << ")" << endl;
    }
    returnBuffer.all_sst_done = true;
    SendDataToBufferManager();
  }
}
void Return::InitBuffer() {
  int server_fd;
  int client_fd;
  int opt = 1;
  struct sockaddr_in serv_addr;  // 소켓주소
  struct sockaddr_in client_addr;
  socklen_t addrlen = sizeof(struct sockaddr_in);
  cout << "Set Return Socket..." << endl;
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }
  cout << "Set Return Socket Done" << endl;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(28080);  // port
  cout << "Return Socket Info => " << inet_ntoa(serv_addr.sin_addr) << ":"
       << 28080 << endl;

  if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("bind");
    exit(EXIT_FAILURE);
  }  // 소켓을 지정 주소와 포트에 바인딩

  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }  // 리스닝
  cout << "Return Socket Listen :: " << inet_ntoa(serv_addr.sin_addr) << ":"
       << 28080 << endl;
  while (1) {
    if ((client_fd = accept(server_fd, (struct sockaddr *)&client_addr,
                            (socklen_t *)&addrlen)) < 0) {
      perror("accept");
      exit(EXIT_FAILURE);
    }
    // printf("Reading from client\n");

    std::string json;
    char buffer[BUFF_SIZE] = {0};

    size_t length;
    read(client_fd, &length, sizeof(length));

    int numread;
    while (1) {
      if ((numread = read(client_fd, buffer, BUFF_SIZE - 1)) == -1) {
        cout << "read error" << endl;
        perror("read");
        exit(1);
      }
      length -= numread;
      buffer[numread] = '\0';
      json += buffer;

      if (length == 0) break;
    }

    ReturnBuffer paredBuffer(json.c_str());
    this->returnBuffer = paredBuffer;

    //로그
    cout << "\n[CSD Return Interface] Buffer Info Receive " << endl;

    close(client_fd);

    ReturnResult();
  }

  close(server_fd);
}
void Return::push_work(MergeResult work) { this->ReturnQueue.push_work(work); }
void Return::SendDataToBufferManager() {
  StringBuffer block_buf;
  PrettyWriter<StringBuffer> writer(block_buf);

  writer.StartObject();

  writer.Key("csdName");
  writer.String(returnBuffer.result.csd_name.c_str());

  writer.Key("queryID");
  writer.Int(returnBuffer.result.query_id);
  writer.Key("workID");
  writer.Int(returnBuffer.result.work_id);

  writer.Key("data");
  writer.StartArray();
  for (int i = 0; i < returnBuffer.table_column.size(); i++) {
    writer.StartObject();
    writer.Key("colName");
    writer.String(returnBuffer.table_column[i].c_str());
    vector<vectortype_r> tmpData;
    tmpData.assign(
        returnBuffer.table_data.at(returnBuffer.table_column[i]).begin(),
        returnBuffer.table_data.at(returnBuffer.table_column[i]).end());
    writer.Key("colData");
    writer.StartArray();
    for (int j = 0; j < tmpData.size(); j++) {
      writer.StartObject();
      writer.Key("strVec");
      writer.String(tmpData[j].strvec.c_str());
      writer.Key("intVec");
      writer.Int64(tmpData[j].intvec);
      writer.Key("floatVec");
      writer.Double(tmpData[j].floatvec);
      writer.Key("type");
      writer.Int(tmpData[j].type);
      writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
  }
  writer.EndArray();
  writer.Key("sstList");
  writer.StartArray();
  for (int i = 0; i < returnBuffer.sstList.size(); i++) {
    writer.String(returnBuffer.sstList[i].c_str());
  }
  writer.EndArray();
  string block_buf_ = block_buf.GetString();
  writer.EndObject();
  int sockfd;
  struct sockaddr_in serv_addr;
  sockfd = socket(PF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror("ERROR opening socket");
  }
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(BUFF_M_IP);
  serv_addr.sin_port = htons(BUFF_M_PORT);

  connect(sockfd, (const sockaddr *)&serv_addr, sizeof(serv_addr));

  size_t len = strlen(block_buf_.c_str());
  send(sockfd, &len, sizeof(len), 0);
  send(sockfd, (char *)block_buf_.c_str(), len, 0);

  close(sockfd);
}