// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory)
#include "scan.hpp"

uint64_t kNumInternalBytes_;
const int indexnum_size = 4;
bool index_valid;
bool check;
char origin_index_num[indexnum_size];
int current_block_count;

char sep = 0x03;
char gap = 0x20;
char fin = 0x02;
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= kNumInternalBytes_);
  return Slice(internal_key.data(), internal_key.size() - kNumInternalBytes_);
}

class InternalKey {
 private:
  string rep_;

 public:
  InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }
  Slice user_key() const { return ExtractUserKey(rep_); }
};

void Scan::Scanning() {
  // cout << "<-----------  Scan Layer Running...  ----------->\n";
  while (1) {
    Snippet snippet = ScanQueue.wait_and_pop();

    string file_path = "/data/" + snippet.sstfilename;  //여기 수정
    // cout << "*****file_path: " << file_path << endl;
    string file_name = file_path.substr(6);
    kNumInternalBytes_ = snippet.kNumInternalBytes;

    Options options;
    SstFileReader sstFileReader(options);

    FilterInfo filterInfo(snippet.table_col, snippet.column_projection,
                          snippet.projection_datatype, snippet.groupby_col,
                          snippet.table_filter);
    Result scanResult(snippet.query_id, snippet.work_id, snippet.csd_name,
                      snippet.table_name, file_name, filterInfo);

    current_block_count = 0;
    check = true;
    index_valid = true;

    // if(snippet.is_inserted){
    //   WalScan(&snippet, &scanResult);
    //   EnQueueData(scanResult, snippet.scan_type);
    //   scanResult.InitResult();
    // }

    ScanFile(&sstFileReader, &snippet, &scanResult, file_path);
    cout << "[CSD Scan] Scan Data Done ... (FileName : " << scanResult.sst_name
         << ")" << endl;
    // cout << "*****row_count: " << row_count << endl;
    EnQueueData(scanResult, snippet.scan_type);
    scanResult.InitResult();
  }
}

void Scan::push_work(Snippet parsedSnippet) {
  ScanQueue.push_work(parsedSnippet);
}

void Scan::ScanFile(SstFileReader* sstBlockReader_, Snippet* snippet_,
                    Result* scan_result, string file_path) {
  Status s = sstBlockReader_->Open(file_path);
  if (!s.ok()) {
    cout << "open error : errorcode( " << s.code() << "-" << s.subcode() << ")"
         << endl;
  }

  cout << file_path << " read Complete" << endl;

  const char* ikey_data;
  const char* row_data;
  size_t row_size;
  int block_log_cnt = 0;
  Iterator* datablock_iter = sstBlockReader_->NewIterator(ReadOptions());
  vector<string> colList = CSDTableManager_.GetSchemaList(snippet_->table_name);
  int totalRows =
      CSDTableManager_.GetTableRow(snippet_->sstfilename, snippet_->table_name);
  bool IsFirstRow = true;

  for (datablock_iter->SeekToFirst(); datablock_iter->Valid();
       datablock_iter->Next()) {  // iterator first부터 순회
    // cout << "[CSD Scan] Scanning Data... (RowCNT : " << row_count << ")"
    //      << endl;
    Status s = datablock_iter->status();
    if (!s.ok()) {
      cout << "Error reading the block - Skipped \n";
      break;
    }
    const Slice& key = datablock_iter->key();
    const Slice& value = datablock_iter->value();

    InternalKey ikey;
    ikey.DecodeFrom(key);
    ikey_data = ikey.user_key().data();
    string ikey_string = ikey.user_key().ToString(true);
    row_data = value.data();
    row_size = value.size();

    // cout << "KEY DATA : " << ikey_string << endl;
    if (ikey_string != snippet_->table_hex_key) {
      continue;
    } else if (ikey_string == snippet_->table_hex_key && IsFirstRow) {
      cout << "Row Size :: " << totalRows << endl;
      scan_result->row_count = totalRows;
      for (int i = 0; i < colList.size(); i++) {
        string colName = colList[i];
        scan_result->data[colName].reserve(totalRows);
      }
      IsFirstRow = false;
    }
    for (int i = 0; i < colList.size(); i++) {
      int schemaType =
          CSDTableManager_.GetSchemaType(snippet_->table_name, colList[i]);

      int schemaLength =
          CSDTableManager_.GetSchemaLength(snippet_->table_name, colList[i]);

      if (schemaType == 15) {
        if (schemaLength > 255) {
          schemaLength = static_cast<int>(((uint8_t)row_data[0]) |
                                          ((uint8_t)row_data[1] << 8));
        } else {
          schemaLength = static_cast<int>(((uint8_t)row_data[0]));
        }
      }

      typeVar l_orderkey(schemaType, row_data, schemaLength);
      row_data = row_data + schemaLength;
    }
  }
}
void Scan::EnQueueData(Result scan_result, int scan_type) {
  if (scan_type == Full_Scan_Filter) {
    filterManager->push_work(scan_result);
  } else if (scan_type == Full_Scan) {
    mergeManager->push_work(scan_result);
  }
}

void Scan::InputSnippet() {
  int server_fd;
  int client_fd;
  int opt = 1;
  struct sockaddr_in serv_addr;  // 소켓주소
  struct sockaddr_in client_addr;
  socklen_t addrlen = sizeof(struct sockaddr_in);
  cout << "Set Input Socket..." << endl;
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    exit(EXIT_FAILURE);
  }

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt");
    exit(EXIT_FAILURE);
  }
  cout << "Set Socket Done" << endl;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(INPUT_IF_PORT);  // port
  cout << "Socket Info => " << inet_ntoa(serv_addr.sin_addr) << ":"
       << INPUT_IF_PORT << endl;

  if (bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("bind");
    exit(EXIT_FAILURE);
  }  // 소켓을 지정 주소와 포트에 바인딩

  if (listen(server_fd, 3) < 0) {
    perror("listen");
    exit(EXIT_FAILURE);
  }  // 리스닝
  cout << "Socket Listen :: " << inet_ntoa(serv_addr.sin_addr) << ":"
       << INPUT_IF_PORT << endl;
  while (1) {
    if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr,
                            (socklen_t*)&addrlen)) < 0) {
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
    // cout << json.c_str() << endl;
    Snippet parsedSnippet(json.c_str());

    //로그
    printf(
        "\n[CSD Input Interface] Recieve Snippet {ID : %d-%d} from Storage "
        "Engine Instance\n",
        parsedSnippet.query_id, parsedSnippet.work_id);

    ScanQueue.push_work(parsedSnippet);

    close(client_fd);
  }

  close(server_fd);
}
