#include "return.hpp"

void Return::ReturnResult() {
  // cout << "<-----------  Return Layer Running...  ----------->\n";
  while (1) {
    MergeResult mergeResult = ReturnQueue.wait_and_pop();
    SendDataToBufferManager(mergeResult);
  }
}

void Return::push_work(MergeResult work) { this->ReturnQueue.push_work(work); }
void Return::SendDataToBufferManager(MergeResult mergeResult) {
  StringBuffer block_buf;
  PrettyWriter<StringBuffer> writer(block_buf);

  writer.StartObject();

  writer.Key("csdName");
  writer.String(mergeResult.csd_name.c_str());

  writer.Key("queryID");
  writer.Int(mergeResult.query_id);
  writer.Key("workID");
  writer.Int(mergeResult.work_id);

  writer.Key("data");
  writer.StartArray();
  for (int i = 0; i < mergeResult.colNames.size(); i++) {
    writer.StartObject();
    writer.Key("colName");
    writer.String(mergeResult.colNames[i].c_str());
    vector<string> tmpData;
    tmpData.assign(mergeResult.data[mergeResult.colNames[i]].begin(),
                   mergeResult.data[mergeResult.colNames[i]].end());
    writer.Key("colData");
    writer.StartArray();
    for (int j = 0; j < tmpData.size(); j++) {
      writer.StartObject();
      writer.Key("strVec");
      writer.String(tmpData[j].c_str());
      writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
  }

  writer.EndObject();
  string block_buf_ = block_buf.GetString();
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