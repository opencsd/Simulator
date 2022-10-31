#pragma once
#include <sys/stat.h>

#include <algorithm>
#include <bitset>
#include <iomanip>

#include "filter.hpp"
#include "rocksdb/cache.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "socket.hpp"
#define CSD_NAME_ENV "CSDNAME"
class Scan : public Socket {
 public:
  Scan(TableManager table_m, Filter *filterManager,
       MergeManager *mergeManager) {
    this->CSDTableManager_ = table_m;
    this->filterManager = filterManager;
    this->mergeManager = mergeManager;
  }

  void Scanning();
  void ScanFile(SstFileReader *sstBlockReader_, Snippet *snippet_,
                Result *scan_result, string file_path);
  void EnQueueData(Result scan_result, int scan_type);
  void push_work(Snippet parsedSnippet);
  void InputSnippet();
  void SetCSDName(string csdName) { this->csdName = csdName; }
  virtual void Accept(int client_fd) override {
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
    logger.info(
        "[CSD Input Interface] Recieve Snippet {ID : %d-%d} from Storage "
        "Engine Instance",
        parsedSnippet.query_id, parsedSnippet.work_id);

    ScanQueue.push_work(parsedSnippet);
  }

 private:
  TableManager CSDTableManager_;
  WorkQueue<Snippet> ScanQueue;
  Filter *filterManager;
  MergeManager *mergeManager;
  unordered_map<string, int> newoffmap;
  unordered_map<string, int> newlenmap;
  string csdName;
  // int dev_fd;
};
