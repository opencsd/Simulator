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

class Scan {
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

 private:
  TableManager CSDTableManager_;
  WorkQueue<Snippet> ScanQueue;
  Filter *filterManager;
  MergeManager *mergeManager;
  unordered_map<string, int> newoffmap;
  unordered_map<string, int> newlenmap;
  // int dev_fd;
};
