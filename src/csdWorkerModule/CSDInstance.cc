// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory)
#include <thread>

#include "scan.hpp"

using namespace std;

// ---------------현재 사용하는 인자(compression, cache X)-----------
// bool blocks_maybe_compressed = false;
// bool blocks_definitely_zstd_compressed = false;
// uint32_t read_amp_bytes_per_bit = 0;
// const bool immortal_table = false;
// std::string dev_name = "/dev/sda";
// -----------------------------------------------------------------

int main(int argc, char **argv) {
  // std::string filename = argv[1];
  // const char *file_path = filename.c_str();

  cout << 1 << endl;
  TableManager CSDTableManager;
  cout << 2 << endl;
  CSDTableManager.InitCSDTableManager();
  cout << 3 << endl;
  Return returnManager(CSDTableManager);
  MergeManager mergeManager(&returnManager);
  Filter filterManager(&mergeManager);

  Scan scanManager(CSDTableManager, &filterManager, &mergeManager);
  if (argc > 1) {
    string csdName = argv[1];
    scanManager.SetCSDName(csdName);
  }
  thread ReturnInterface = thread(&Return::ReturnResult, &returnManager);
  thread InputInterface = thread(&Scan::InputSnippet, &scanManager);
  // Scan scan = Scan(CSDTableManager);
  // scan.Scanning();
  thread ScanLayer = thread(&Scan::Scanning, &scanManager);
  thread FilterLayer1 = thread(&Filter::Filtering, &filterManager);
  // thread FilterLayer2 = thread(&Filter::Filtering, Filter());
  // thread FilterLayer3 = thread(&Filter::Filtering, Filter());
  thread MergeLayer = thread(&MergeManager::Merging, &mergeManager);

  ReturnInterface.join();
  InputInterface.join();

  ScanLayer.join();
  FilterLayer1.join();
  // FilterLayer2.join();
  // FilterLayer3.join();
  MergeLayer.join();

  return 0;
}
