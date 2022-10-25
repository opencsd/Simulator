#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>

#include "document.h"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

using namespace std;

struct CSDInfo {
  string CSDIP;
  string CSDReplica;
  int CSDWorkingBlock;
  vector<string> SSTList;
  vector<string> CSDList;
};

class CSDManager {
 public:
  void CSDManagerInit();
  CSDManager() { CSDManagerInit(); }
  void CSDBlockDesc(string id, int num);
  CSDInfo getCSDInfo(string CSDID);
  vector<string> getCSDIDs();
  string getsstincsd(string sstname);

 private:
  unordered_map<string, CSDInfo> CSD_Map_;
};