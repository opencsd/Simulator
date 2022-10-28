#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>

#include "document.h"
#include "logger.hpp"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

#define CSD_GEN "GeneratedCSD.json"
#define CSD_ENV "CSDGENPATH"

using namespace std;

struct CSDInfo {
  string CSDIP;
  vector<string> SSTList;
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
  Logger logger;
};