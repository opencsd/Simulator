#include "CSDManager.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "document.h"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

using namespace rapidjson;
using namespace std;

void CSDManager::CSDManagerInit() {
  cout << "CSD Manager Init" << endl;
  Document document;

  int json_fd;
  string json = "";
  string csdName[8] = {"csd1", "csd2", "csd3", "csd4",
                       "csd5", "csd6", "csd7", "csd8"};
  // string csdIP[8] = {"18080", "18080", "18080", "18080",
  //                    "18080", "18080", "18080", "18080"};
  char buf;

  const char* tmp = getenv(CSD_ENV);
  string filePath(tmp ? tmp : "");
  if (filePath.empty()) {
    logger.info("CSD_ENV NOT found!");
    filePath = "/root/workspace/KETI-Pushdown-Process-Container/asset/";
  }
  filePath += CSD_GEN;
  if ((json_fd = open(filePath.c_str(), O_RDONLY)) < 0) {
    fprintf(stderr, "open error for %s\n", "NewTableManager.json");
    exit(1);
  }
  int i = 0;
  int res;

  while (1) {
    if (read(json_fd, &buf, 1) > 0)
      json += buf;
    else
      break;
  }
  close(json_fd);
  document.Parse(json.c_str());
  for (int i = 0; i < 8; i++) {
    Value& csdList = document[csdName[i].c_str()];
    CSDInfo initinfo;
    // initinfo.CSDIP = "10.0.5.123+10.0.5.123:" + csdIP[i];
    initinfo.CSDIP = "/data/" + csdName[i] + ".sock";
    auto sst = csdList["SSTList"].GetArray();
    for (auto it = sst.Begin(); it != sst.End(); it++) {
      string value = it->GetString();
      initinfo.SSTList.emplace_back(value);
    }
    CSD_Map_.insert(make_pair(csdName[i], initinfo));
  }
}

CSDInfo CSDManager::getCSDInfo(string CSDID) { return CSD_Map_[CSDID]; }

vector<string> CSDManager::getCSDIDs() {
  vector<string> ret;
  for (auto i = CSD_Map_.begin(); i != CSD_Map_.end(); i++) {
    pair<string, CSDInfo> tmppair;
    tmppair = *i;
    ret.push_back(tmppair.first);
  }
  return ret;
}

string CSDManager::getsstincsd(string sstname) {
  for (auto it = CSD_Map_.begin(); it != CSD_Map_.end(); it++) {
    pair<string, CSDInfo> tmpp = *it;
    for (int i = 0; i < tmpp.second.SSTList.size(); i++) {
      if (sstname == tmpp.second.SSTList[i]) {
        return tmpp.first;
      }
    }
  }
  return "";
}