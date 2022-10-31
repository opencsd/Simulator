#pragma once

#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <locale>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "CSDScheduler.hpp"
#include "SnippetManager.hpp"
#include "document.h"
#include "prettywriter.h"
#include "stringbuffer.h"
#include "writer.h"

#define PORT 8080
#define LBA2PBAPORT 8081
#define MAXLINE 256

using namespace rapidjson;
using namespace std;

class EngineModule {
 public:
  TableManager tblManager;
  CSDManager csdmanager;
  Scheduler csdscheduler;
  BufferManager bufma;
  string dirName;
  string jsonPath;
  // SnippetManager snippetmanager(tblManager,csdscheduler,csdmanager);
  SnippetManager snippetmanager;
  EngineModule() {
    csdscheduler.init_scheduler(csdmanager);
    bufma.InitBufferManager(csdscheduler);
  }
  EngineModule(string dirname, string jsonPath) {
    csdscheduler.init_scheduler(csdmanager);
    bufma.InitBufferManager(csdscheduler);
    this->dirName = dirName;
    this->jsonPath = jsonPath;
  }
  void StartEngine();
};