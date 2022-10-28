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

// Include boost thread
#include <google/protobuf/empty.pb.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "CSDScheduler.hpp"
#include "SnippetManager.hpp"
#include "WalManager.hpp"
#include "document.h"
#include "logger.hpp"
#include "prettywriter.h"
#include "socket.hpp"
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
  // SnippetManager snippetmanager(tblManager,csdscheduler,csdmanager);
  SnippetManager snippetmanager;
  EngineModule() {
    csdscheduler.init_scheduler(csdmanager);
    bufma.InitBufferManager(csdscheduler);
  }
  void StartEngine();

 private:
  Logger logger;
  Socket socketHandler;
};