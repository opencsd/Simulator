#ifndef __LOGGER_H__
#define __LOGGER_H__ 1
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#define LOG_LEVEL_OFF 0
#define LOG_LEVEL_FATAL 10
#define LOG_LEVEL_ERROR 20
#define LOG_LEVEL_WARN 30
#define LOG_LEVEL_INFO 40
#define LOG_LEVEL_DEBUG 50
#define LOG_LEVEL_TRACE 60
#define LOG_LEVEL_ALL 100
#define fatal(str, ...) writeLog(__FILE__, __LINE__, LOG_LEVEL_FATAL, str)
#define error(str, ...) writeLog(__FILE__, __LINE__, LOG_LEVEL_ERROR, str)
#define warn(str, ...) writeLog(__FILE__, __LINE__, LOG_LEVEL_WARN, str)
#define info(str, ...) writeLog(__FILE__, __LINE__, LOG_LEVEL_INFO, str)
#define debug(str, ...) writeLog(__FILE__, __LINE__, LOG_LEVEL_DEBUG, str)
#define trace(str, ...) writeLog(__FILE__, __LINE__, LOG_LEVEL_TRACE, str)
using namespace std;
class Logger {
 private:
  int logLevel;
  bool isOutput;
  string getTimestamp();

 public:
  Logger();
  Logger(int level);
  void writeLog(const char *fileName, int line, int level, const char *str);
};
static Logger log;
#endif