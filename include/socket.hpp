#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

#include "logger.hpp"
#define TCP_SOCKET AF_INET
#define UNIX_SOCKET AF_UNIX

using namespace std;

class Socket {
 private:
  int server_fd;

  int opt = 1;
  int domain;
  struct sockaddr_in serv_addr;  // 소켓주소
  struct sockaddr_in client_addr;
  struct sockaddr_un local, remote;
  socklen_t addrlen;
  Logger logger;

 public:
  int client_fd;
  Socket(int __domain);
  Socket();
  bool SetSocket();
  int BindSocket(string socketPath);
  int BindSocket(uint16_t port);
  int ListenSocket();
  virtual void Accept(int client_fd);
};