#include "socket.hpp"

Socket::Socket(int __domain) {
  domain = __domain;
  addrlen = sizeof(struct sockaddr_in);
  if ((server_fd = socket(__domain, SOCK_STREAM, 0)) <= 0) {
    logger.fatal("socket failed");
    exit(EXIT_FAILURE);
  }
}
Socket::Socket() {}
bool Socket::SetSocket() {}
int Socket::BindSocket(string socketPath) {
  local.sun_family = AF_UNIX;
  strcpy(local.sun_path, socketPath.c_str());
  unlink(local.sun_path);
  addrlen = strlen(local.sun_path) + sizeof(local.sun_family);
  if (bind(server_fd, (struct sockaddr*)&local, addrlen) != 0) {
    logger.fatal("Error on binding socket");
    exit(EXIT_FAILURE);
  }
}
int Socket::BindSocket(uint16_t port) {
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(port);  // port
  if (bind(server_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    logger.fatal("Error on binding socket");
    exit(EXIT_FAILURE);
  }  // 소켓을 지정 주소와 포트에 바인딩
}
int Socket::ListenSocket() {
  switch (domain) {
    case TCP_SOCKET: {
      if (listen(server_fd, 3) < 0) {
        logger.fatal("Error on listen call \n");
        exit(EXIT_FAILURE);
      }  // 리스닝
      break;
    }
    case UNIX_SOCKET: {
      if (listen(server_fd, 5) != 0) {
        logger.fatal("Error on listen call \n");
        exit(EXIT_FAILURE);
      }  // 리스닝
      break;
    }
  }
  logger.info("Socket listen");
  while (1) {
    switch (domain) {
      case TCP_SOCKET: {
        if ((client_fd = accept(server_fd, (struct sockaddr*)&client_addr,
                                (socklen_t*)&addrlen)) < 0) {
          logger.fatal("Error on accept() call \n");
          exit(EXIT_FAILURE);
        }
        break;
      }
      case UNIX_SOCKET: {
        if ((client_fd = accept(server_fd, (struct sockaddr*)&remote,
                                (socklen_t*)&addrlen)) < 0) {
          logger.fatal("Error on accept() call \n");
          exit(EXIT_FAILURE);
        }
        break;
      }
    }

    Accept(client_fd);
    close(client_fd);
  }

  close(server_fd);
}
