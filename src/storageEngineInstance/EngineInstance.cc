#include "StorageEngineInputInterface.hpp"

int main(int argc, char const *argv[]) {
  EngineModule em;

  // 1: snippet directory path
  // 2: tablemanager.json path
  if (argc > 2) {
    em.testrun(argv[1], argv[2]);
    return 0;
  } else if (argc > 1) {
    em.RunServer(argv[1]);
    return 0;
  } else {
    return 1;
  }

  return 0;
}
