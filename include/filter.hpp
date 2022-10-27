#pragma once
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <vector>

#include "merge_manager.hpp"

struct ScanResult;
struct FilterResult;

typedef enum opertype {
  GE = 1,   // >=
  LE,       // <=
  GT,       // >
  LT,       // <
  ET,       // ==
  NE,       // !=
  LIKE,     // RV로 스트링
  BETWEEN,  // RV로 배열형식 [10,20] OR [COL1,20] --> extra
  IN,       // RV로 배열형식 [10,20,30,40] + 크기 --> extra
  IS,       // IS 와 IS NOT을 구분 RV는 무조건 NULL
  ISNOT,    // IS와 구분 필요 RV는 무조건 NULL
  NOT,  // ISNOT과 관련 없음 OPERATOR 앞에 붙는 형식 --> 혼자 들어오는 oper
  AND,  // AND --> 혼자 들어오는 oper
  OR,   // OR --> 혼자 들어오는 oper
  SUBSTRING = 16,
} opertype;

class Filter {
 private:
  MergeManager *mergeManager;
  WorkQueue<Result> FilterQueue;

 public:
  Filter(MergeManager *mergeManager) { this->mergeManager = mergeManager; }
  void push_work(Result filterResult);
  vector<string> column_filter;
  unordered_map<string, int> newstartptr;
  unordered_map<string, int> newlengthraw;
  unordered_map<string, string> joinmap;
  char data[BUFF_SIZE];
  struct RowFilterData {
    vector<string> forFilterColumn;
    vector<opertype> forFilterOperators;
    vector<string> forFilterValue;
    vector<string> forFilterColumn2;
    vector<int> startoff;
    vector<int> offlen;
    vector<int> datatype;
    vector<string> ColName;
    vector<int> varcharlist;

    unordered_map<string, int> ColIndexmap;
    int offsetcount;
    int rowoffset;
    char *rowbuf;
  };

  RowFilterData rowfilterdata;
  int BlockFilter(Result &scanResult);  //*
  void Filtering();
  void SavedRow(char *row, int startoff, Result &filterresult,
                int nowlength);  //*
  vector<string> split(string str, char Delimiter);
  bool LikeSubString(string lv, string rv);
  bool LikeSubString_v2(string lv, string rv);
  bool InOperator(string lv, Value &rv, unordered_map<string, int> typedata,
                  char *rowbuf);
  bool InOperator(int lv, Value &rv, unordered_map<string, int> typedata,
                  char *rowbuf);
  bool BetweenOperator(int lv, int rv1, int rv2);
  bool BetweenOperator(string lv, string rv1, string rv2);
  bool IsOperator(string lv, char *nonnullbit, int isnot);
  bool isvarc(vector<int> datatype, int ColNum, vector<int> &varcharlist);
  void makedefaultmap(vector<string> ColName, vector<int> startoff,
                      vector<int> offlen, vector<int> datatype, int ColNum,
                      unordered_map<string, int> &startptr,
                      unordered_map<string, int> &lengthRaw,
                      unordered_map<string, int> &typedata);
  void makenewmap(int isvarchar, int ColNum,
                  unordered_map<string, int> &newstartptr,
                  unordered_map<string, int> &newlengthraw,
                  vector<int> datatype, unordered_map<string, int> lengthRaw,
                  vector<string> ColName, int &iter, vector<int> startoff,
                  vector<int> offlen, char *rowbuf);
  void compareGE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareGE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareLE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareLE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareGT(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareGT(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareLT(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareLT(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareET(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareET(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareNE(string LV, string RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  void compareNE(int LV, int RV, bool &CV, bool &TmpV, bool &canSaved,
                 bool isnot);
  int typeLittle(unordered_map<string, int> typedata, string colname,
                 char *rowbuf);
  void JoinOperator(string colname);
  string ItoDec(int inum);
  string typeBig(string colname, char *rowbuf);
  string typeDecimal(string colname, char *rowbuf);
  void sendfilterresult(Result &filterresult);
  void GetColumnoff(string ColName);

  int row_offset;
};
