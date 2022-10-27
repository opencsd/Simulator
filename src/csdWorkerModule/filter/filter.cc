#include "filter.hpp"

int rownum = 0;
// int test = 0;
int saverowcount = 0;

void Filter::push_work(Result filterResult) {
  FilterQueue.push_work(filterResult);
}
void Filter::Filtering() {
  int cnt = 0;
  while (1) {
    Result scanResult = FilterQueue.wait_and_pop();

    BlockFilter(scanResult);
  }
}

int Filter::BlockFilter(Result &scanResult) {
  cout << "[CSD Filter] Start Filtering... (FileName : " << scanResult.sst_name
       << ")" << endl;

  unordered_map<string, int> startptr;
  unordered_map<string, int> lengthRaw;
  unordered_map<string, int> typedata;

  Result filterresult(scanResult.query_id, scanResult.work_id,
                      scanResult.csd_name, scanResult.table_name,
                      scanResult.sst_name, scanResult.filter_info);

  filterresult.row_count = scanResult.row_count;
  for (int i = 0; i < filterresult.filter_info.columnAlias.size(); i++) {
    filterresult.filter_info.mergedData[filterresult.filter_info.columnAlias[i]]
        .reserve(scanResult.row_count);
  }

  for (int i = 0; i < filterresult.row_count; i++) {
    for (int j = 0; j < filterresult.filter_info.table_filter.size(); j++) {
      switch (filterresult.filter_info.table_filter[j].FilterOperator) {
        case GE: {
          if (filterresult.filter_info.table_filter[j].LeftValue.isColumn[0]) {
            if (filterresult.filter_info.table_filter[j]
                    .RightValue.isColumn[0]) {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) >=
                  atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .RightValue.value[0]][i]
                           .getVal()
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            } else {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) >=
                  atof(filterresult.filter_info.table_filter[j]
                           .RightValue.value[0]
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            }
          }
          break;
        }
        case LE: {
          if (filterresult.filter_info.table_filter[j].LeftValue.isColumn[0]) {
            if (filterresult.filter_info.table_filter[j]
                    .RightValue.isColumn[0]) {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) <=
                  atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .RightValue.value[0]][i]
                           .getVal()
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            } else {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) <=
                  atof(filterresult.filter_info.table_filter[j]
                           .RightValue.value[0]
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            }
          }
          break;
        }
        case GT: {
          if (filterresult.filter_info.table_filter[j].LeftValue.isColumn[0]) {
            if (filterresult.filter_info.table_filter[j]
                    .RightValue.isColumn[0]) {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) >
                  atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .RightValue.value[0]][i]
                           .getVal()
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            } else {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) >
                  atof(filterresult.filter_info.table_filter[j]
                           .RightValue.value[0]
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            }
          }
          break;
        }
        case LT: {
          if (filterresult.filter_info.table_filter[j].LeftValue.isColumn[0]) {
            if (filterresult.filter_info.table_filter[j]
                    .RightValue.isColumn[0]) {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) <
                  atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .RightValue.value[0]][i]
                           .getVal()
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            } else {
              if (atof(filterresult
                           .data[filterresult.filter_info.table_filter[j]
                                     .LeftValue.value[0]][i]
                           .getVal()
                           .c_str()) <
                  atof(filterresult.filter_info.table_filter[j]
                           .RightValue.value[0]
                           .c_str())) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            }
          }
          break;
        }
        case ET: {
          if (filterresult.filter_info.table_filter[j].LeftValue.isColumn[0]) {
            if (filterresult.filter_info.table_filter[j]
                    .RightValue.isColumn[0]) {
              if (filterresult
                      .data[filterresult.filter_info.table_filter[j]
                                .LeftValue.value[0]][i]
                      .getVal() ==
                  filterresult
                      .data[filterresult.filter_info.table_filter[j]
                                .RightValue.value[0]][i]
                      .getVal()
                      .c_str()) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            } else {
              if (filterresult
                      .data[filterresult.filter_info.table_filter[j]
                                .LeftValue.value[0]][i]
                      .getVal()
                      .c_str() == filterresult.filter_info.table_filter[j]
                                      .RightValue.value[0]
                                      .c_str()) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            }
          }
          break;
        }
        case NE: {
          if (filterresult.filter_info.table_filter[j].LeftValue.isColumn[0]) {
            if (filterresult.filter_info.table_filter[j]
                    .RightValue.isColumn[0]) {
              if (filterresult
                      .data[filterresult.filter_info.table_filter[j]
                                .LeftValue.value[0]][i]
                      .getVal() !=
                  filterresult
                      .data[filterresult.filter_info.table_filter[j]
                                .RightValue.value[0]][i]
                      .getVal()
                      .c_str()) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            } else {
              if (filterresult
                      .data[filterresult.filter_info.table_filter[j]
                                .LeftValue.value[0]][i]
                      .getVal()
                      .c_str() != filterresult.filter_info.table_filter[j]
                                      .RightValue.value[0]
                                      .c_str()) {
                filterresult.filter_info.filterResults.emplace_back(true);
              } else {
                filterresult.filter_info.filterResults.emplace_back(false);
              }
            }
          }
          break;
        }
        case LIKE: {
          break;
        }
        case BETWEEN: {
          filterresult.filter_info.filterResults.emplace_back(BetweenOperator(
              filterresult
                  .data[filterresult.filter_info.table_filter[j]
                            .LeftValue.value[0]][i]
                  .getVal(),
              filterresult.filter_info.table_filter[j].RightValue.value[0],
              filterresult.filter_info.table_filter[j].RightValue.value[1]));
          break;
        }
        case IN: {
          break;
        }
        case IS: {
          break;
        }
        case ISNOT: {
          break;
        }
        case NOT: {
          break;
        }
        case AND: {
          filterresult.filter_info.logicalOperators.emplace_back('&');
          break;
        }
        case OR: {
          filterresult.filter_info.logicalOperators.emplace_back('|');
          break;
        }
        case SUBSTRING: {
          break;
        }
      }
    }
    bool rowFilterResult = false;
    int lvCnt = 0;
    int rvCnt = 1;
    for (int j = 0; i < filterresult.filter_info.logicalOperators.size(); j++) {
      if (filterresult.filter_info.logicalOperators[j] == '&') {
        if (filterresult.filter_info.filterResults[lvCnt] &&
            filterresult.filter_info.filterResults[rvCnt]) {
          rowFilterResult = true;
        } else {
          rowFilterResult = false;
          break;
        }
        lvCnt += 2;
        rvCnt += 2;
      } else {
        if (filterresult.filter_info.filterResults[lvCnt] ||
            filterresult.filter_info.filterResults[rvCnt]) {
          rowFilterResult = true;
        } else {
          rowFilterResult = false;
          break;
        }
        lvCnt += 2;
        rvCnt += 2;
      }
    }
    if (rowFilterResult) {
      filterresult.filter_info.filteredIndex.emplace_back(i);
    }
  }
  sendfilterresult(filterresult);
  // cout << rownum << endl;
  return 0;
}

void Filter::sendfilterresult(Result &filterresult_) {
  cout << "filter result push :: sstFileName -> " << filterresult_.sst_name
       << ", filteredRowCount -> "
       << filterresult_.filter_info.filteredIndex.size() << endl;
  mergeManager->push_work(filterresult_);
}

bool Filter::LikeSubString(
    string lv, string rv) {  // case 0, 1, 2, 3, 4 --> %sub%(문자열 전체) or
                             // %sub(맨 뒤 문자열) or sub%(맨 앞 문자열) or
                             // sub(똑같은지) or %s%u%b%(생각 필요)
  // 해당 문자열 포함 검색 * 또는 % 존재 a like 'asd'
  int len = rv.length();
  int LvLen = lv.length();
  std::string val;
  if (rv[0] == '%' && rv[len - 1] == '%') {
    // case 0
    val = rv.substr(1, len - 2);
    for (int i = 0; i < LvLen - len + 1; i++) {
      if (lv.substr(i, val.length()) == val) {
        return true;
      }
    }
  } else if (rv[0] == '%') {
    // case 1
    val = rv.substr(1, len - 1);
    if (lv.substr(lv.length() - val.length() - 1, val.length()) == val) {
      return true;
    }
  } else if (rv[len - 1] == '%') {
    // case 2
    val = rv.substr(0, len - 1);
    if (lv.substr(0, val.length()) == val) {
      return true;
    }
  } else {
    // case 3
    if (rv == lv) {
      return true;
    }
  }
  return false;
}

bool Filter::LikeSubString_v2(string lv, string rv) {  // % 위치 찾기
  // 해당 문자열 포함 검색 * 또는 % 존재 a like 'asd'
  int len = rv.length();
  int LvLen = lv.length();
  int i = 0, j = 0;
  int substringsize = 0;
  bool isfirst = false,
       islast = false;  // %가 맨 앞 또는 맨 뒤에 있는지에 대한 변수
  // cout << rv[0] << endl;
  if (rv[0] == '%') {
    isfirst = true;
  }
  if (rv[len - 1] == '%') {
    islast = true;
  }
  vector<string> val = split(rv, '%');
  // for (int k = 0; k < val.size(); k++){
  //     cout << val[k] << endl;
  // }
  // for(int k = 0; k < val.size(); k ++){
  //     cout << val[k] << endl;
  // }
  if (isfirst) {
    i = 1;
  }
  // cout << LvLen << " " << val[val.size() - 1].length() << endl;
  // cout << LvLen - val[val.size() - 1].length() << endl;
  for (i; i < val.size(); i++) {
    // cout << "print i : " << i << endl;

    for (j; j < LvLen - val[val.size() - 1].length() + 1;
         j++) {  // 17까지 돌아야함 lvlen = 19 = 17
      // cout << "print j : " << j << endl;
      substringsize = val[i].length();
      if (!isfirst) {
        if (lv.substr(0, substringsize) != val[i]) {
          // cout << "111111" << endl;
          return false;
        }
      }
      if (!islast) {
        if (lv.substr(LvLen - val[val.size() - 1].length(),
                      val[val.size() - 1].length()) != val[val.size() - 1]) {
          // cout << lv.substr(LvLen - val[val.size()-1].length() + 1,
          // val[val.size()-1].length()) << " " << val[val.size()-1] << endl;
          // cout << "222222" << endl;
          return false;
        }
      }
      if (lv.substr(j, val[i].length()) == val[i]) {
        // cout << lv.substr(j,val[i].length()) << endl;
        if (i == val.size() - 1) {
          // cout << lv.substr(j, val[i].length()) << " " << val[i] << endl;
          return true;
        } else {
          j = j + val[i].length();
          i++;
          continue;
        }
      }
    }
    return false;
  }

  return false;
}

bool Filter::InOperator(string lv, Value &rv,
                        unordered_map<string, int> typedata, char *rowbuf) {
  // 여러 상수 or 연산 ex) a IN (50,60) == a = 50 or a = 60
  for (int i = 0; i < rv.Size(); i++) {
    string RV = "";
    if (rv[i].IsString()) {
      if (typedata[rv[i].GetString()] == 3 ||
          typedata[rv[i].GetString()] == 14)  //리틀에디안
      {
        int tmp = typeLittle(typedata, rv[i].GetString(), rowbuf);
        // cout << "type little" << endl;
        //나중 다른 데이트 처리를 위한 구분
        RV = ItoDec(tmp);
      }

      else if (typedata[rv[i].GetString()] == 254 ||
               typedata[rv[i].GetString()] == 15)  //빅에디안
      {
        RV = typeBig(rv[i].GetString(), rowbuf);
        // cout << "type big" << endl;
      } else if (typedata[rv[i].GetString()] == 246)  //예외 Decimal일때
      {
        RV = typeDecimal(rv[i].GetString(), rowbuf);
        // cout << "type decimal" << endl;
      } else {
        string tmps;
        tmps = rv[i].GetString();
        RV = tmps.substr(1);
      }
    } else  //걍 int면?
    {
      RV = ItoDec(rv[i].GetInt());
    }
    if (lv == RV) {
      return true;
    }
  }
  return false;
}
bool Filter::InOperator(int lv, Value &rv, unordered_map<string, int> typedata,
                        char *rowbuf) {
  for (int i = 0; i < rv.Size(); i++) {
    if (rv[i].IsString()) {
      if (typedata[rv[i].GetString()] == 3 ||
          typedata[rv[i].GetString()] == 14)  //리틀에디안
      {
        int RV = typeLittle(typedata, rv[i].GetString(), rowbuf);
        // cout << "type little" << endl;
        //나중 다른 데이트 처리를 위한 구분
        if (lv == RV) {
          return true;
        }
      }

      else if (typedata[rv[i].GetString()] == 254 ||
               typedata[rv[i].GetString()] == 15)  //빅에디안
      {
        string RV = typeBig(rv[i].GetString(), rowbuf);
        // cout << "type big" << endl;
        try {
          if (lv == atoi(RV.c_str())) {
            return true;
          }
        } catch (...) {
          continue;
        }
      } else if (typedata[rv[i].GetString()] == 246)  //예외 Decimal일때
      {
        string RV = typeDecimal(rv[i].GetString(), rowbuf);
        // cout << "type decimal" << endl;

        if (ItoDec(lv) == RV) {
          return true;
        }
      } else {
        string tmps;
        int RV;
        tmps = rv[i].GetString();
        try {
          RV = atoi(tmps.substr(1).c_str());
        } catch (...) {
          continue;
        }
      }
    } else  // int or string
    {
      int RV = rv[i].GetInt();
      if (lv == RV) {
        return true;
      }
    }
  }
  return false;
}

bool Filter::BetweenOperator(int lv, int rv1, int rv2) {
  // a between 10 and 20 == a >= 10 and a <= 20
  if (lv >= rv1 && lv <= rv2) {
    return true;
  }
  return false;
}

bool Filter::BetweenOperator(string lv, string rv1, string rv2) {
  // a between 10 and 20 == a >= 10 and a <= 20
  if (lv >= rv1 && lv <= rv2) {
    return true;
  }
  return false;
}

bool Filter::IsOperator(string lv, char *nonnullbit, int isnot) {
  // a is null or a is not null
  int colindex = rowfilterdata.ColIndexmap[lv];
  if (lv.empty()) {
    if (isnot == 0) {
      return true;
    } else {
      return false;
    }
  }
  if (isnot == 0) {
    return false;
  } else {
    return true;
  }
}

vector<string> Filter::split(string str, char Delimiter) {
  istringstream iss(str);  // istringstream에 str을 담는다.
  string buffer;  // 구분자를 기준으로 절삭된 문자열이 담겨지는 버퍼

  vector<string> result;

  // istringstream은 istream을 상속받으므로 getline을 사용할 수 있다.
  while (getline(iss, buffer, Delimiter)) {
    result.push_back(buffer);  // 절삭된 문자열을 vector에 저장
  }

  return result;
}

bool Filter::isvarc(vector<int> datatype, int ColNum,
                    vector<int> &varcharlist) {
  int isvarchar = 0;
  for (int i = 0; i < ColNum; i++)  // varchar 확인
  {
    if (datatype[i] == 15) {
      isvarchar = 1;
      varcharlist.push_back(i);
    }
  }
  return isvarchar;
}

int Filter::typeLittle(unordered_map<string, int> typedata, string colname,
                       char *rowbuf) {
  // cout << "tttteeessssttttt  " << typedata[colname] << endl;
  if (typedata[colname] == 14) {  // date
    // cout << 1 << endl;
    int *tmphex;
    char temphexbuf[4];
    //  = new char[4];
    int retint;

    memset(temphexbuf, 0, 4);
    // cout << rowfilterdata.ColName[10] << endl;
    // cout << colname << endl;
    // if(newstartptr[colname] == 0){
    GetColumnoff(colname);
    // }
    //     for(auto k = newstartptr.begin(); k != newstartptr.end(); k++){
    //     pair<string,int> a = *k;
    //     cout << a.first << " " << a.second << endl;
    // }
    //     cout << "!123" << endl;
    //     cout << newstartptr[colname] << endl;
    // cout << "col length : " << newlengthraw[rowfilterdata.ColName[0]] <<
    // endl; cout << "date size : " << newstartptr[colname] << endl;
    for (int k = 0; k < newlengthraw[colname]; k++) {
      // cout << rowbuf[newstartptr[colname] + k] << endl;
      temphexbuf[k] = rowbuf[newstartptr[colname] + k];
      // cout << hex << (int)rowbuf[newstartptr[colname] + k] << endl;
    }
    tmphex = (int *)temphexbuf;
    retint = tmphex[0];
    // cout << "tmphex : " << retint << endl;
    // delete[] temphexbuf;
    // cout << "return int = "<< retint << endl;
    return retint;
  } else if (typedata[colname] == 8) {  // int
    char intbuf[8];
    //  = new char[4];
    int64_t *intbuff;
    int64_t retint;

    memset(intbuf, 0, 8);
    // cout << newstartptr[colname] << endl;
    // if(newstartptr[colname] == 0){
    GetColumnoff(colname);
    // }
    // cout << "date size : " << newlengthraw[colname] << endl;
    for (int k = 0; k < newlengthraw[colname]; k++) {
      intbuf[k] = rowbuf[newstartptr[colname] + k];
    }
    intbuff = (int64_t *)intbuf;
    retint = intbuff[0];
    // delete[] intbuf;
    //  cout << intbuff[0] << endl;
    return retint;
  } else if (typedata[colname] == 3) {  // int
    char intbuf[4];
    //  = new char[4];
    int *intbuff;
    int retint;

    memset(intbuf, 0, 4);
    // cout << newstartptr[colname] << endl;
    // if(newstartptr[colname] == 0){
    GetColumnoff(colname);
    // }
    // cout << "date size : " << newlengthraw[colname] << endl;
    // cout << newstartptr[colname] << endl;
    for (int k = 0; k < newlengthraw[colname]; k++) {
      // cout << rowbuf[newstartptr[colname] + k] << endl;
      // printf("%02X ",(u_char)rowbuf[newstartptr[colname] + k]);
      intbuf[k] = rowbuf[newstartptr[colname] + k];
    }
    // cout << endl;
    intbuff = (int *)intbuf;
    retint = intbuff[0];
    // delete[] intbuf;
    //  cout << intbuff[0] << endl;
    return retint;
  } else {
    string tmpstring = joinmap[colname];
    return stoi(tmpstring);
  }
  return 0;
  // else
  // {
  //     //예외 타입
  //     return NULL;
  // }
}

string Filter::typeBig(string colname, char *rowbuf) {
  // if(newstartptr[colname] == 0){
  GetColumnoff(colname);
  // }
  string tmpstring = "";
  for (int k = 0; k < newlengthraw[colname]; k++) {
    tmpstring = tmpstring + (char)rowbuf[newstartptr[colname] + k];
  }
  return tmpstring;
}
string Filter::typeDecimal(string colname, char *rowbuf) {
  // if(newstartptr[colname] == 0){
  GetColumnoff(colname);
  // }
  char tmpbuf[4];
  string tmpstring = "";
  for (int k = 0; k < newlengthraw[colname]; k++) {
    ostringstream oss;
    int *tmpdata;
    tmpbuf[0] = 0x80;
    tmpbuf[1] = 0x00;
    tmpbuf[2] = 0x00;
    tmpbuf[3] = 0x00;
    tmpbuf[0] = rowbuf[newstartptr[colname] + k];
    tmpdata = (int *)tmpbuf;
    oss << hex << tmpdata[0];
    // oss << hex << rowbuf[newstartptr[WhereClauses[j]] + k];
    if (oss.str().length() <= 1) {
      tmpstring = tmpstring + "0" + oss.str();
    } else {
      tmpstring = tmpstring + oss.str();
    }
    // delete[] tmpbuf;
  }
  return tmpstring;
}

string Filter::ItoDec(int inum) {
  std::stringstream ss;
  std::string s;
  ss << hex << inum;
  s = ss.str();
  string decimal = "80";
  for (int i = 0; i < 10 - s.length(); i++) {
    decimal = decimal + "0";
  }
  decimal = decimal + s;
  decimal = decimal + "00";
  return decimal;
}

void Filter::GetColumnoff(string colname) {
  int startoff = 0;
  int offlen = 0;
  int tmpcount = rowfilterdata.offsetcount;
  // cout << rowfilterdata.offsetcount << " " <<
  // rowfilterdata.ColIndexmap[colname] << endl; bool varcharflag = 0;
  bool varcharflag = 0;
  for (int i = rowfilterdata.offsetcount;
       i < rowfilterdata.ColIndexmap[colname] + 1; i++) {
    if (i == 0) {
      startoff = rowfilterdata.rowoffset + rowfilterdata.startoff[i];
      // cout << rowfilterdata.rowoffset << endl;
    }
    // else if (varcharflag == 0){
    //     startoff = rowfilterdata.startoff[i] + rowfilterdata.rowoffset;
    // }
    else {
      startoff = newstartptr[rowfilterdata.ColName[i - 1]] +
                 newlengthraw[rowfilterdata.ColName[i - 1]];
    }
    varcharflag = 0;
    // cout << startoff << endl;
    // cout << startoff << endl;
    // cout << rowfilterdata.varcharlist.size() << endl;
    for (int j = 0; j < rowfilterdata.varcharlist.size(); j++) {
      //내가 varchar 타입일때
      // cout << rowfilterdata.varcharlist[j] << endl;
      if (i == rowfilterdata.varcharlist[j]) {
        // cout << rowfilterdata.varcharlist[j] << endl;
        if (rowfilterdata.offlen[i] < 256) {
          // cout <<"varchar row len : " << (int)rowfilterdata.rowbuf[startoff]
          // << endl;
          offlen = (int)rowfilterdata.rowbuf[startoff];
          // cout << offlen << endl;
          newstartptr.insert(make_pair(rowfilterdata.ColName[i], startoff + 1));
          newlengthraw.insert(make_pair(rowfilterdata.ColName[i], offlen));
        } else {
          char lenbuf[4];
          memset(lenbuf, 0, 4);
          int *lengthtmp;
          for (int k = 0; k < 2; k++) {
            lenbuf[k] = rowfilterdata.rowbuf[startoff + k];
            lengthtmp = (int *)lenbuf;
          }
          offlen = lengthtmp[0];
          newstartptr.insert(make_pair(rowfilterdata.ColName[i], startoff + 2));
          newlengthraw.insert(make_pair(rowfilterdata.ColName[i], offlen));
        }
        varcharflag = 1;
        break;
      }
    }
    if (varcharflag == 0) {
      // varchar 타입이 아닐때 varchar이전인지 확인할필요가 있을까?
      //  cout << startoff << endl;
      //  cout << "colname : " << rowfilterdata.ColName[i] << " startoff : " <<
      //  startoff << endl;
      newstartptr.insert(make_pair(rowfilterdata.ColName[i], startoff));
      newlengthraw.insert(
          make_pair(rowfilterdata.ColName[i], rowfilterdata.offlen[i]));
    }
    // for(auto k = newstartptr.begin(); k != newstartptr.end(); k++){
    //     pair<string,int> a = *k;
    //     cout << a.first << " " << a.second << endl;
    // }
    tmpcount++;
  }
  rowfilterdata.offsetcount = tmpcount;
  // cout << rowfilterdata.offsetcount << endl;
}

void Filter::JoinOperator(string colname) {}