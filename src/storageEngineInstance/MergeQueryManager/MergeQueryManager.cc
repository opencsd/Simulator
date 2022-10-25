#include "MergeQueryManager.hpp"

unordered_map<string, vector<vectortype>> GetBufMTable(string tablename,
                                                       SnippetStruct& snippet,
                                                       BufferManager& buff) {
  unordered_map<string, vector<vectortype>> table =
      buff.GetTableData(snippet.query_id, tablename).table_data;
  //   cout<< table.size() << endl;
  //   cout << tablename << endl;
  //   for (auto it = table.begin(); it != table.end(); it++) {
  //   pair<string, vector<vectortype>> a = *it;
  //   cout << a.first << endl;
  //   cout << a.second.size() << endl;
  //}
  // buff.GetTableInfo(snippet.query_id, tablename); //테이블 데이터 말고 type,
  // name, rownum, blocknum까지 채워줌
  return table;
}

vector<vectortype> Postfix(
    unordered_map<string, vector<vectortype>> tablelist,
    vector<Projection> data,
    unordered_map<string, vector<vectortype>> savedTable) {
  unordered_map<string, int> stackmap;
  vector<vectortype> ret;
  pair<string, vector<vectortype>> tmppair;

  auto tmpiter = tablelist.begin();
  tmppair = *tmpiter;
  int rownum = tmppair.second.size();

  if (data[0].value == "0") {
    stack<vectortype> tmpstack;
    for (int i = 0; i < rownum; i++) {
      for (int j = 1; j < data.size(); j++) {
        vectortype tmpvect;
        if (data[j].type == 10) {
          tmpstack.push(tablelist[data[j].value][i]);
        } else if (data[j].type == 11) {
          if (data[j].value == "+") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec + value2.intvec;
              tmpvect.type = 1;
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.floatvec = value1.floatvec + value2.floatvec;
              tmpvect.type = 2;
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "-") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec - value2.intvec;
              tmpvect.type = 1;
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.floatvec = value1.floatvec - value2.floatvec;
              tmpvect.type = 2;
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "*") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if (value1.type == 1) {
              if (value2.type == 1) {
                tmpvect.intvec = value1.intvec * value2.intvec;
                tmpvect.type = 1;
              } else {
                tmpvect.floatvec = value1.intvec * value2.floatvec;
                tmpvect.type = 2;
              }
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              if (value2.type == 2) {
                tmpvect.floatvec = value1.floatvec * value2.floatvec;
              } else {
                tmpvect.floatvec = value1.floatvec * value2.intvec;
              }
              tmpvect.type = 2;
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "/") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec / value2.intvec;
              tmpvect.type = 1;
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.floatvec = value1.floatvec / value2.floatvec;
              tmpvect.type = 2;
              tmpstack.push(tmpvect);
            }
          } else {
            // string
          }
        } else if (data[j].type == 4 || data[j].type == 5 ||
                   data[j].type == 6) {
          tmpvect.type = 2;
          tmpvect.floatvec = stod(data[j].value);
          tmpstack.push(tmpvect);
        } else {
          tmpvect.type = 1;
          tmpvect.intvec = stoi(data[j].value);
          tmpstack.push(tmpvect);
        }
      }
      ret.emplace_back(tmpstack.top());
    }
  } else if (data[0].value == "1") {  // sum
    vectortype retdata;
    stack<vectortype> tmpstack;
    // tmpstack.type = typeid(tablelist[data[1].value]).name;
    for (int i = 0; i < rownum; i++) {
      for (int j = 1; j < data.size(); j++) {
        vectortype tmpvect;
        if (data[j].type == 10) {
          tmpstack.push(tablelist[data[j].value][i]);
        } else if (data[j].type == 11) {
          if (data[j].value == "+") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec + value2.intvec;
              tmpvect.type = 1;
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.floatvec = value1.floatvec + value2.floatvec;
              tmpvect.type = 2;
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "-") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec - value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.floatvec = value1.floatvec - value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "*") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec * value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.floatvec = value1.floatvec * value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "/") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec / value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.floatvec = value1.floatvec / value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else {
            // string
          }
        } else if (data[j].type == 4 || data[j].type == 5 ||
                   data[j].type == 6) {
          tmpvect.type = 2;
          tmpvect.floatvec = stod(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
        } else {
          // cout << data[j].value << endl;
          tmpvect.type = 1;
          tmpvect.intvec = stoi(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
          // tmpstack.push(stoi(data[j].value));
        }
      }
      //이부분이 sum을 해야함

      vectortype tmpnum = tmpstack.top();  // int인지 flaot인지 구분 필요
      if (tmpnum.type == 1) {
        retdata.type = 1;
        retdata.intvec = retdata.intvec + tmpnum.intvec;
        // retdata = any_cast<int>(retdata) + any_cast<int>(tmpnum);
      } else {
        retdata.type = 2;
        retdata.floatvec = retdata.floatvec + tmpnum.floatvec;
        // retdata = any_cast<float>(retdata) + any_cast<float>(tmpnum);
      }
    }
    ret.emplace_back(retdata);
  } else if (data[0].value == "2") {
    vectortype retdata;
    stack<vectortype> tmpstack;
    // tmpstack.type = typeid(tablelist[data[1].value]).name;
    for (int i = 0; i < rownum; i++) {
      for (int j = 1; j < data.size(); j++) {
        vectortype tmpvect;
        if (data[j].type == 10) {
          tmpstack.push(tablelist[data[j].value][i]);
        } else if (data[j].type == 11) {
          if (data[j].value == "+") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec + value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec + value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "-") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec - value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec - value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "*") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec * value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec * value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "/") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec / value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec / value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else {
            // string
          }
        } else if (data[j].type == 4 || data[j].type == 5 ||
                   data[j].type == 6) {
          tmpvect.type = 2;
          tmpvect.floatvec = stof(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
        } else {
          // cout << data[j].value << endl;
          tmpvect.type = 1;
          tmpvect.intvec = stoi(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
          // tmpstack.push(stoi(data[j].value));
        }
      }
      //이부분이 sum을 해야함

      vectortype tmpnum = tmpstack.top();  // int인지 flaot인지 구분 필요
      if (tmpnum.type == 1) {
        retdata.type = 1;
        retdata.intvec = retdata.intvec + tmpnum.intvec;
        // retdata = any_cast<int>(retdata) + any_cast<int>(tmpnum);
      } else {
        retdata.type = 2;
        retdata.floatvec = retdata.floatvec + tmpnum.floatvec;
        // retdata = any_cast<float>(retdata) + any_cast<float>(tmpnum);
      }
    }
    if (retdata.type == 1) {
      retdata.type = 2;
      retdata.floatvec = retdata.intvec / rownum;
    } else {
      retdata.type = 2;
      retdata.floatvec = retdata.floatvec / rownum;
    }
    ret.emplace_back(retdata);
  } else if (data[0].value == "4") {
    auto it = tablelist.begin();
    pair<string, vector<vectortype>> tmpv = *it;
    vectortype retdata;
    retdata.type = 1;
    retdata.intvec = tmpv.second.size();
    ret.emplace_back(retdata);
  } else if (data[0].value == "6") {
    vectortype retdata;
    for (int i = 0; i < rownum; i++) {
      stack<vectortype> tmpstack;
      for (int j = 1; j < data.size(); j++) {
        vectortype tmpvect;
        if (data[j].type == 10) {
          // cout << any_cast<int>(tablelist[data[j].value][i]) << endl;
          tmpstack.push(tablelist[data[j].value][i]);
        } else if (data[j].type == 11) {
          if (data[j].value == "+") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec + value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec + value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "-") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec - value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec - value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "*") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec * value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec * value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "/") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec / value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec / value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else {
            // string
          }
        } else if (data[j].type == 4 || data[j].type == 5 ||
                   data[j].type == 6) {
          tmpvect.type = 2;
          tmpvect.floatvec = stod(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
        } else {
          // cout << data[j].value << endl;
          tmpvect.type = 1;
          tmpvect.intvec = stoi(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
          // tmpstack.push(stoi(data[j].value));
        }
      }
      if (i == 0) {
        retdata = tmpstack.top();
      } else {
        if (retdata.type == 0) {
          if (retdata.strvec > tmpstack.top().strvec) {
            retdata = tmpstack.top();
          }
        } else if (retdata.type == 1) {
          if (retdata.intvec > tmpstack.top().intvec) {
            retdata = tmpstack.top();
          }
        } else {
          if (retdata.floatvec > tmpstack.top().floatvec) {
            retdata = tmpstack.top();
          }
        }
      }
    }
    ret.emplace_back(retdata);
  } else if (data[0].value == "7") {
    vectortype retdata;
    for (int i = 0; i < rownum; i++) {
      stack<vectortype> tmpstack;
      for (int j = 1; j < data.size(); j++) {
        vectortype tmpvect;
        if (data[j].type == 10) {
          // cout << any_cast<int>(tablelist[data[j].value][i]) << endl;
          tmpstack.push(tablelist[data[j].value][i]);
        } else if (data[j].type == 11) {
          if (data[j].value == "+") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec + value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec + value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "-") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec - value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec - value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "*") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec * value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec * value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else if (data[j].value == "/") {
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            // if(value1.type == typeid(int&))
            if (value1.type == 1) {
              tmpvect.intvec = value1.intvec / value2.intvec;
              tmpvect.type = 1;
              // int retnum = any_cast<int>(value1) + any_cast<int>(value2);
              tmpstack.push(tmpvect);
            } else if (value1.type == 2) {
              tmpvect.intvec = value1.floatvec / value2.floatvec;
              tmpvect.type = 2;
              // float retnum = any_cast<float>(value1) +
              // any_cast<float>(value2);
              tmpstack.push(tmpvect);
            }
          } else {
            // string
          }
        } else if (data[j].type == 4 || data[j].type == 5 ||
                   data[j].type == 6) {
          tmpvect.type = 2;
          tmpvect.floatvec = stod(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
        } else {
          // cout << data[j].value << endl;
          tmpvect.type = 1;
          tmpvect.intvec = stoi(data[j].value);
          // tmpstack.push(stof(data[j].value));
          tmpstack.push(tmpvect);
          // tmpstack.push(stoi(data[j].value));
        }
      }
      if (i == 0) {
        retdata = tmpstack.top();
      } else {
        if (retdata.type == 0) {
          if (retdata.strvec < tmpstack.top().strvec) {
            retdata = tmpstack.top();
          }
        } else if (retdata.type == 1) {
          if (retdata.intvec < tmpstack.top().intvec) {
            retdata = tmpstack.top();
          }
        } else {
          if (retdata.floatvec < tmpstack.top().floatvec) {
            retdata = tmpstack.top();
          }
        }
      }
    }
    ret.emplace_back(retdata);
  } else if (data[0].value == "8") {
    for (int i = 0; i < rownum; i++) {
      // stack<vectortype> tmpstack;
      vectortype lv;
      vectortype rv;
      vectortype tmpv;
      bool lvflag = false;
      bool whenflag = false;
      bool thenflag = false;
      bool elsefalg = false;
      bool CV = false;
      for (int j = 1; j < data.size(); j++) {
        if (data[j].type == 10) {
          // tmpstack.push(tablelist[data[j].value][i]);
          if (whenflag) {
            if (lvflag) {
              rv = tablelist[data[j].value][i];
            } else {
              lv = tablelist[data[j].value][i];
              lvflag = true;
            }
          } else if (thenflag) {
            if (CV) {
              tmpv = tablelist[data[j].value][i];
              break;
            } else {
              continue;
            }
          } else {
            tmpv = tablelist[data[j].value][i];
            break;
          }
        } else if (data[j].type == 9 || data[j].type == 11) {
          if (data[j].value == "CASE") {
            continue;
          } else if (data[j].value == "WHEN") {
            whenflag = true;
            continue;
          } else if (data[j].value == "THEN") {
            thenflag = true;
            whenflag = false;
            continue;
          } else if (data[j].value == "ELSE") {
            elsefalg = true;
            thenflag = false;
            whenflag = false;
            continue;
          } else if (data[j].value == "=") {
            if (lv.type == 0) {
              if (lv.strvec == rv.strvec) {
                CV = true;
                continue;
              }
            } else if (lv.type == 1) {
              if (lv.intvec == rv.intvec) {
                CV = true;
                continue;
              }
            } else {
              if (lv.floatvec == rv.floatvec) {
                CV == true;
                continue;
              }
            }
            elsefalg = true;
            continue;
          } else if (data[j].value == "LIKE") {
            // cout << lv.strvec << " " << rv.strvec << endl;
            CV = LikeSubString_v2(lv.strvec, rv.strvec);
            continue;
          } else if (data[j].value == "END") {
            break;
          } else if (whenflag) {
            if (lvflag) {
              rv.type = 0;
              rv.strvec = data[j].value;
            } else {
              lv.type = 0;
              lv.strvec = data[j].value;
              lvflag = true;
            }
            continue;
          } else if (thenflag) {
            if (CV) {
              if (data[j].type == 9) {
                tmpv.type = 0;
                tmpv.strvec = data[j].value;
              } else if (data[j].type == 0 || data[j].type == 1 ||
                         data[j].type == 2 || data[j].type == 3 ||
                         data[j].type == 7) {
                tmpv.type = 1;
                // cout << data[j].value << endl;
                tmpv.intvec = stoi(data[j].value);
              } else {
                tmpv.type = 2;
                tmpv.floatvec = stod(data[j].value);
              }
              break;
            } else {
              continue;
            }
          } else if (elsefalg) {
            if (data[j].type == 9) {
              tmpv.type = 0;
              tmpv.strvec = data[j].value;
            } else if (data[j].type == 0 || data[j].type == 1 ||
                       data[j].type == 2 || data[j].type == 3 ||
                       data[j].type == 7) {
              tmpv.type = 1;
              // cout << data[j].value << endl;
              tmpv.intvec = stoi(data[j].value);
            } else {
              tmpv.type = 2;
              tmpv.floatvec = stod(data[j].value);
            }
            break;
          }
        }
      }
      ret.emplace_back(tmpv);
    }
  } else if (data[0].value == "13") {
    for (int i = 0; i < rownum; i++) {
      // stack<vectortype> tmpstack;
      vectortype lv;

      bool CV = false;
      for (int j = 1; j < data.size(); j++) {
        if (data[j].type == 10) {
          // cout << any_cast<int>(tablelist[data[j].value][i]) << endl;
          lv = tablelist[data[j].value][i];
        }
        lv.intvec = lv.intvec / 32 / 16;
        // cout << lv.intvec << endl;
      }
      ret.emplace_back(lv);
    }
  }
  // cout << 3 << endl;
  return ret;
}

void Aggregation(SnippetStruct& snippet, BufferManager& buff, bool tablecount) {
  unordered_map<string, vector<vectortype>> tablelist;
  keti_log("Merge Query Manager", "Start Aggregation Snippet Work : " +
                                      std::to_string(snippet.work_id));
  // cout << "[Merge Query Manager] Start Aggregation Snippet" <<
  // snippet.query_id << "-" << snippet.work_id << endl;

  // cout << "[Merge Query Manager] Strat Aggregation Time : ";
  time_t t = time(0);
  // cout << t << endl;
  keti_log("Merge Query Manager",
           "Strat Aggregation Time : " + std::to_string(t));
  if (tablecount) {
    for (int i = 0; i < snippet.tablename.size(); i++) {
      //혹시 모를 중복 제거 필요
      // cout << "here1" << endl;
      TableData table_data;
      table_data = buff.GetTableData(snippet.query_id, snippet.tablename[i]);
      unordered_map<string, vector<vectortype>> table = table_data.table_data;
      // cout << "here2" << endl;
      for (auto it = table.begin(); it != table.end(); it++) {
        pair<string, vector<vectortype>> pair;
        pair = *it;
        // cout << pair.first << " " << pair.second.size()<< endl;
        tablelist.insert(pair);
      }
    }
  } else {
    // sleep(1);
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tableAlias).table_data;
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> pair;
      pair = *it;
      // cout << pair.first << " " << pair.second.size()<< endl;
      // for(int k = 0; k < pair.second.size(); k++){
      //     cout << pair.first << " " << pair.second[k].type << " " <<
      //     pair.second[k].floatvec << endl;
      // }
      tablelist.insert(pair);
    }
  }
  // cout << 1 << endl;
  unordered_map<string, vector<vectortype>> savedTable;
  // cout << snippet.columnProjection.size() << endl;
  for (int i = 0; i < snippet.columnProjection.size(); i++) {
    // cout << i << endl;
    // any ret;
    // ret = Postfix(tablelist,snippet.columnProjection[i], savedTable);
    // vector<any> tmpvec;
    vector<vectortype> tmpdata =
        Postfix(tablelist, snippet.columnProjection[i], savedTable);
    savedTable.insert(make_pair(snippet.column_alias[i], tmpdata));
  }
  t = time(0);
  // cout << "[Merge Query Manager] End Aggregation Time : ";
  // cout << t << endl;
  keti_log("Merge Query Manager",
           "End Aggregation Time : " + std::to_string(t));
  if (!tablecount) {
    buff.DeleteTableData(snippet.query_id, snippet.tableAlias);
  }
  // buff.DeleteTableData(snippet.query_id,snippet.tableAlias);
  cout << "SaveTable Call in :: " << 802 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

void JoinTable(SnippetStruct& snippet, BufferManager& buff) {
  time_t t = time(0);
  // cout <<"[Merge Query Manager] Start Join Table1.Name : " <<
  // snippet.tablename[0] << " | Table2.Name : " << snippet.tablename[1]  <<"
  // Time : " << t << endl;
  keti_log("Merge Query Manager",
           "Start Join Table1.Name : " + snippet.tablename[0] +
               " | Table2.Name : " + snippet.tablename[1] +
               " Time : " + std::to_string(t));
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < 2; i++) {
    vector<string> tmpvector;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff);
    keti_log("Merge Query Manager",
             "Buffer Manager Access Get Table, Table Name : " +
                 snippet.tablename[i]);
    // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
    // Name : " << snippet.tablename[i] << endl;
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      // cout << tmppair.first << " " << tmppair.second.size() << endl;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
    }
    tablenamelist.emplace_back(tmpvector);
  }

  unordered_map<string, vector<vectortype>> savedTable;
  // for(auto i = tablelist[0].begin(); i != tablelist[0].end(); i++){
  //     pair<string,vector<any>> tabledata;
  //     tabledata = *i;
  //     vector<any> table;
  //     savedTable.insert(make_pair(tabledata.first,table));

  // }
  // for(auto i = tablelist[1].begin(); i != tablelist[1].end(); i++){
  //     pair<string,vector<any>> tabledata;
  //     tabledata = *i;
  //     vector<any> table;
  //     if(savedTable.find(tabledata.first) != savedTable.end()){
  //         savedTable.insert(make_pair(tabledata.first + "_2", table));
  //     }else{
  //         savedTable.insert(make_pair(tabledata.first, table));
  //     }
  // }
  // cout << 1 << endl;
  // cout << snippet.table_filter.GetType() << endl;
  // cout << snippet.columnProjection[0][0].value << endl;
  // cout << tablelist.size() << endl;
  // cout << tablelist.size() << endl;
  // cout << tablelist["n_regionkey"].size() << endl;
  // for(auto i = tablelist.begin(); i != tablelist.end(); i++){
  //     pair<string,vector<any>> cc = *i;
  //     cout << cc.first << endl;
  // }
  string joinColumn1 = snippet.table_filter[0].LV.value[0];
  string joinColumn2;
  auto ttit =
      find(tablenamelist[0].begin(), tablenamelist[0].end(), joinColumn1);
  if (ttit != tablenamelist[0].end()) {
    joinColumn1 = snippet.table_filter[0].LV.value[0];
    joinColumn2 = snippet.table_filter[0].RV.value[0];
  } else {
    joinColumn1 = snippet.table_filter[0].RV.value[0];
    joinColumn2 = snippet.table_filter[0].LV.value[0];
  }

  // string joinColumn1 = snippet.table_filter[0].LV.value[0];

  // cout << "[Merge Query Manager] JoinColumn1 : " << joinColumn1 << " |
  // JoinColumn2 : " << joinColumn2 << endl; cout << "[Merge Query Manager] " <<
  // joinColumn1 <<".Rows : " << tablelist[joinColumn1].size() << " | "<<
  // joinColumn2 << ".Rows : " <<tablelist[joinColumn2].size() << endl;
  keti_log("Merge Query Manager",
           "JoinColumn1 : " + joinColumn1 + " | JoinColumn2 : " + joinColumn2);
  keti_log("Merge Query Manager",
           joinColumn1 +
               ".Rows : " + std::to_string(tablelist[joinColumn1].size()) +
               " | " + joinColumn2 +
               ".Rows : " + std::to_string(tablelist[joinColumn2].size()));
  // string joinColumn2 = snippet.table_filter[0].RV.value[0];
  // cout << joinColumn2 << endl;
  // cout << tablelist[joinColumn1].size() << endl;
  // cout << tablelist[joinColumn2].size() << endl;
  // cout << 1 << endl;
  // cout << snippet.table_filter.Size() << endl;
  int countcout = 0;
  for (int i = 0; i < tablelist[joinColumn1].size(); i++) {
    for (int j = 0; j < tablelist[joinColumn2].size(); j++) {
      bool savedflag = true;
      if (tablelist[joinColumn1][i].type == 1) {
        // cout << any_cast<int>(tablelist[joinColumn1][j]) << endl;
        if (tablelist[joinColumn1][i].intvec ==
            tablelist[joinColumn2][j].intvec) {
          // cout << tablelist[joinColumn1][i].intvec << " " <<
          // tablelist[joinColumn2][j].intvec << endl;
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.value.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
              if (countcout == 0) {
                // cout << "[Merge Query Manager] Join Column 1 : " <<
                // tmpcolumn1 << " Join Column 2 : " << tmpcolumn2 << endl;
                keti_log("Merge Query Manager",
                         "Join Column 1 : " + tmpcolumn1 +
                             " Join Column 2 : " + tmpcolumn2);
                countcout++;
              }
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            // for(auto it = tablelist.begin(); it != tablelist.end(); it++){
            //     pair<string,vector<any>> tabledata;
            //     tabledata = *it;
            //     vector<any> tmptable = tabledata.second;
            //     savedTable[tabledata.first].emplace_back(tmptable[i]);
            // }
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              if (savedTable.find(tabledata.first + "_v2") !=
                  savedTable.end()) {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
              } else {
                for (int k = 0; k < tablenamelist.size(); k++) {
                  // cout << tabledata.first << endl;
                  auto tit = find(tablenamelist[k].begin(),
                                  tablenamelist[k].end(), tabledata.first);
                  if (tit != tablenamelist[k].end()) {
                    if (k == 0) {
                      // cout << tabledata.first << endl;
                      vector<vectortype> tmptable = tabledata.second;
                      // cout << i << endl;
                      // cout << tmptable[i].intvec << endl;
                      savedTable[tabledata.first].emplace_back(tmptable[i]);
                    } else {
                      vector<vectortype> tmptable = tabledata.second;
                      savedTable[tabledata.first].emplace_back(tmptable[j]);
                    }
                  }
                }
              }
            }
          }
        }
      } else if (tablelist[joinColumn1][i].type == 2) {
        if (tablelist[joinColumn1][i].floatvec ==
            tablelist[joinColumn2][j].floatvec) {
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.type.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            // for(auto it = tablelist[0].begin(); it != tablelist[0].end();
            // it++){
            //     pair<string,vector<any>> tabledata;
            //     tabledata = *it;
            //     vector<any> tmptable = tabledata.second;
            //     savedTable[tabledata.first].emplace_back(tmptable[i]);
            // }
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              if (savedTable.find(tabledata.first + "_v2") !=
                  savedTable.end()) {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
              } else {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first].emplace_back(tmptable[j]);
              }
            }
          }
        }

      } else if (tablelist[joinColumn1][i].type == 0) {
        if (tablelist[joinColumn1][i].strvec ==
            tablelist[joinColumn2][j].strvec) {
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.type.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            // for(auto it = tablelist[0].begin(); it != tablelist[0].end();
            // it++){
            //     pair<string,vector<any>> tabledata;
            //     tabledata = *it;
            //     vector<any> tmptable = tabledata.second;
            //     savedTable[tabledata.first].emplace_back(tmptable[i]);
            // }
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              if (savedTable.find(tabledata.first + "_v2") !=
                  savedTable.end()) {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
              } else {
                for (int k = 0; k < tablenamelist.size(); k++) {
                  auto tit = find(tablenamelist[k].begin(),
                                  tablenamelist[k].end(), tabledata.first);
                  if (tit != tablenamelist[k].end()) {
                    if (k == 0) {
                      vector<vectortype> tmptable = tabledata.second;
                      savedTable[tabledata.first].emplace_back(tmptable[i]);
                    } else {
                      vector<vectortype> tmptable = tabledata.second;
                      savedTable[tabledata.first].emplace_back(tmptable[j]);
                    }
                  }
                }
                // vector<vectortype> tmptable = tabledata.second;
                // savedTable[tabledata.first].emplace_back(tmptable[j]);
              }
            }
          }
        }
      }
    }
  }
  // cout << "end" << endl;
  time_t t1 = time(0);
  keti_log("Merge Query Manager", "End Join Snippet " +
                                      std::to_string(snippet.query_id) + "-" +
                                      std::to_string(snippet.work_id) +
                                      " Time : " + std::to_string(t1));
  // cout  << "[Merge Query Manager] End Join Snippet " << snippet.query_id <<
  // "-" << snippet.work_id <<" Time : " << t1 << endl;
  cout << "SaveTable Call in :: " << 1131 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

// void NaturalJoin(SnippetStruct snippet, BufferManager &buff){
//     //조인 조건 없음 컬럼명이 같은 모든 데이터 비교해서 같으면 조인
// }

// void OuterFullJoin(SnippetStruct snippet, BufferManager &buff){
//     //같은 값이 없을경우 null을 채워 모든 데이터 유지
// }

void LOJoin(SnippetStruct& snippet, BufferManager& buff) {
  //왼쪽 테이블 기준으로 같은 값이 없으면 null을 채움
  time_t t = time(0);
  keti_log("Merge Query Manager",
           "Start Join Table1.Name : " + snippet.tablename[0] +
               " | Table2.Name : " + snippet.tablename[1] +
               " Time : " + std::to_string(t));
  // cout <<"[Merge Query Manager] Start Join Table1.Name : " <<
  // snippet.tablename[0] << " | Table2.Name : " << snippet.tablename[1]  <<"
  // Time : " << t << endl;
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < 2; i++) {
    vector<string> tmpvector;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff);
    keti_log("Merge Query Manager",
             "Buffer Manager Access Get Table, Table Name : " +
                 snippet.tablename[i]);
    // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
    // Name : " << snippet.tablename[i] << endl;
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
    }
    tablenamelist.emplace_back(tmpvector);
  }

  unordered_map<string, vector<vectortype>> savedTable;
  // for(auto i = tablelist[0].begin(); i != tablelist[0].end(); i++){
  //     pair<string,vector<any>> tabledata;
  //     tabledata = *i;
  //     vector<any> table;
  //     savedTable.insert(make_pair(tabledata.first,table));

  // }
  // for(auto i = tablelist[1].begin(); i != tablelist[1].end(); i++){
  //     pair<string,vector<any>> tabledata;
  //     tabledata = *i;
  //     vector<any> table;
  //     if(savedTable.find(tabledata.first) != savedTable.end()){
  //         savedTable.insert(make_pair(tabledata.first + "_2", table));
  //     }else{
  //         savedTable.insert(make_pair(tabledata.first, table));
  //     }
  // }
  // cout << 1 << endl;
  // cout << snippet.table_filter.GetType() << endl;
  // cout << snippet.columnProjection[0][0].value << endl;
  // cout << tablelist.size() << endl;
  // cout << tablelist.size() << endl;
  // cout << tablelist["n_regionkey"].size() << endl;
  // for(auto i = tablelist.begin(); i != tablelist.end(); i++){
  //     pair<string,vector<any>> cc = *i;
  //     cout << cc.first << endl;
  // }
  string joinColumn1 = snippet.table_filter[0].LV.value[0];
  string joinColumn2;
  auto ttit =
      find(tablenamelist[0].begin(), tablenamelist[0].end(), joinColumn1);
  if (ttit != tablenamelist[0].end()) {
    joinColumn1 = snippet.table_filter[0].LV.value[0];
    joinColumn2 = snippet.table_filter[0].RV.value[0];
  } else {
    joinColumn1 = snippet.table_filter[0].RV.value[0];
    joinColumn2 = snippet.table_filter[0].LV.value[0];
  }

  // string joinColumn1 = snippet.table_filter[0].LV.value[0];
  keti_log("Merge Query Manager",
           "JoinColumn1 : " + joinColumn1 + " | JoinColumn2 : " + joinColumn2);
  // cout << "[Merge Query Manager] JoinColumn1 : " << joinColumn1 << " |
  // JoinColumn2 : " << joinColumn2 << endl;
  keti_log("Merge Query Manager",
           joinColumn1 +
               ".Rows : " + std::to_string(tablelist[joinColumn1].size()) +
               " | " + joinColumn2 +
               ".Rows : " + std::to_string(tablelist[joinColumn2].size()));
  // cout << "[Merge Query Manager] " << joinColumn1 <<".Rows : " <<
  // tablelist[joinColumn1].size() << " | "<< joinColumn2 << ".Rows : "
  // <<tablelist[joinColumn2].size() << endl;
  //  string joinColumn2 = snippet.table_filter[0].RV.value[0];
  //  cout << joinColumn2 << endl;
  //  cout << tablelist[joinColumn1].size() << endl;
  //  cout << tablelist[joinColumn2].size() << endl;
  //  cout << 1 << endl;
  //  cout << snippet.table_filter.Size() << endl;
  int countcout = 0;
  for (int i = 0; i < tablelist[joinColumn1].size(); i++) {
    bool leftjoinflag = true;
    for (int j = 0; j < tablelist[joinColumn2].size(); j++) {
      bool savedflag = true;
      if (tablelist[joinColumn1][i].type == 1) {
        // cout << any_cast<int>(tablelist[joinColumn1][j]) << endl;
        if (tablelist[joinColumn1][i].intvec ==
            tablelist[joinColumn2][j].intvec) {
          // cout << tablelist[joinColumn1][i].intvec << " " <<
          // tablelist[joinColumn2][j].intvec << endl;
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.value.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
              if (countcout == 0) {
                keti_log("Merge Query Manager",
                         "Join Column 1 : " + tmpcolumn1 +
                             " Join Column 2 : " + tmpcolumn2);
                // cout << "[Merge Query Manager] Join Column 1 : " <<
                // tmpcolumn1 << " Join Column 2 : " << tmpcolumn2 << endl;
                countcout++;
              }
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            leftjoinflag = false;
            // for(auto it = tablelist.begin(); it != tablelist.end(); it++){
            //     pair<string,vector<any>> tabledata;
            //     tabledata = *it;
            //     vector<any> tmptable = tabledata.second;
            //     savedTable[tabledata.first].emplace_back(tmptable[i]);
            // }
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              if (savedTable.find(tabledata.first + "_v2") !=
                  savedTable.end()) {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
              } else {
                for (int k = 0; k < tablenamelist.size(); k++) {
                  // cout << tabledata.first << endl;
                  auto tit = find(tablenamelist[k].begin(),
                                  tablenamelist[k].end(), tabledata.first);
                  if (tit != tablenamelist[k].end()) {
                    if (k == 0) {
                      // cout << tabledata.first << endl;
                      vector<vectortype> tmptable = tabledata.second;
                      // cout << i << endl;
                      // cout << tmptable[i].intvec << endl;
                      savedTable[tabledata.first].emplace_back(tmptable[i]);
                    } else {
                      vector<vectortype> tmptable = tabledata.second;
                      savedTable[tabledata.first].emplace_back(tmptable[j]);
                    }
                  }
                }
              }
            }
          }
        }
      } else if (tablelist[joinColumn1][i].type == 2) {
        if (tablelist[joinColumn1][i].floatvec ==
            tablelist[joinColumn2][j].floatvec) {
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.type.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            leftjoinflag = false;
            // for(auto it = tablelist[0].begin(); it != tablelist[0].end();
            // it++){
            //     pair<string,vector<any>> tabledata;
            //     tabledata = *it;
            //     vector<any> tmptable = tabledata.second;
            //     savedTable[tabledata.first].emplace_back(tmptable[i]);
            // }
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              if (savedTable.find(tabledata.first + "_v2") !=
                  savedTable.end()) {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
              } else {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first].emplace_back(tmptable[j]);
              }
            }
          }
        }

      } else if (tablelist[joinColumn1][i].type == 0) {
        if (tablelist[joinColumn1][i].strvec ==
            tablelist[joinColumn2][j].strvec) {
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.type.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            leftjoinflag = false;
            // for(auto it = tablelist[0].begin(); it != tablelist[0].end();
            // it++){
            //     pair<string,vector<any>> tabledata;
            //     tabledata = *it;
            //     vector<any> tmptable = tabledata.second;
            //     savedTable[tabledata.first].emplace_back(tmptable[i]);
            // }
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              if (savedTable.find(tabledata.first + "_v2") !=
                  savedTable.end()) {
                vector<vectortype> tmptable = tabledata.second;
                savedTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
              } else {
                for (int k = 0; k < tablenamelist.size(); k++) {
                  auto tit = find(tablenamelist[k].begin(),
                                  tablenamelist[k].end(), tabledata.first);
                  if (tit != tablenamelist[k].end()) {
                    if (k == 0) {
                      vector<vectortype> tmptable = tabledata.second;
                      savedTable[tabledata.first].emplace_back(tmptable[i]);
                    } else {
                      vector<vectortype> tmptable = tabledata.second;
                      savedTable[tabledata.first].emplace_back(tmptable[j]);
                    }
                  }
                }
                // vector<vectortype> tmptable = tabledata.second;
                // savedTable[tabledata.first].emplace_back(tmptable[j]);
              }
            }
          }
        }
      }
    }
    if (leftjoinflag) {
      //여기가 null로 채우는 부분
      for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
        pair<string, vector<vectortype>> tabledata;
        tabledata = *it;
        for (int k = 0; k < tablenamelist.size(); k++) {
          auto tit = find(tablenamelist[k].begin(), tablenamelist[k].end(),
                          tabledata.first);
          if (tit != tablenamelist[k].end()) {
            if (k == 0) {
              vector<vectortype> tmptable = tabledata.second;
              savedTable[tabledata.first].emplace_back(tmptable[i]);
            } else {
              vectortype tmptable;
              tmptable.type = 0;
              tmptable.strvec = "NULL";
              savedTable[tabledata.first].emplace_back(tmptable);
            }
          }
        }
      }
    }
  }
  // cout << "end" << endl;
  time_t t1 = time(0);
  // cout  << "[Merge Query Manager] End Join Snippet " << snippet.query_id <<
  // "-" << snippet.work_id <<" Time : " << t1 << endl;
  keti_log("Merge Query Manager", "End Join Snippet " +
                                      std::to_string(snippet.query_id) + "-" +
                                      std::to_string(snippet.work_id) +
                                      " Time : " + std::to_string(t1));
  cout << "SaveTable Call in :: " << 1494 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

// void OuterRightJoin(SnippetStruct snippet, BufferManager &buff){
//     //오른쪽 테이블 기준으로 같은 값이 없으면 null을 채움
// }

// void CrossJoin(SnippetStruct snippet, BufferManager &buff){
//     //테이블 곱 조인
// }

// void InnerJoin(SnippetStruct snippet, BufferManager &buff){

// }

void GroupBy(SnippetStruct& snippet, BufferManager& buff) {
  int groupbycount = snippet.groupBy.size();
  unordered_map<string, vector<vectortype>> table =
      GetBufMTable(snippet.tablename[0], snippet, buff);
  keti_log("Merge Query Manager", "Start Group by...");
  // cout << "[Merge Query Manager] Start Group by...";
  //무슨 테이블로 저장이 되어있을지에 대한 논의 필요(스니펫)

  // for(auto it = table.begin(); it != table.end(); it++){
  //     pair<string,vector<vectortype>> tmpp;
  //     tmpp = *it;
  //     for(int i = 0; i < tmpp.second.size(); i++){
  //         cout << tmpp.second[i].floatvec << " " << tmpp.second[i].type <<
  //         endl;
  //     }
  // }

  // unordered_map<>
  // cout << "start group by" << endl;
  unordered_map<string, groupby> groupbymap;
  for (int i = 0; i < table[snippet.groupBy[0]].size(); i++) {
    string key = "";
    vector<vectortype> tmpsavedkey;
    for (int j = 0; j < groupbycount; j++) {
      if (table[snippet.groupBy[j]][i].type == 1) {
        string tmpstring = to_string(table[snippet.groupBy[j]][i].intvec);
        key = key + tmpstring + ",";
        // cout << 2 << endl;
      } else if (table[snippet.groupBy[j]][i].type == 2) {
        // std::stringstream sstream;
        // sstream << table[snippet.groupBy[j]][i].floatvec;
        string tmpstring = to_string(table[snippet.groupBy[j]][i].floatvec);
        key = key + tmpstring + ",";
        // cout << 1 << endl;
      } else {
        key += table[snippet.groupBy[j]][i].strvec;
        key += ",";
        // cout << key << endl;
      }
      tmpsavedkey.emplace_back(table[snippet.groupBy[j]][i]);
    }
    // cout << key << endl;
    if (groupbymap.find(key) == groupbymap.end()) {
      // cout << "findkey : " << key << endl;
      groupby tmpgroupby;
      tmpgroupby.count = 0;
      tmpgroupby.rowcount = 0;
      // vectortype tmpvt;
      // tmpvt.type = 2;
      // tmpgroupby.value.emplace_back(tmpvt);
      groupbymap.insert(make_pair(key, tmpgroupby));
    }
    groupbymap[key].rowcount++;
    for (int j = 0; j < snippet.columnProjection.size(); j++) {
      // cout << j << endl;
      if (snippet.columnProjection[j][0].value == "0") {
        // cout << 11 << endl;
        if (groupbymap[key].count > snippet.columnProjection.size() - 1) {
          // break;
          continue;
        } else {
          //산술연산 진행
          // cout << "else" << endl;
          groupbymap[key].value.emplace_back(
              table[snippet.columnProjection[j][1].value][i]);
          // cout << groupbymap[key].count << endl;
          groupbymap[key].count = groupbymap[key].count + 1;
          // cout << groupbymap[key].count << endl;
        }
      } else if (snippet.columnProjection[j][0].value == "1") {
        // cout << 21 << endl;
        // cout << groupbymap[key].count  << endl;
        // sum
        if (groupbymap[key].count > snippet.columnProjection.size() - 1) {
          //덧셈 연산
          // cout << groupbymap[key].value[1].type << endl;
          if (groupbymap[key].value[j].type == 2) {
            // cout << table[snippet.columnProjection[j][1].value][i].floatvec
            // << endl;
            groupbymap[key].value[j].floatvec =
                groupbymap[key].value[j].floatvec +
                table[snippet.columnProjection[j][1].value][i].floatvec;
            // cout << groupbymap[key].value[1].floatvec << endl;
          } else if (groupbymap[key].value[j].type == 1) {
            groupbymap[key].value[j].intvec =
                groupbymap[key].value[j].intvec +
                table[snippet.columnProjection[j][1].value][i].intvec;
          }
        } else {
          // 값 넣기
          // cout << "else" << endl;
          groupbymap[key].value.emplace_back(
              table[snippet.columnProjection[j][1].value][i]);
          // cout <<table[snippet.columnProjection[j][1].value][i].floatvec <<
          // endl;
          //  cout <<table[snippet.columnProjection[j][1].value][i].type <<
          //  endl; cout << snippet.columnProjection[j][1].value << endl;
          groupbymap[key].count = groupbymap[key].count + 1;
          //  cout << groupbymap[key].count << endl;
        }
      } else if (snippet.columnProjection[j][0].value == "2") {
        if (groupbymap[key].count > snippet.columnProjection.size() - 1) {
          //덧셈 연산
          // cout << groupbymap[key].value[1].type << endl;
          if (groupbymap[key].value[j].type == 2) {
            groupbymap[key].value[j].floatvec =
                groupbymap[key].value[j].floatvec +
                table[snippet.columnProjection[j][1].value][i].floatvec;
            // cout << groupbymap[key].value[1].floatvec << endl;
          } else if (groupbymap[key].value[j].type == 1) {
            groupbymap[key].value[j].intvec =
                groupbymap[key].value[j].intvec +
                table[snippet.columnProjection[j][1].value][i].intvec;
          }
          // groupbymap[key].count = groupbymap[key].count + 1;
          if (i == table[snippet.groupBy[0]].size() - 1) {
            for (auto it2 = groupbymap.begin(); it2 != groupbymap.end();
                 it2++) {
              pair<string, groupby> tmpp2 = *it2;
              if (groupbymap[tmpp2.first].value[j].type == 2) {
                groupbymap[tmpp2.first].value[j].floatvec =
                    groupbymap[tmpp2.first].value[j].floatvec /
                    groupbymap[tmpp2.first].rowcount;
                // cout << groupbymap[key].value[j].floatvec << endl;
                // cout << groupbymap[key].value[1].floatvec << endl;
              } else if (groupbymap[tmpp2.first].value[j].type == 1) {
                groupbymap[tmpp2.first].value[j].type = 2;
                groupbymap[tmpp2.first].value[j].floatvec =
                    groupbymap[tmpp2.first].value[j].intvec /
                    groupbymap[tmpp2.first].rowcount;
              }
            }
            // if(groupbymap[key].value[j].type == 2){
            //     groupbymap[key].value[j].floatvec =
            //     groupbymap[key].value[j].floatvec / groupbymap[key].rowcount;
            //     // cout << groupbymap[key].value[j].floatvec << endl;
            // // cout << groupbymap[key].value[1].floatvec << endl;
            // }else if(groupbymap[key].value[j].type == 1){
            //     groupbymap[key].value[j].type = 2;
            //     groupbymap[key].value[j].floatvec =
            //     groupbymap[key].value[j].intvec / groupbymap[key].rowcount;
            // }
          }
        } else {
          // 값 넣기
          // cout << "else" << endl;
          groupbymap[key].value.emplace_back(
              table[snippet.columnProjection[j][1].value][i]);
          // cout <<table[snippet.columnProjection[j][1].value][i].floatvec <<
          // endl;
          //  cout <<table[snippet.columnProjection[j][1].value][i].type <<
          //  endl; cout << snippet.columnProjection[j][1].value << endl;
          groupbymap[key].count = groupbymap[key].count + 1;
          //  cout << groupbymap[key].count << endl;
        }
      } else if (snippet.columnProjection[j][0].value == "4") {
        if (groupbymap[key].count > snippet.columnProjection.size() - 1) {
          groupbymap[key].value[j].intvec = groupbymap[key].value[j].intvec + 1;
        } else {
          vectortype tmpv;
          tmpv.type = 1;
          tmpv.intvec = 1;
          groupbymap[key].value.emplace_back(tmpv);
          groupbymap[key].count = groupbymap[key].count + 1;
        }
      }
    }
  }

  //     groupbymap[key].savedkey = tmpsavedkey;
  //     for(int j = 0; j < snippet.columnProjection.size(); j++){
  //         if(snippet.columnProjection[j][0].value == "0"){
  //             if(groupbymap.find(key) == groupbymap.end()){
  //                 groupby tmpgroupby;
  //                 tmpgroupby.count = 0;
  //                 groupbymap.insert(make_pair(key,tmpgroupby));
  //             }else{
  //                 break;
  //             }
  //             if(groupbymap[key].value.size() < j + 1){
  //                 // groupbymap[key].value.emplace_back(table[])
  //                 //0이라도 산술연산 가능성 있음
  //                 stack<vectortype> tmpstack;
  //                 for(int k = 1; k < snippet.columnProjection[j].size();
  //                 k++){
  //                     if(snippet.columnProjection[j][k].type == 2){
  //                         //산술연산자 +-*/
  //                         vectortype tmp1 = tmpstack.top();
  //                         tmpstack.pop();
  //                         vectortype tmp2 = tmpstack.top();
  //                         tmpstack.pop();
  //                         if(snippet.columnProjection[j][k].value == "+"){
  //                             // tmpstack.push()
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec + tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec +
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }else if(snippet.columnProjection[j][k].value ==
  //                         "-"){
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec - tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec -
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }else if(snippet.columnProjection[j][k].value ==
  //                         "*"){
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec * tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec *
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }else if(snippet.columnProjection[j][k].value ==
  //                         "/"){
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec / tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec /
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }
  //                     }else if(snippet.columnProjection[j][k].type == 10){
  //                         //컬럼
  //                         tmpstack.push(table[snippet.columnProjection[j][k].value][i]);
  //                     }else{
  //                         //벨류
  //                         vectortype tmpvect;
  //                         // if()
  //                         tmpstack.push(tmpvect);
  //                         //
  //                         tmpstack.push(snippet.columnProjection[j][k].value);
  //                     }
  //                 }
  //                 groupbymap[key].value.emplace_back(tmpstack.top());
  //             }
  //             continue;
  //         }else{
  //             stack<vectortype> tmpstack;
  //             for(int k = 1; k < snippet.columnProjection[j].size(); k++){
  //                 if(snippet.columnProjection[j][k].type == 2){
  //                         //산술연산자 +-*/
  //                         vectortype tmp1 = tmpstack.top();
  //                         tmpstack.pop();
  //                         vectortype tmp2 = tmpstack.top();
  //                         tmpstack.pop();
  //                         if(snippet.columnProjection[j][k].value == "+"){
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec + tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec +
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }else if(snippet.columnProjection[j][k].value ==
  //                         "-"){
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec - tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec -
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }else if(snippet.columnProjection[j][k].value ==
  //                         "*"){
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec * tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec *
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }else if(snippet.columnProjection[j][k].value ==
  //                         "/"){
  //                             if(tmp1.type == 1){
  //                                 vectortype tmpvect;
  //                                 tmpvect.intvec = tmp1.intvec / tmp2.intvec;
  //                                 tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else if(tmp1.type == 2){
  //                                 vectortype tmpvect;
  //                                 tmpvect.floatvec = tmp1.floatvec /
  //                                 tmp2.floatvec; tmpvect.type = tmp1.type;
  //                                 tmpstack.push(tmpvect);
  //                             }else{
  //                                 //string이 산술연산이 가능한가?
  //                             }
  //                         }
  //                     }else if(snippet.columnProjection[j][k].type == 3){
  //                         //컬럼
  //                         tmpstack.push(table[snippet.columnProjection[j][k].value][i]);
  //                     }else{
  //                         //벨류
  //                         vectortype tmpvect;
  //                         tmpstack.push(tmpvect);
  //                         //
  //                         tmpstack.push(snippet.columnProjection[j][k].value);
  //                     }

  //             }
  //             if(snippet.columnProjection[j][0].value == "1"){
  //                 //sum
  //                 vectortype tmpvect;
  //                 if(groupbymap[key].count == 0){
  //                     groupbymap[key].value.emplace_back(tmpstack.top());
  //                     groupbymap[key].count++;
  //                 }else{
  //                     if(groupbymap[key].value[j].type == 1){
  //                         tmpvect.type = 1;
  //                         tmpvect.intvec = groupbymap[key].value[j].intvec +
  //                         tmpstack.top().intvec; groupbymap[key].value[j] =
  //                         tmpvect;
  //                     }else if(groupbymap[key].value[j].type == 2){
  //                         tmpvect.type = 2;
  //                         tmpvect.floatvec =
  //                         groupbymap[key].value[j].floatvec +
  //                         tmpstack.top().floatvec; groupbymap[key].value[j] =
  //                         tmpvect;
  //                         // groupbymap[key].value[j] =
  //                         any_cast<float>(groupbymap[key].value[j]) +
  //                         any_cast<float>(tmpstack.top());
  //                     }
  //                 }
  //             }else if(snippet.columnProjection[j][0].value == "2"){
  //                 //average
  //                 vectortype tmpvect;
  //                 if(groupbymap[key].count == 0){
  //                     groupbymap[key].value.emplace_back(tmpstack.top());
  //                     groupbymap[key].count++;
  //                 }else{
  //                     if(groupbymap[key].value[j].type == 1){
  //                         tmpvect.type = 1;
  //                         tmpvect.intvec = groupbymap[key].value[j].intvec +
  //                         tmpstack.top().intvec; groupbymap[key].value[j] =
  //                         tmpvect;
  //                     }else if(groupbymap[key].value[j].type == 2){
  //                         tmpvect.type = 2;
  //                         tmpvect.floatvec =
  //                         groupbymap[key].value[j].floatvec +
  //                         tmpstack.top().floatvec; groupbymap[key].value[j] =
  //                         tmpvect;
  //                     }
  //                     groupbymap[key].count++;
  //                 }
  //             }
  //         }
  //     }
  // }
  //여기서 average 계산 후 저장
  unordered_map<string, vector<vectortype>> savedtable;
  auto it = groupbymap.begin();
  // cout << groupbymap.size() << endl;
  pair<string, groupby> tmpp = *it;
  // cout << "end gr" << endl;
  for (int k = 0; k < tmpp.second.value.size(); k++) {
    vector<vectortype> tmpvector;
    // cout << k << endl;
    for (auto j = groupbymap.begin(); j != groupbymap.end(); j++) {
      pair<string, groupby> tmppair = *j;
      // cout << tmppair.first <<" " <<tmppair.second.value.size() << endl;
      tmpvector.emplace_back(tmppair.second.value[k]);
    }
    savedtable.insert(make_pair(snippet.column_alias[k], tmpvector));
  }
  // buff.DeleteTableData(snippet.query_id,snippet.tableAlias);
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedtable);
  // cout << "af st" << endl;
}

// void Having(SnippetStruct& snippet, BufferManager &buff){
//     //일반적인 필터링과 다른게 뭘까?(그룹바이 어그리게이션을 미리 해놓는다면)
//     --> 서브쿼리 존재 가능
//     //그룹바이와 해빙은 따로 처리를 해야함(다른 스니펫)
//     //드라이빙 테이블은 무엇인가?
//     unordered_map<string,vector<any>> totaltable;
//     for(int i = 0; i < snippet.tablename.size(); i++){
//         unordered_map<string,vector<any>> table =
//         GetBufMTable(snippet.tablename[i], snippet, buff); for(auto it =
//         table.begin(); it != table.end(); i++){
//             pair<string,vector<any>> pair;
//             pair = *it;
//             totaltable.insert(pair);
//         }
//     }
//     for(int i = 0; i < snippet.table_Having.Size(); i++){

//     }

// }

void OrderBy(SnippetStruct& snippet, BufferManager& buff) {
  time_t t = time(0);
  cout << "[Merge Query Manager] Strat Order By Column : " << snippet.orderBy[0]
       << endl;
  // cout << snippet.tableAlias << endl;
  unordered_map<string, vector<vectortype>> table =
      GetBufMTable(snippet.tableAlias, snippet, buff);
  int ordercount = snippet.orderBy.size();
  vector<sortclass> sortbuf;
  for (int i = 0; i < table[snippet.orderBy[0]].size(); i++) {
    sortclass tmpclass;
    unordered_map<string, vectortype> value;
    for (int j = 0; j < ordercount; j++) {
      tmpclass.ordername.emplace_back(snippet.orderBy[j]);
      tmpclass.ordertype.emplace_back(snippet.orderType[j]);
    }
    for (auto j = table.begin(); j != table.end(); j++) {
      pair<string, vector<vectortype>> tmppair = *j;
      value.insert(make_pair(tmppair.first, tmppair.second[i]));
    }
    tmpclass.value = value;
    tmpclass.ordercount = ordercount;
    sortbuf.emplace_back(tmpclass);
  }
  sort(sortbuf.begin(), sortbuf.end());

  unordered_map<string, vector<vectortype>> orderedtable;
  for (int i = 0; i < sortbuf.size(); i++) {
    // orderedtable.insert(make_pair())
    for (auto j = sortbuf[i].value.begin(); j != sortbuf[i].value.end(); j++) {
      pair<string, vectortype> tmppair = *j;
      if (i == 0) {
        vector<vectortype> tmpvector;
        orderedtable.insert(make_pair(tmppair.first, tmpvector));
      }
      orderedtable[tmppair.first].emplace_back(tmppair.second);
    }
  }
  buff.DeleteTableData(snippet.query_id, snippet.tableAlias);
  cout << "SaveTable Call in :: " << 1971 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, orderedtable);
  time_t t1 = time(0);
  cout << "[Merge Query Manager] End Order By" << endl;
}

// void GetAccessData()
// { // access lib 구현 후 작성(구현 x)
// }

// void ColumnProjection(SnippetStruct snippet)
// {
//     int nullsize = 0; //논널비트에 대한 정보 테이블 매니저에 추가 해야함
//     //결과를 저장할 벡터 + 데이터를 가져올 벡터 필요
//     // unordered_map<string, int> typemap;
//     snippet.tabledata.clear();
//     snippet.resultstack.clear();
//     snippet.resultdata.clear();
//     for (int i = 0; i < snippet.tableAlias.size(); i++)
//     {
//         VectorType vectortype;
//         vectortype.type = snippet.savetype[i];
//         snippet.resultdata.insert(make_pair(snippet.tableAlias[i],
//         vectortype)); StackType stacktype; stacktype.type =
//         snippet.savetype[i];
//         snippet.resultstack.insert(make_pair(snippet.tableAlias[i],
//         stacktype));
//         // snippet.resultstack.insert(make_pair(snippet.tableAlias[i],
//         StackType{}));
//     }
//     for (int i = 0; i < snippet.table_col.size(); i++)
//     {
//         // typemap.insert(make_pair(snippet.table_col[i],
//         snippet.table_datatype[i])); VectorType tmpvector;

//         snippet.tabledata.insert(make_pair(snippet.table_col[i], tmpvector));
//     }
//     // for (int n = 0; n < snippet.tableblocknum; n++)
//     // {
//     for (int i = 0; i < snippet.columnProjection.size(); i++)
//     {
//         // vector<string> a =
//         buffermanager.gettable(snippet.tableProjection[i])
//         // stack<string> tmpstack;
//         switch (atoi(snippet.columnProjection[i][0].value.c_str()))
//         {
//         case KETI_Column_name:
//             for (int j = 1; j < snippet.columnProjection[i].size(); j++)
//             {
//                 for (int k = 0; k < snippet.tablerownum; k++)
//                 {
//                     switch (snippet.columnProjection[i][j].type)
//                     {
//                     case PROJECTION_STRING:
//                         //진짜 string or decimal
//                         /* code */
//                         break;
//                     case PROJECTION_INT:
//                         /* code */
//                         break;
//                     case PROJECTION_FLOAT:
//                         /* code */
//                         break;
//                     case PROJECTION_COL:
//                         //버퍼매니저에서 데이터 가져와야함
//                         /* code */
//                         //
//                         snippet.tabledata[snippet.tableProjection[i][j].value].type
//                         // if(typemap[snippet.tableProjection[i][j].value] ==
//                         3){
//                         //     // buffermanager.gettable()

//                         // }else
//                         if(typemap[snippet.tableProjection[i][j].value] ==
//                         246){

//                         // }else
//                         if(typemap[snippet.tableProjection[i][j].value] ==
//                         14){

//                         // }else
//                         if(typemap[snippet.tableProjection[i][j].value] ==
//                         4){

//                         // }
//                         if(snippet.resultstack[snippet.tableAlias[i]].type ==
//                         1){ //int

//                         }else
//                         if(snippet.resultstack[snippet.tableAlias[i]].type ==
//                         2){ //float

//                         }

//                         break;
//                     case PROJECTION_OPER:
//                         break;
//                     default:
//                         break;
//                     }
//                 }
//             }

//             /* code */
//             break;
//         case KETI_SUM:
//             /* code */
//             break;
//         case KETI_AVG:
//             /* code */
//             break;
//         case KETI_COUNT:
//             /* code */
//             break;
//         case KETI_COUNTALL:
//             /* code */
//             break;
//         case KETI_MIN:
//             /* code */
//             break;
//         case KETI_MAX:
//             /* code */
//             break;
//         case KETI_CASE:
//             /* code */
//             break;
//         case KETI_WHEN:
//             /* code */
//             break;
//         case KETI_THEN:
//             /* code */
//             break;
//         case KETI_ELSE:
//             /* code */
//             break;
//         case KETI_LIKE:
//             /* code */
//             break;
//         default:
//             break;
//         }
//         // }
//     }
// }

// void GetColOff()
// {
// }

void DependencyExist(SnippetStruct& snippet, BufferManager& buff) {
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < snippet.tablename.size(); i++) {
    vector<string> tmpvector;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff); cout << "[Merge Query
    // Manager] Buffer Manager Access Get Table, Table Name : " <<
    // snippet.tablename[i] << endl;
    keti_log("Merge Query Manager",
             "Buffer Manager Access Get Table, Table Name : " +
                 snippet.tablename[i]);
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
      // cout << tmppair.first << " " << tmppair.second.size() << endl;
    }
    tablenamelist.emplace_back(tmpvector);
  }
  // cout << "[Merge Query Manager] Start Dependency Subquery Exist" << endl;
  keti_log("Merge Query Manager", "Start Dependency Subquery Exist");
  unordered_map<string, vector<vectortype>> savedTable;
  bool filterflag = false;
  bool rowflag = true;
  for (int i = 0; i < tablelist[snippet.table_filter[0].LV.value[0]].size();
       i++) {
    for (int k = 0; k < tablelist[snippet.table_filter[0].RV.value[0]].size();
         k++) {
      filterflag = false;
      for (int j = 0; j < snippet.table_filter.size(); j++) {
        if (snippet.table_filter[j].LV.type.size() == 0) {
          continue;
        }
        if (tablelist[snippet.table_filter[j].LV.value[0]][i].type == 0) {
          if (snippet.table_filter[j].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].strvec ==
                tablelist[snippet.table_filter[j].RV.value[0]][k].strvec) {
              filterflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].strvec !=
                tablelist[snippet.table_filter[j].RV.value[0]][k].strvec) {
              filterflag = true;
              continue;
            }
          }
        } else if (tablelist[snippet.table_filter[j].LV.value[0]][i].type ==
                   1) {
          if (snippet.table_filter[j].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].intvec ==
                tablelist[snippet.table_filter[j].RV.value[0]][k].intvec) {
              filterflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].intvec !=
                tablelist[snippet.table_filter[j].RV.value[0]][k].intvec) {
              filterflag = true;
              continue;
            }
          }
        } else {
          if (snippet.table_filter[j].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].floatvec ==
                tablelist[snippet.table_filter[j].RV.value[0]][k].floatvec) {
              filterflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].floatvec !=
                tablelist[snippet.table_filter[j].RV.value[0]][k].floatvec) {
              filterflag = true;
              continue;
            }
          }
        }
        filterflag = false;
        break;
      }
      //이번 조건이 맞는지(이전과 비교하여)
      if (filterflag) {
        rowflag = true;
        break;
      } else {
        rowflag = false;
        continue;
      }
    }
    //여기가 한 로우 세이브 가능한지 체크
    if (rowflag) {
      for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
        pair<string, vector<vectortype>> tabledata;
        tabledata = *it;
        // cout << tabledata.first << endl;
        auto tit = find(tablenamelist[0].begin(), tablenamelist[0].end(),
                        tabledata.first);
        if (tit != tablenamelist[0].end()) {
          // cout << tabledata.first << endl;
          vector<vectortype> tmptable = tabledata.second;
          // cout << i << endl;
          // cout << tmptable[i].intvec << endl;
          savedTable[tabledata.first].emplace_back(tmptable[i]);
        }
      }
    }
  }
  //디펜던시 완료
  keti_log("Merge Query Manager", "End Dependency Subquery Exist");
  cout << "SaveTable Call in :: " << 2236 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

void DependencyNotExist(SnippetStruct& snippet, BufferManager& buff) {
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < snippet.tablename.size(); i++) {
    vector<string> tmpvector;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff);
    // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
    // Name : " << snippet.tablename[i] << endl;
    keti_log("Merge Query Manager",
             "Buffer Manager Access Get Table, Table Name : " +
                 snippet.tablename[i]);
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
    }
    tablenamelist.emplace_back(tmpvector);
  }
  unordered_map<string, vector<vectortype>> savedTable;
  bool filterflag = false;
  bool rowflag = true;
  for (int i = 0; i < tablelist[snippet.table_filter[0].LV.value[0]].size();
       i++) {
    for (int k = 0; k < tablelist[snippet.table_filter[0].RV.value[0]].size();
         k++) {
      filterflag = false;
      for (int j = 0; j < snippet.table_filter.size(); j++) {
        if (snippet.table_filter[j].LV.type.size() == 0) {
          continue;
        }
        if (tablelist[snippet.table_filter[j].LV.value[0]][i].type == 0) {
          if (snippet.table_filter[j].filteroper == 4) {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].strvec ==
                tablelist[snippet.table_filter[j].RV.value[0]][k].strvec) {
              filterflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].strvec !=
                tablelist[snippet.table_filter[j].RV.value[0]][k].strvec) {
              filterflag = true;
              continue;
            }
          }
        } else if (tablelist[snippet.table_filter[j].LV.value[0]][i].type ==
                   1) {
          if (snippet.table_filter[j].filteroper == 4) {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].intvec ==
                tablelist[snippet.table_filter[j].RV.value[0]][k].intvec) {
              filterflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].intvec !=
                tablelist[snippet.table_filter[j].RV.value[0]][k].intvec) {
              filterflag = true;
              continue;
            }
          }
        } else {
          if (snippet.table_filter[j].filteroper == 4) {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].floatvec ==
                tablelist[snippet.table_filter[j].RV.value[0]][k].floatvec) {
              filterflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[j].LV.value[0]][i].floatvec !=
                tablelist[snippet.table_filter[j].RV.value[0]][k].floatvec) {
              filterflag = true;
              continue;
            }
          }
        }
        filterflag = false;
        break;
      }
      //이번 조건이 맞는지(이전과 비교하여)
      if (filterflag) {
        rowflag = false;
        break;
      } else {
        rowflag = true;
        continue;
      }
    }
    //여기가 한 로우 세이브 가능한지 체크
    if (rowflag) {
      for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
        pair<string, vector<vectortype>> tabledata;
        tabledata = *it;
        // cout << tabledata.first << endl;
        auto tit = find(tablenamelist[0].begin(), tablenamelist[0].end(),
                        tabledata.first);
        if (tit != tablenamelist[0].end()) {
          // cout << tabledata.first << endl;
          vector<vectortype> tmptable = tabledata.second;
          // cout << i << endl;
          // cout << tmptable[i].intvec << endl;
          savedTable[tabledata.first].emplace_back(tmptable[i]);
        }
      }
    }
  }
  //디펜던시 완료
  keti_log("Merge Query Manager", "End Dependency Subquery Exist");
  cout << "SaveTable Call in :: " << 2350 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

void DependencyOPER(SnippetStruct& snippet, BufferManager& buff) {
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < snippet.tablename.size(); i++) {
    vector<string> tmpvector;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff);
    // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
    // Name : " << snippet.tablename[i] << endl;
    keti_log("Merge Query Manager",
             "Buffer Manager Access Get Table, Table Name : " +
                 snippet.tablename[i]);
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
      // cout << tmppair.first << " " << tmppair.second.size() << endl;
    }
    tablenamelist.emplace_back(tmpvector);
  }
  // for(auto it = tablelist.begin(); it != tablelist.end(); it++){
  //     pair<string,vector<vectortype>> tmppair = *it;
  //     tablelist.insert(make_pair(tmppair.first,tmppair.second));
  //     cout << tmppair.first << " " << tmppair.second.size() << endl;
  // }
  // cout << snippet.table_filter[0].RV.value[0] << endl;
  // cout << tablelist.size() << endl;
  unordered_map<string, vector<vectortype>> savedTable;
  // cout << "start dependencyoper" << endl;
  // cout << tablelist[snippet.table_filter[0].LV.value[0]].size() << " " <<
  // tablelist[snippet.table_filter[0].RV.value[0]].size() << endl; cout <<
  // tablelist[snippet.table_filter[0].LV.value[0]][0].intvec << " " <<
  // tablelist[snippet.table_filter[0].RV.value[0]][0].intvec << endl;
  for (int i = 0; i < tablelist[snippet.table_filter[0].LV.value[0]].size();
       i++) {
    unordered_map<string, vector<vectortype>> tmpTable;
    for (int j = 0; j < tablelist[snippet.table_filter[0].RV.value[0]].size();
         j++) {
      // cout << tablelist[snippet.table_filter[0].LV.value[0]][i].intvec << " "
      // << tablelist[snippet.table_filter[0].RV.value[0]][j].intvec << endl;
      bool savedflag = true;
      for (int k = 0; k < snippet.table_filter.size(); k++) {
        if (snippet.table_filter[k].LV.type.size() == 0) {
          continue;
        }
        if (tablelist[snippet.table_filter[k].LV.value[0]][i].type == 0) {
          if (snippet.table_filter[k].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].strvec ==
                tablelist[snippet.table_filter[k].RV.value[0]][j].strvec) {
              savedflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].strvec >
                tablelist[snippet.table_filter[k].RV.value[0]][j].strvec) {
              savedflag = true;
              continue;
            }
          }
        } else if (tablelist[snippet.table_filter[k].LV.value[0]][i].type ==
                   1) {
          // cout << tablelist[snippet.table_filter[k].LV.value[0]][i].intvec <<
          // " " << tablelist[snippet.table_filter[k].RV.value[0]][j].intvec <<
          // endl;
          if (snippet.table_filter[k].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].intvec ==
                tablelist[snippet.table_filter[k].RV.value[0]][j].intvec) {
              savedflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].intvec >
                tablelist[snippet.table_filter[k].RV.value[0]][j].intvec) {
              savedflag = true;
              continue;
            }
          }
        } else {
          if (snippet.table_filter[k].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].floatvec ==
                tablelist[snippet.table_filter[k].RV.value[0]][j].floatvec) {
              savedflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].floatvec >
                tablelist[snippet.table_filter[k].RV.value[0]][j].floatvec) {
              savedflag = true;
              continue;
            }
          }
        }
        savedflag = false;
        break;
      }
      //템프 테이블 저장
      if (savedflag) {
        for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
          pair<string, vector<vectortype>> tabledata;
          tabledata = *it;
          if (tmpTable.find(tabledata.first + "_v2") != tmpTable.end()) {
            vector<vectortype> tmptable = tabledata.second;
            tmpTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
          } else {
            for (int k = 0; k < tablenamelist.size(); k++) {
              // cout << tabledata.first << endl;
              auto tit = find(tablenamelist[k].begin(), tablenamelist[k].end(),
                              tabledata.first);
              if (tit != tablenamelist[k].end()) {
                if (k == 0) {
                  continue;  //아마 1번 테이블은 필요 없을듯
                  // cout << tabledata.first << endl;
                  vector<vectortype> tmptable = tabledata.second;
                  // cout << i << endl;
                  // cout << tmptable[i].intvec << endl;
                  tmpTable[tabledata.first].emplace_back(tmptable[i]);
                } else {
                  vector<vectortype> tmptable = tabledata.second;
                  tmpTable[tabledata.first].emplace_back(tmptable[j]);
                }
              }
            }
          }
        }
      }
    }
    // cout << tmpTable.size() << endl;
    //어그리게이션 및 비교 수행 및 로우 저장 여부 결정
    for (int j = 0; j < snippet.dependencyProjection.size(); j++) {
      //아마 무조건 1번만 수행하는 반복문
      vector<vectortype> tmpdata =
          Postfix(tmpTable, snippet.dependencyProjection[j], savedTable);
      if (snippet.dependencyFilter[0].filteroper ==
          static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
        if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].type == 0) {
          if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].strvec ==
              tmpdata[0].strvec) {
            //같다 1번 테이블만 필요
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                // cout << tabledata.first << endl;
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    // cout << tabledata.first << endl;
                    vector<vectortype> tmptable = tabledata.second;
                    // cout << i << endl;
                    // cout << tmptable[i].intvec << endl;
                    savedTable[tabledata.first].emplace_back(tmptable[i]);
                  } else {
                    continue;
                    vector<vectortype> tmptable = tabledata.second;
                    savedTable[tabledata.first].emplace_back(tmptable[j]);
                  }
                }
              }
            }
          }
        } else if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].type ==
                   1) {
          if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].intvec ==
              tmpdata[0].intvec) {
            //같다
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                // cout << tabledata.first << endl;
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    // cout << tabledata.first << endl;
                    vector<vectortype> tmptable = tabledata.second;
                    // cout << i << endl;
                    // cout << tmptable[i].intvec << endl;
                    savedTable[tabledata.first].emplace_back(tmptable[i]);
                  } else {
                    continue;
                    vector<vectortype> tmptable = tabledata.second;
                    savedTable[tabledata.first].emplace_back(tmptable[j]);
                  }
                }
              }
            }
          }
        } else {
          if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].floatvec ==
              tmpdata[0].floatvec) {
            //같다
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                // cout << tabledata.first << endl;
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    // cout << tabledata.first << endl;
                    vector<vectortype> tmptable = tabledata.second;
                    // cout << i << endl;
                    // cout << tmptable[i].intvec << endl;
                    savedTable[tabledata.first].emplace_back(tmptable[i]);
                  } else {
                    continue;
                    vector<vectortype> tmptable = tabledata.second;
                    savedTable[tabledata.first].emplace_back(tmptable[j]);
                  }
                }
              }
            }
          }
        }
      } else {
        // >
        if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].type == 0) {
          if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].strvec >
              tmpdata[0].strvec) {
            //같다 1번 테이블만 필요
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                // cout << tabledata.first << endl;
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    // cout << tabledata.first << endl;
                    vector<vectortype> tmptable = tabledata.second;
                    // cout << i << endl;
                    // cout << tmptable[i].intvec << endl;
                    savedTable[tabledata.first].emplace_back(tmptable[i]);
                  } else {
                    continue;
                    vector<vectortype> tmptable = tabledata.second;
                    savedTable[tabledata.first].emplace_back(tmptable[j]);
                  }
                }
              }
            }
          }
        } else if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].type ==
                   1) {
          if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].intvec >
              tmpdata[0].intvec) {
            //같다
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                // cout << tabledata.first << endl;
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    // cout << tabledata.first << endl;
                    vector<vectortype> tmptable = tabledata.second;
                    // cout << i << endl;
                    // cout << tmptable[i].intvec << endl;
                    savedTable[tabledata.first].emplace_back(tmptable[i]);
                  } else {
                    continue;
                    vector<vectortype> tmptable = tabledata.second;
                    savedTable[tabledata.first].emplace_back(tmptable[j]);
                  }
                }
              }
            }
          }
        } else {
          if (tablelist[snippet.dependencyFilter[0].LV.value[0]][i].floatvec >
              tmpdata[0].floatvec) {
            //같다
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                // cout << tabledata.first << endl;
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    // cout << tabledata.first << endl;
                    vector<vectortype> tmptable = tabledata.second;
                    // cout << i << endl;
                    // cout << tmptable[i].intvec << endl;
                    savedTable[tabledata.first].emplace_back(tmptable[i]);
                  } else {
                    continue;
                    vector<vectortype> tmptable = tabledata.second;
                    savedTable[tabledata.first].emplace_back(tmptable[j]);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  cout << "SaveTable Call in :: " << 2665 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

void DependencyIN(SnippetStruct& snippet, BufferManager& buff) {
  //나중에 구현 예정
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < snippet.tablename.size(); i++) {
    vector<string> tmpvector;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff);
    // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
    // Name : " << snippet.tablename[i] << endl;
    keti_log("Merge Query Manager",
             "Buffer Manager Access Get Table, Table Name : " +
                 snippet.tablename[i]);
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
    }
    tablenamelist.emplace_back(tmpvector);
  }

  unordered_map<string, vector<vectortype>> savedTable;

  for (int i = 0; i < tablelist[snippet.table_filter[0].LV.value[0]].size();
       i++) {
    unordered_map<string, vector<vectortype>> tmpTable;
    for (int j = 0; j < tablelist[snippet.table_filter[0].RV.value[0]].size();
         j++) {
      bool savedflag = true;
      for (int k = 0; k < snippet.table_filter.size(); k++) {
        if (snippet.table_filter[k].LV.type.size() == 0) {
          continue;
        }
        if (tablelist[snippet.table_filter[k].LV.value[0]][i].type == 0) {
          if (snippet.table_filter[k].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].strvec ==
                tablelist[snippet.table_filter[k].RV.value[0]][j].strvec) {
              savedflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].strvec !=
                tablelist[snippet.table_filter[k].RV.value[0]][j].strvec) {
              savedflag = true;
              continue;
            }
          }
        } else if (tablelist[snippet.table_filter[k].LV.value[0]][i].type ==
                   1) {
          if (snippet.table_filter[k].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].intvec ==
                tablelist[snippet.table_filter[k].RV.value[0]][j].intvec) {
              savedflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].intvec !=
                tablelist[snippet.table_filter[k].RV.value[0]][j].intvec) {
              savedflag = true;
              continue;
            }
          }
        } else {
          if (snippet.table_filter[k].filteroper ==
              static_cast<int>(KETI_OPER_TYPE::KETI_ET)) {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].floatvec ==
                tablelist[snippet.table_filter[k].RV.value[0]][j].floatvec) {
              savedflag = true;
              continue;
            }
          } else {
            if (tablelist[snippet.table_filter[k].LV.value[0]][i].floatvec !=
                tablelist[snippet.table_filter[k].RV.value[0]][j].floatvec) {
              savedflag = true;
              continue;
            }
          }
        }
        savedflag = false;
        break;
      }
      //템프 테이블 저장
      if (savedflag) {
        for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
          pair<string, vector<vectortype>> tabledata;
          tabledata = *it;
          if (tmpTable.find(tabledata.first + "_v2") != tmpTable.end()) {
            vector<vectortype> tmptable = tabledata.second;
            tmpTable[tabledata.first + "_v2"].emplace_back(tmptable[j]);
          } else {
            for (int k = 0; k < tablenamelist.size(); k++) {
              // cout << tabledata.first << endl;
              auto tit = find(tablenamelist[k].begin(), tablenamelist[k].end(),
                              tabledata.first);
              if (tit != tablenamelist[k].end()) {
                if (k == 0) {
                  continue;  //아마 1번 테이블은 필요 없을듯
                  // cout << tabledata.first << endl;
                  vector<vectortype> tmptable = tabledata.second;
                  // cout << i << endl;
                  // cout << tmptable[i].intvec << endl;
                  tmpTable[tabledata.first].emplace_back(tmptable[i]);
                } else {
                  vector<vectortype> tmptable = tabledata.second;
                  tmpTable[tabledata.first].emplace_back(tmptable[j]);
                }
              }
            }
          }
        }
      }
    }
    // tmpTable의 row들과 In연산을 통해 값이 있는지 확인
  }
  cout << "SaveTable Call in :: " << 2788 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

void Storage_Filter(SnippetStruct& snippet, BufferManager& buff) {
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < snippet.tablename.size(); i++) {
    vector<string> tmpvector;
    unordered_map<string, vector<vectortype>> table;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff);
    if (snippet.tablename.size() > 1) {
      // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
      // Name : " << snippet.tablename[i] << endl;
      keti_log("Merge Query Manager",
               "Buffer Manager Access Get Table, Table Name : " +
                   snippet.tablename[i]);
      table =
          buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    } else {
      // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
      // Name : " << snippet.tableAlias << endl;
      keti_log("Merge Query Manager",
               "Buffer Manager Access Get Table, Table Name : " +
                   snippet.tableAlias);
      table =
          buff.GetTableData(snippet.query_id, snippet.tableAlias).table_data;
    }
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      // cout << tmppair.first << " " << tmppair.second.size() << endl;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
    }
    tablenamelist.emplace_back(tmpvector);
  }
  // cout << snippet.table_filter.size() << endl;
  // cout <<"start filter" << endl;
  unordered_map<string, vector<vectortype>> savedTable;
  //단일 테이블이다 = 모든 컬럼의 row수가 같다
  auto it = tablelist.begin();
  pair<string, vector<vectortype>> tmppair = *it;
  int rownum = tmppair.second.size();
  bool rvflag = false;
  for (int i = 0; i < snippet.table_filter.size(); i++) {
    // cout << i << endl;
    // if(snippet.table_filter[i].RV.type)
    for (int j = 0; j < snippet.table_filter[i].RV.type.size(); j++) {
      if (snippet.table_filter[i].RV.type[j] == 10) {
        rvflag = true;
        break;
      }
    }
  }
  vectortype rv;
  vector<vectortype> rvlist;
  if (!rvflag) {
    for (int j = 0; j < snippet.table_filter.size(); j++) {
      if (snippet.table_filter[j].RV.type.size() == 0) {
        rvlist.emplace_back(rv);
      } else {
        rvlist.emplace_back(
            GetFilterValue(snippet.table_filter[j].RV, 0, tablelist));
      }
    }
  }
  for (int i = 0; i < rownum; i++) {
    // cout << i << endl;
    bool passed = false;
    bool isSaved = true;
    bool CV = false;
    bool notflag = false;
    // if(i % 1000 == 0){
    //     cout << i << endl;
    //     // if(i != 0){
    //     //     auto it = savedTable.begin();
    //     //     pair<string,vector<vectortype>> tmp = *it;
    //     //     cout << tmp.second.size() << endl;
    //     // }
    // }
    for (int j = 0; j < snippet.table_filter.size(); j++) {
      // cout << j << endl;
      // cout << j << endl;
      if (passed) {
        if (snippet.table_filter[j].filteroper ==
            static_cast<int>(KETI_OPER_TYPE::KETI_OR)) {
          passed = false;
        }
        continue;
      }
      switch (snippet.table_filter[j].filteroper) {
        case static_cast<int>(KETI_OPER_TYPE::KETI_GE): {
          /* code */
          // lv가 여러개일 가능성을 생각해야한다?
          //  time_t t1 = time(0);
          //  cout << t1 << endl;
          //  vectortype lv;
          //  vectortype rv;
          //  if(snippet.table_filter[j].LV.type[0] == KETI_COLUMN){
          //      lv = tablelist[snippet.table_filter[j].LV.value[0]][i];
          //  }
          //  if(snippet.table_filter[j].RV.type[0] == KETI_DATE){
          //      rv.type = 1;
          //      rv.intvec = stoi(snippet.table_filter[j].RV.value[0]);
          //  }
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.intvec << endl;
          if (rvflag) {
            rv = GetFilterValue(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          // t1 = time(0);
          // cout << t1 << endl;
          // cout << lv.type << " " << rv.type << endl;
          if (lv.type == 0) {
            if (lv.strvec >= rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            // cout << lv.intvec << " " << rv.intvec << endl;
            if (lv.intvec >= rv.intvec) {
              // cout << lv.intvec << " " << rv.intvec << endl;
              // cout << "fliter" << endl;
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec >= rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_LE): {
          // vectortype lv;
          // vectortype rv;
          // if(snippet.table_filter[j].LV.type[0] == KETI_COLUMN){
          //     lv = tablelist[snippet.table_filter[j].LV.value[0]][i];
          // }
          // if(snippet.table_filter[j].RV.type[0] == KETI_DATE){
          //     rv.type = 1;
          //     rv.intvec = stoi(snippet.table_filter[j].RV.value[0]);
          // }
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.intvec << endl;
          if (rvflag) {
            rv = GetFilterValue(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          if (lv.type == 0) {
            if (lv.strvec <= rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            // cout << lv.intvec << " " << rv.intvec << endl;
            if (lv.intvec <= rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec <= rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_GT): {
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.type << endl;
          if (rvflag) {
            rv = GetFilterValue(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          if (lv.type == 0) {
            if (lv.strvec > rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            if (lv.intvec > rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            // cout << lv.floatvec << " " << rv.floatvec << endl;
            if (lv.floatvec > rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_LT): {
          // vectortype lv;
          // vectortype rv;
          // if(snippet.table_filter[j].LV.type[0] == KETI_COLUMN){
          //     lv = tablelist[snippet.table_filter[j].LV.value[0]][i];
          // }
          // if(snippet.table_filter[j].RV.type[0] == KETI_DATE){
          //     rv.type = 1;
          //     rv.intvec = stoi(snippet.table_filter[j].RV.value[0]);
          // }
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.intvec << endl;
          // cout << rvflag << endl;
          if (rvflag) {
            // cout << 1 << endl;
            rv = GetFilterValue(snippet.table_filter[j].RV, i, tablelist);
            // cout << 2 << endl;
          } else {
            rv = rvlist[j];
          }
          // cout << lv.type << endl;
          // cout << rv.type << endl;
          // break;
          if (lv.type == 0) {
            if (lv.strvec < rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            // cout << lv.intvec << " " << rv.intvec << endl;
            if (lv.intvec < rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec < rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_ET): {
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          if (rvflag) {
            rv = GetFilterValue(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          // cout << lv.type << endl;
          if (lv.type == 0) {
            if (lv.strvec == rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            if (lv.intvec == rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec == rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_NE): {
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          if (rvflag) {
            rv = GetFilterValue(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          if (lv.type == 0) {
            if (lv.strvec != rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            if (lv.intvec != rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec != rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_LIKE): {
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          if (rvflag) {
            rv = GetFilterValue(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          CV = LikeSubString_v2(lv.strvec, rv.strvec);
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_BETWEEN): {
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          vector<vectortype> rv;
          for (int k = 0; k < snippet.table_filter[j].RV.value.size(); k++) {
            vectortype tmpv;
            if (snippet.table_filter[j].RV.type[k] ==
                static_cast<int>(KETI_Type::KETI_COLUMN)) {
              tmpv = tablelist[snippet.table_filter[j].RV.value[k]][i];
              rv.emplace_back(tmpv);
            } else if (lv.type == 1) {
              tmpv.type = 1;
              tmpv.intvec = stoi(snippet.table_filter[j].RV.value[k]);
              rv.emplace_back(tmpv);
            } else if (lv.type == 2) {
              tmpv.type = 2;
              tmpv.intvec = stof(snippet.table_filter[j].RV.value[k]);
              rv.emplace_back(tmpv);
            }
          }
          if (lv.type == 1) {
            if (lv.intvec >= rv[0].intvec && lv.intvec <= rv[1].intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 2) {
            if (lv.floatvec >= rv[0].floatvec &&
                lv.floatvec <= rv[1].floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_IN): {
          vectortype lv =
              GetFilterValue(snippet.table_filter[j].LV, i, tablelist);
          string tmpstring;
          if (lv.type == 0) {
            tmpstring = lv.strvec;
          } else if (lv.type == 1) {
            tmpstring = to_string(lv.intvec);
          } else {
            tmpstring = to_string(lv.floatvec);
          }
          if (snippet.table_filter[j].RV.type.size() == 1 &&
              snippet.table_filter[j].RV.type[0] == 10) {
            for (int k = 0;
                 k < tablelist[snippet.table_filter[j].RV.value[0]].size();
                 k++) {
              vectortype rv = tablelist[snippet.table_filter[j].RV.value[0]][k];
              if (lv.type == 1) {
                if (rv.type == 1) {
                  if (lv.intvec == rv.intvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                } else {
                  if (lv.intvec == rv.floatvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                }
              } else if (lv.type == 2) {
                if (rv.type == 1) {
                  if (lv.floatvec == rv.intvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                } else {
                  if (lv.floatvec == rv.floatvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                }
              } else {
                if (rv.type == 1) {
                  if (lv.strvec == to_string(rv.intvec)) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                } else {
                  if (lv.strvec == to_string(rv.floatvec)) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                }
              }
            }
          } else {
            for (int k = 0; k < snippet.table_filter[j].RV.type.size(); k++) {
              if (tmpstring == snippet.table_filter[j].RV.value[k]) {
                CV = true;
                break;
              } else {
                CV = false;
              }
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_IS):
          break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_ISNOT):
          break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_NOT):
          if (notflag) {
            notflag = false;
          } else {
            notflag = true;
          }
          break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_AND): {
          if (CV) {
            isSaved = true;
          } else {
            isSaved = false;
            passed = true;
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_OR): {
          if (CV) {
            isSaved = true;
            break;
          } else {
            passed = false;
            continue;
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_SUBSTRING): {
          if (CV) {
            isSaved = true;
            break;
          } else {
            passed = false;
            continue;
          }
          break;
        }
        default:
          break;
      }
    }
    if (isSaved && CV) {
      //로우 살림
      // cout << "saved" << endl;
      // break;
      // continue;
      for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
        pair<string, vector<vectortype>> tabledata;
        tabledata = *it;
        // savedTable[tabledata.first].emplace_back(tabledata.second[i]);
        auto tit = find(tablenamelist[0].begin(), tablenamelist[0].end(),
                        tabledata.first);
        if (tit != tablenamelist[0].end()) {
          // cout << tabledata.first << endl;
          vector<vectortype> tmptable = tabledata.second;
          // cout << i << endl;
          // cout << tmptable[i].intvec << endl;
          savedTable[tabledata.first].emplace_back(tmptable[i]);
        }
      }
    }
  }
  if (snippet.tablename.size() == 1) {
    buff.DeleteTableData(snippet.query_id, snippet.tableAlias);
  }
  cout << "SaveTable Call in :: " << 3358 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

vectortype GetFilterValue(filtervalue filterdata, int index,
                          unordered_map<string, vector<vectortype>> tablelist) {
  vectortype tmpvalue;
  stack<vectortype> tmpstack;
  for (int i = 0; i < filterdata.type.size(); i++) {
    vectortype tmpvect;
    switch (filterdata.type[i]) {
      case snippetsample::Snippet::COLUMN: {
        tmpstack.push(tablelist[filterdata.value[i]][0]);
      } break;
      case snippetsample::Snippet::INT16:
      case snippetsample::Snippet::INT32:
      case snippetsample::Snippet::INT64: {
        tmpvect.type = 1;
        tmpvect.intvec = stoll(filterdata.value[i]);
        tmpstack.push(tmpvect);
      } break;
      case snippetsample::Snippet::FLOAT32:
      case snippetsample::Snippet::FLOAT64: {
        tmpvect.type = 2;
        tmpvect.intvec = stod(filterdata.value[i]);
        tmpstack.push(tmpvect);
      } break;
      case snippetsample::Snippet::DATE: {
        vectortype value1;
        value1.type = 1;
        value1.intvec = stoi(filterdata.value[i]);
        tmpstack.push(value1);
      } break;
      case snippetsample::Snippet::STRING: {
        vectortype value1;
        value1.type = 0;
        value1.strvec = filterdata.value[i];
        tmpstack.push(value1);
      } break;
      case snippetsample::Snippet::OPERATOR: {
        std::string op_value = filterdata.value[i];
        if (op_value == "+") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec + value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec + value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        } else if (op_value == "-") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec - value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec - value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        } else if (op_value == "/") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec / value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec / value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        } else if (op_value == "*") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec * value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec * value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        }
      } break;
    }
  }
  return tmpstack.top();
}

vectortype GetLV(lv lvdata, int index,
                 unordered_map<string, vector<vectortype>> tablelist) {
  //여러개일 가능성이 생긴순간 postfix로 가야함
  // if(lvdata.type[0] == KETI_COLUMN){
  //     return tablelist[lvdata.value[0]][index];
  // }else{
  //     vectortype tmpvect;
  //     tmpvect.type = 1;
  //     tmpvect.intvec = stoi(lvdata.value[0]);
  //     return tmpvect;
  // }
  vectortype tmpvalue;
  // tmpvalue.type = 1;
  // return tmpvalue;
  stack<vectortype> tmpstack;
  for (int i = 0; i < lvdata.type.size(); i++) {
    vectortype tmpvect;
    if (lvdata.type[i] == static_cast<int>(KETI_Type::KETI_COLUMN)) {
      //무조건 컬럼이 1순위로 들어와야 함
      // sub스트링은 다른곳에서 처리
      tmpstack.push(tablelist[lvdata.value[i]][index]);
      // cout << tmpstack.top().intvec << endl;
    } else if (lvdata.type[i] == static_cast<int>(KETI_Type::KETI_STRING)) {
      if (lvdata.value[i] == "+") {
        vectortype value2 = tmpstack.top();
        tmpstack.pop();
        vectortype value1 = tmpstack.top();
        tmpstack.pop();
        if (value1.type == 1) {
          tmpvect.intvec = value1.intvec + value2.intvec;
          tmpvect.type = 1;
          tmpstack.push(tmpvect);
        } else if (value1.type == 2) {
          tmpvect.intvec = value1.floatvec + value2.floatvec;
          tmpvect.type = 2;
          tmpstack.push(tmpvect);
        }
      } else if (lvdata.value[i] == "-") {
        vectortype value2 = tmpstack.top();
        tmpstack.pop();
        vectortype value1 = tmpstack.top();
        tmpstack.pop();
        if (value1.type == 1) {
          tmpvect.intvec = value1.intvec - value2.intvec;
          tmpvect.type = 1;
          tmpstack.push(tmpvect);
        } else if (value1.type == 2) {
          tmpvect.intvec = value1.floatvec - value2.floatvec;
          tmpvect.type = 2;
          tmpstack.push(tmpvect);
        }
      } else if (lvdata.value[i] == "/") {
        vectortype value2 = tmpstack.top();
        tmpstack.pop();
        vectortype value1 = tmpstack.top();
        tmpstack.pop();
        if (value1.type == 1) {
          tmpvect.intvec = value1.intvec / value2.intvec;
          tmpvect.type = 1;
          tmpstack.push(tmpvect);
        } else if (value1.type == 2) {
          tmpvect.intvec = value1.floatvec / value2.floatvec;
          tmpvect.type = 2;
          tmpstack.push(tmpvect);
        }
      } else if (lvdata.value[i] == "*") {
        vectortype value2 = tmpstack.top();
        tmpstack.pop();
        vectortype value1 = tmpstack.top();
        tmpstack.pop();
        if (value1.type == 1) {
          tmpvect.intvec = value1.intvec * value2.intvec;
          tmpvect.type = 1;
          tmpstack.push(tmpvect);
        } else if (value1.type == 2) {
          tmpvect.intvec = value1.floatvec * value2.floatvec;
          tmpvect.type = 2;
          tmpstack.push(tmpvect);
        }
      } else {
        //그냥 스트링이 있나?
        //있다 like와 같은것
        tmpvect.type = 0;
        tmpvect.strvec = lvdata.value[i];
        tmpstack.push(tmpvect);
      }
    } else {
      // int 나 float등 과 같은 일반적인 상수
      if (tmpstack.top().type == 1) {
        tmpvect.type = 1;
        tmpvect.intvec = stoi(lvdata.value[i]);
        tmpstack.push(tmpvect);
      } else {
        tmpvect.type = 2;
        tmpvect.intvec = stof(lvdata.value[i]);
        tmpstack.push(tmpvect);
      }
    }
  }
  return tmpstack.top();
}

vectortype GetRV(rv rvdata, int index,
                 unordered_map<string, vector<vectortype>> tablelist) {
  // if(rvdata.type[0] == KETI_COLUMN){
  //     return tablelist[rvdata.value[0]][index];
  // }else if(){
  //     vectortype tmpvect;
  //     tmpvect.type = 1;
  //     tmpvect.intvec = stoi(rvdata.value[0]);
  //     return tmpvect;
  // }
  vectortype tmpvalue;
  stack<vectortype> tmpstack;
  for (int i = 0; i < rvdata.type.size(); i++) {
    vectortype tmpvect;
    switch (rvdata.type[i]) {
      case snippetsample::Snippet::COLUMN: {
        tmpstack.push(tablelist[rvdata.value[i]][0]);
      } break;
      case snippetsample::Snippet::INT16:
      case snippetsample::Snippet::INT32:
      case snippetsample::Snippet::INT64: {
        tmpvect.type = 1;
        tmpvect.intvec = stoll(rvdata.value[i]);
        tmpstack.push(tmpvect);
      } break;
      case snippetsample::Snippet::FLOAT32:
      case snippetsample::Snippet::FLOAT64: {
        tmpvect.type = 2;
        tmpvect.intvec = stod(rvdata.value[i]);
        tmpstack.push(tmpvect);
      } break;
      case snippetsample::Snippet::DATE: {
        vectortype value1;
        value1.type = 1;
        value1.intvec = stoi(rvdata.value[i]);
        tmpstack.push(value1);
      } break;
      case snippetsample::Snippet::STRING: {
        vectortype value1;
        value1.type = 0;
        value1.strvec = rvdata.value[i];
        tmpstack.push(value1);
      } break;
      case snippetsample::Snippet::OPERATOR: {
        std::string op_value = rvdata.value[i];
        if (op_value == "+") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec + value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec + value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        } else if (op_value == "-") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec - value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec - value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        } else if (op_value == "/") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec / value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec / value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        } else if (op_value == "*") {
          vectortype value2 = tmpstack.top();
          tmpstack.pop();
          vectortype value1 = tmpstack.top();
          tmpstack.pop();
          if (value1.type == 1) {
            tmpvect.intvec = value1.intvec * value2.intvec;
            tmpvect.type = 1;
            tmpstack.push(tmpvect);
          } else if (value1.type == 2) {
            tmpvect.floatvec = value1.floatvec * value2.floatvec;
            tmpvect.type = 2;
            tmpstack.push(tmpvect);
          }
        } else {
          vectortype value1;
          value1.type = 0;
          value1.strvec = rvdata.value[i];
          tmpstack.push(value1);
        }
      } break;
    }
    /*
    else if(rvdata.type[i] == KETI_STRING){
        if(rvdata.value[i] == "+"){
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if(value1.type == 1){
                tmpvect.intvec = value1.intvec + value2.intvec;
                tmpvect.type = 1;
                tmpstack.push(tmpvect);
            }else if(value1.type == 2){
                tmpvect.intvec = value1.floatvec + value2.floatvec;
                tmpvect.type = 2;
                tmpstack.push(tmpvect);
            }
        }else if(rvdata.value[i] == "-"){
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if(value1.type == 1){
                tmpvect.intvec = value1.intvec - value2.intvec;
                tmpvect.type = 1;
                tmpstack.push(tmpvect);
            }else if(value1.type == 2){
                tmpvect.intvec = value1.floatvec - value2.floatvec;
                tmpvect.type = 2;
                tmpstack.push(tmpvect);
            }
        }else if(rvdata.value[i] == "/"){
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if(value1.type == 1){
                tmpvect.intvec = value1.intvec / value2.intvec;
                tmpvect.type = 1;
                tmpstack.push(tmpvect);
            }else if(value1.type == 2){
                tmpvect.intvec = value1.floatvec / value2.floatvec;
                tmpvect.type = 2;
                tmpstack.push(tmpvect);
            }
        }else if(rvdata.value[i] == "*"){
            vectortype value2 = tmpstack.top();
            tmpstack.pop();
            vectortype value1 = tmpstack.top();
            tmpstack.pop();
            if(value1.type == 1){
                tmpvect.intvec = value1.intvec * value2.intvec;
                tmpvect.type = 1;
                tmpstack.push(tmpvect);
            }else if(value1.type == 2){
                tmpvect.intvec = value1.floatvec * value2.floatvec;
                tmpvect.type = 2;
                tmpstack.push(tmpvect);
            }
        }else{
            vectortype value1;
            value1.type = 0;
            value1.strvec = rvdata.value[i];
            tmpstack.push(value1);
        }
    }else if(rvdata.type[i] == snippetsample::Snippet::DATE){
        vectortype value1;
        value1.type = 1;
        value1.intvec = stoi(rvdata.value[i]);
        tmpstack.push(value1);
    }else{
        //int 나 float등 과 같은 일반적인 상수
        if(rvdata.type[i] == snippetsample::Snippet::INT32 || rvdata.type[i] ==
    snippetsample::Snippet::INT64 || rvdata.type[i] ==
    snippetsample::Snippet::FLOAT32){ tmpvect.type = 1; tmpvect.intvec =
    stoi(rvdata.value[i]); tmpstack.push(tmpvect); }else{ tmpvect.type = 2;
            tmpvect.intvec = stod(rvdata.value[i]);
            tmpstack.push(tmpvect);
        }
    }*/
  }
  return tmpstack.top();
}

bool LikeSubString_v2(string lv, string rv) {  // % 위치 찾기
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

// vector<string> split(string str, char Delimiter)
// {
//     istringstream iss(str); // istringstream에 str을 담는다.
//     string buffer;          // 구분자를 기준으로 절삭된 문자열이 담겨지는
//     버퍼

//     vector<string> result;

//     // istringstream은 istream을 상속받으므로 getline을 사용할 수 있다.
//     while (getline(iss, buffer, Delimiter))
//     {
//         result.emplace_back(buffer); // 절삭된 문자열을 vector에 저장
//     }

//     return result;
// }

void JoinThread(SnippetStruct& snippet, BufferManager& buff) {
  time_t t = time(0);
  // cout <<"[Merge Query Manager] Start Join Table1.Name : " <<
  // snippet.tablename[0] << " | Table2.Name : " << snippet.tablename[1]  <<"
  // Time : " << t << endl;
  keti_log("Merge Query Manager",
           "Start Join Table1.Name : " + snippet.tablename[0] +
               " | Table2.Name : " + snippet.tablename[1] +
               " Time : " + std::to_string(t));
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < 2; i++) {
    vector<string> tmpvector;
    keti_log("Merge Query Manager",
             "Buffer Manager Access Get Table, Table Name : " +
                 snippet.tablename[i]);
    // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
    // Name : " << snippet.tablename[i] << endl;
    unordered_map<string, vector<vectortype>> table =
        buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      // cout << tmppair.first << " " << tmppair.second.size() << endl;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
    }
    tablenamelist.emplace_back(tmpvector);
  }

  string joinColumn1 = snippet.table_filter[0].LV.value[0];
  string joinColumn2;
  auto ttit =
      find(tablenamelist[0].begin(), tablenamelist[0].end(), joinColumn1);
  if (ttit != tablenamelist[0].end()) {
    joinColumn1 = snippet.table_filter[0].LV.value[0];
    joinColumn2 = snippet.table_filter[0].RV.value[0];
  } else {
    joinColumn1 = snippet.table_filter[0].RV.value[0];
    joinColumn2 = snippet.table_filter[0].LV.value[0];
  }
  int rownum = tablelist[joinColumn2].size();

  unordered_map<string, vector<vectortype>> savedTable;
  joinclass joinc;
  int threadnum;
  if (rownum < 8) {
    threadnum = rownum;
  } else {
    threadnum = 10;
  }
  boost::thread_group tg;
  for (int i = 0; i < threadnum; i++) {
    int start = rownum / threadnum * i;
    int finish = rownum / threadnum * (i + 1);
    if (i == threadnum - 1) {
      finish = rownum;
    }
    tg.create_thread(boost::bind(&joinclass::JoinThreadRun, &joinc, start,
                                 finish, tablelist, snippet, tablenamelist));
  }
  tg.join_all();

  savedTable = joinc.returntable();
  // cout << "end" << endl;
  time_t t1 = time(0);
  // cout  << "[Merge Query Manager] End Join Snippet " << snippet.query_id <<
  // "-" << snippet.work_id <<" Time : " << t1 << endl;
  keti_log("Merge Query Manager", "End Join Snippet " +
                                      std::to_string(snippet.query_id) + "-" +
                                      std::to_string(snippet.work_id) +
                                      " Time : " + std::to_string(t1));
  cout << "SaveTable Call in :: " << 3912 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

void joinclass::JoinThreadRun(
    int start, int finish, unordered_map<string, vector<vectortype>> tablelist,
    SnippetStruct& snippet, vector<vector<string>> tablenamelist) {
  string joinColumn1 = snippet.table_filter[0].LV.value[0];
  string joinColumn2;
  // cout << start << " " << finish << endl;
  auto ttit =
      find(tablenamelist[0].begin(), tablenamelist[0].end(), joinColumn1);
  if (ttit != tablenamelist[0].end()) {
    joinColumn1 = snippet.table_filter[0].LV.value[0];
    joinColumn2 = snippet.table_filter[0].RV.value[0];
  } else {
    joinColumn1 = snippet.table_filter[0].RV.value[0];
    joinColumn2 = snippet.table_filter[0].LV.value[0];
  }

  // cout << "[Merge Query Manager] JoinColumn1 : " << joinColumn1 << " |
  // JoinColumn2 : " << joinColumn2 << endl; cout << "[Merge Query Manager] " <<
  // joinColumn1 <<".Rows : " << tablelist[joinColumn1].size() << " | "<<
  // joinColumn2 << ".Rows : " <<tablelist[joinColumn2].size() << endl;
  keti_log("Merge Query Manager",
           "JoinColumn1 : " + joinColumn1 + " | JoinColumn2 : " + joinColumn2);
  keti_log("Merge Query Manager",
           joinColumn1 +
               ".Rows : " + std::to_string(tablelist[joinColumn1].size()) +
               " | " + joinColumn2 +
               ".Rows : " + std::to_string(tablelist[joinColumn2].size()));
  int countcout = 0;
  for (int i = 0; i < tablelist[joinColumn1].size(); i++) {
    for (int j = start; j < finish; j++) {
      bool savedflag = true;
      if (tablelist[joinColumn1][i].type == 1) {
        if (tablelist[joinColumn1][i].intvec ==
            tablelist[joinColumn2][j].intvec) {
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.value.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
              if (countcout == 0) {
                // cout << "[Merge Query Manager] Join Column 1 : " <<
                // tmpcolumn1 << " Join Column 2 : " << tmpcolumn2 << endl;
                keti_log("Merge Query Manager",
                         "Join Column 1 : " + tmpcolumn1 +
                             " Join Column 2 : " + tmpcolumn2);
                countcout++;
              }
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    vector<vectortype> tmptable = tabledata.second;
                    insertTable(tabledata.first, tmptable[i]);
                  } else {
                    vector<vectortype> tmptable = tabledata.second;
                    insertTable(tabledata.first, tmptable[j]);
                  }
                }
              }
            }
          }
        }
      } else if (tablelist[joinColumn1][i].type == 2) {
        if (tablelist[joinColumn1][i].floatvec ==
            tablelist[joinColumn2][j].floatvec) {
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.type.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              vector<vectortype> tmptable = tabledata.second;
              insertTable(tabledata.first, tmptable[j]);
            }
          }
        }

      } else if (tablelist[joinColumn1][i].type == 0) {
        if (tablelist[joinColumn1][i].strvec ==
            tablelist[joinColumn2][j].strvec) {
          for (int k = 1; k < snippet.table_filter.size(); k++) {
            string tmpcolumn1;
            string tmpcolumn2;
            if (snippet.table_filter[k].LV.type.size() > 0) {
              tmpcolumn1 = snippet.table_filter[k].LV.value[0];
              tmpcolumn2 = snippet.table_filter[k].RV.value[0];
            } else {
              continue;
            }
            if (tablelist[tmpcolumn1][i].type == 1) {
              if (tablelist[tmpcolumn1][i].intvec ==
                  tablelist[tmpcolumn2][j].intvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else if (tablelist[tmpcolumn1][i].type == 2) {
              if (tablelist[tmpcolumn1][i].floatvec ==
                  tablelist[tmpcolumn2][j].floatvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            } else {
              if (tablelist[tmpcolumn1][i].strvec ==
                  tablelist[tmpcolumn2][j].strvec) {
                continue;
              } else {
                savedflag = false;
                break;
              }
            }
          }
          if (savedflag) {
            for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
              pair<string, vector<vectortype>> tabledata;
              tabledata = *it;
              for (int k = 0; k < tablenamelist.size(); k++) {
                auto tit = find(tablenamelist[k].begin(),
                                tablenamelist[k].end(), tabledata.first);
                if (tit != tablenamelist[k].end()) {
                  if (k == 0) {
                    vector<vectortype> tmptable = tabledata.second;
                    insertTable(tabledata.first, tmptable[i]);
                  } else {
                    vector<vectortype> tmptable = tabledata.second;
                    insertTable(tabledata.first, tmptable[j]);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

void Storage_Filter_Thread(SnippetStruct& snippet, BufferManager& buff) {
  unordered_map<string, vector<vectortype>> tablelist;
  vector<vector<string>> tablenamelist;
  for (int i = 0; i < snippet.tablename.size(); i++) {
    vector<string> tmpvector;
    unordered_map<string, vector<vectortype>> table;
    // unordered_map<string,vector<any>> table =
    // GetBufMTable(snippet.tablename[i], snippet, buff);
    if (snippet.tablename.size() > 1) {
      // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
      // Name : " << snippet.tablename[i] << endl;
      keti_log("Merge Query Manager",
               "Buffer Manager Access Get Table, Table Name : " +
                   snippet.tablename[i]);
      table =
          buff.GetTableData(snippet.query_id, snippet.tablename[i]).table_data;
    } else {
      // cout << "[Merge Query Manager] Buffer Manager Access Get Table, Table
      // Name : " << snippet.tableAlias << endl;
      keti_log("Merge Query Manager",
               "Buffer Manager Access Get Table, Table Name : " +
                   snippet.tableAlias);
      table =
          buff.GetTableData(snippet.query_id, snippet.tableAlias).table_data;
    }
    // tablelist.emplace_back(table);
    for (auto it = table.begin(); it != table.end(); it++) {
      pair<string, vector<vectortype>> tmppair = *it;
      // cout << tmppair.first << " " << tmppair.second.size() << endl;
      tablelist.insert(make_pair(tmppair.first, tmppair.second));
      tmpvector.emplace_back(tmppair.first);
    }
    tablenamelist.emplace_back(tmpvector);
  }
  // cout << snippet.table_filter.size() << endl;

  auto it = tablelist.begin();
  pair<string, vector<vectortype>> tmppair = *it;
  int rownum = tmppair.second.size();
  // cout <<"start filter" << endl;
  unordered_map<string, vector<vectortype>> savedTable;
  FilterClass filterc;
  int threadnum = 20;
  boost::thread_group tg;
  for (int i = 0; i < threadnum; i++) {
    int start = rownum / threadnum * i;
    int finish = rownum / threadnum * (i + 1);
    if (i == threadnum - 1) {
      finish = rownum;
    }
    tg.create_thread(boost::bind(&FilterClass::FilterThreadRun, &filterc, start,
                                 finish, tablelist, snippet, tablenamelist));
  }
  tg.join_all();
  savedTable = filterc.returntable();
  //단일 테이블이다 = 모든 컬럼의 row수가 같다

  if (snippet.tablename.size() == 1) {
    buff.DeleteTableData(snippet.query_id, snippet.tableAlias);
  }
  cout << "SaveTable Call in :: " << 4184 << endl;
  buff.SaveTableData(snippet.query_id, snippet.tableAlias, savedTable);
}

void FilterClass::FilterThreadRun(
    int start, int finish, unordered_map<string, vector<vectortype>> tablelist,
    SnippetStruct& snippet, vector<vector<string>> tablenamelist) {
  bool rvflag = false;
  for (int i = 0; i < snippet.table_filter.size(); i++) {
    // cout << i << endl;
    // if(snippet.table_filter[i].RV.type)
    for (int j = 0; j < snippet.table_filter[i].RV.type.size(); j++) {
      if (snippet.table_filter[i].RV.type[j] == 10) {
        rvflag = true;
        break;
      }
    }
  }
  vectortype rv;
  vector<vectortype> rvlist;
  if (!rvflag) {
    for (int j = 0; j < snippet.table_filter.size(); j++) {
      if (snippet.table_filter[j].RV.type.size() == 0) {
        rvlist.emplace_back(rv);
      } else {
        rvlist.emplace_back(GetRV(snippet.table_filter[j].RV, 0, tablelist));
      }
    }
  }
  for (int i = start; i < finish; i++) {
    // cout << i << endl;
    bool passed = false;
    bool isSaved = true;
    bool CV = false;
    bool notflag = false;
    if (i % 1000 == 0) {
      cout << i << endl;
      // if(i != 0){
      //     auto it = savedTable.begin();
      //     pair<string,vector<vectortype>> tmp = *it;
      //     cout << tmp.second.size() << endl;
      // }
    }
    for (int j = 0; j < snippet.table_filter.size(); j++) {
      // cout << j << endl;
      // cout << j << endl;
      if (passed) {
        if (snippet.table_filter[j].filteroper ==
            static_cast<int>(KETI_OPER_TYPE::KETI_OR)) {
          passed = false;
        }
        continue;
      }
      switch (snippet.table_filter[j].filteroper) {
        case static_cast<int>(KETI_OPER_TYPE::KETI_GE): {
          /* code */
          // lv가 여러개일 가능성을 생각해야한다?
          //  time_t t1 = time(0);
          //  cout << t1 << endl;
          //  vectortype lv;
          //  vectortype rv;
          //  if(snippet.table_filter[j].LV.type[0] == KETI_COLUMN){
          //      lv = tablelist[snippet.table_filter[j].LV.value[0]][i];
          //  }
          //  if(snippet.table_filter[j].RV.type[0] == KETI_DATE){
          //      rv.type = 1;
          //      rv.intvec = stoi(snippet.table_filter[j].RV.value[0]);
          //  }
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.intvec << endl;
          if (rvflag) {
            rv = GetRV(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          // t1 = time(0);
          // cout << t1 << endl;
          // cout << lv.type << " " << rv.type << endl;
          if (lv.type == 0) {
            if (lv.strvec >= rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            // cout << lv.intvec << " " << rv.intvec << endl;
            if (lv.intvec >= rv.intvec) {
              // cout << lv.intvec << " " << rv.intvec << endl;
              // cout << "fliter" << endl;
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec >= rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_LE): {
          // vectortype lv;
          // vectortype rv;
          // if(snippet.table_filter[j].LV.type[0] == KETI_COLUMN){
          //     lv = tablelist[snippet.table_filter[j].LV.value[0]][i];
          // }
          // if(snippet.table_filter[j].RV.type[0] == KETI_DATE){
          //     rv.type = 1;
          //     rv.intvec = stoi(snippet.table_filter[j].RV.value[0]);
          // }
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.intvec << endl;
          if (rvflag) {
            rv = GetRV(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          if (lv.type == 0) {
            if (lv.strvec <= rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            // cout << lv.intvec << " " << rv.intvec << endl;
            if (lv.intvec <= rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec <= rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_GT): {
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.type << endl;
          if (rvflag) {
            rv = GetRV(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          if (lv.type == 0) {
            if (lv.strvec > rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            if (lv.intvec > rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            // cout << lv.floatvec << " " << rv.floatvec << endl;
            if (lv.floatvec > rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_LT): {
          // vectortype lv;
          // vectortype rv;
          // if(snippet.table_filter[j].LV.type[0] == KETI_COLUMN){
          //     lv = tablelist[snippet.table_filter[j].LV.value[0]][i];
          // }
          // if(snippet.table_filter[j].RV.type[0] == KETI_DATE){
          //     rv.type = 1;
          //     rv.intvec = stoi(snippet.table_filter[j].RV.value[0]);
          // }
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          // cout << lv.intvec << endl;
          // cout << rvflag << endl;
          if (rvflag) {
            // cout << 1 << endl;
            rv = GetRV(snippet.table_filter[j].RV, i, tablelist);
            // cout << 2 << endl;
          } else {
            rv = rvlist[j];
          }
          // cout << lv.type << endl;
          // cout << rv.type << endl;
          // break;
          if (lv.type == 0) {
            if (lv.strvec < rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            // cout << lv.intvec << " " << rv.intvec << endl;
            if (lv.intvec < rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (rv.type == 2) {
              if (lv.floatvec < rv.floatvec) {
                CV = true;
              } else {
                CV = false;
              }
            } else {
              if (lv.floatvec < rv.intvec) {
                CV = true;
              } else {
                CV = false;
              }
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_ET): {
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          if (rvflag) {
            rv = GetRV(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          // cout << lv.type << endl;
          if (lv.type == 0) {
            if (lv.strvec == rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            if (lv.intvec == rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec == rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_NE): {
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          if (rvflag) {
            rv = GetRV(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          if (lv.type == 0) {
            if (lv.strvec != rv.strvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 1) {
            if (lv.intvec != rv.intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else {
            if (lv.floatvec != rv.floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
        } break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_LIKE): {
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          if (rvflag) {
            rv = GetRV(snippet.table_filter[j].RV, i, tablelist);
          } else {
            rv = rvlist[j];
          }
          CV = LikeSubString_v2(lv.strvec, rv.strvec);
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_BETWEEN): {
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          vector<vectortype> rv;
          for (int k = 0; k < snippet.table_filter[j].RV.value.size(); k++) {
            vectortype tmpv;
            if (snippet.table_filter[j].RV.type[k] ==
                static_cast<int>(KETI_Type::KETI_COLUMN)) {
              tmpv = tablelist[snippet.table_filter[j].RV.value[k]][i];
              rv.emplace_back(tmpv);
            } else if (lv.type == 1) {
              tmpv.type = 1;
              tmpv.intvec = stoi(snippet.table_filter[j].RV.value[k]);
              rv.emplace_back(tmpv);
            } else if (lv.type == 2) {
              tmpv.type = 2;
              tmpv.intvec = stod(snippet.table_filter[j].RV.value[k]);
              rv.emplace_back(tmpv);
            }
          }
          if (lv.type == 1) {
            if (lv.intvec >= rv[0].intvec && lv.intvec <= rv[1].intvec) {
              CV = true;
            } else {
              CV = false;
            }
          } else if (lv.type == 2) {
            if (lv.floatvec >= rv[0].floatvec &&
                lv.floatvec <= rv[1].floatvec) {
              CV = true;
            } else {
              CV = false;
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_IN): {
          vectortype lv = GetLV(snippet.table_filter[j].LV, i, tablelist);
          string tmpstring;
          if (lv.type == 0) {
            tmpstring = lv.strvec;
          } else if (lv.type == 1) {
            tmpstring = to_string(lv.intvec);
          } else {
            tmpstring = to_string(lv.floatvec);
          }
          if (snippet.table_filter[j].RV.type.size() == 1 &&
              snippet.table_filter[j].RV.type[0] == 10) {
            for (int k = 0;
                 k < tablelist[snippet.table_filter[j].RV.value[0]].size();
                 k++) {
              vectortype rv = tablelist[snippet.table_filter[j].RV.value[0]][k];
              if (lv.type == 1) {
                if (rv.type == 1) {
                  if (lv.intvec == rv.intvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                } else {
                  if (lv.intvec == rv.floatvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                }
              } else if (lv.type == 2) {
                if (rv.type == 1) {
                  if (lv.floatvec == rv.intvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                } else {
                  if (lv.floatvec == rv.floatvec) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                }
              } else {
                if (rv.type == 1) {
                  if (lv.strvec == to_string(rv.intvec)) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                } else {
                  if (lv.strvec == to_string(rv.floatvec)) {
                    CV = true;
                    break;
                  } else {
                    CV = false;
                  }
                }
              }
            }
          } else {
            for (int k = 0; k < snippet.table_filter[j].RV.type.size(); k++) {
              if (tmpstring == snippet.table_filter[j].RV.value[k]) {
                CV = true;
                break;
              } else {
                CV = false;
              }
            }
          }
          if (notflag) {
            if (CV) {
              CV = false;
            } else {
              CV = true;
            }
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_IS):
          break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_ISNOT):
          break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_NOT):
          if (notflag) {
            notflag = false;
          } else {
            notflag = true;
          }
          break;
        case static_cast<int>(KETI_OPER_TYPE::KETI_AND): {
          if (CV) {
            isSaved = true;
          } else {
            isSaved = false;
            passed = true;
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_OR): {
          if (CV) {
            isSaved = true;
            break;
          } else {
            passed = false;
            continue;
          }
          break;
        }
        case static_cast<int>(KETI_OPER_TYPE::KETI_SUBSTRING): {
          if (CV) {
            isSaved = true;
            break;
          } else {
            passed = false;
            continue;
          }
          break;
        }
        default:
          break;
      }
    }
    if (isSaved && CV) {
      //로우 살림
      // cout << "saved" << endl;
      // break;
      // continue;
      for (auto it = tablelist.begin(); it != tablelist.end(); it++) {
        pair<string, vector<vectortype>> tabledata;
        tabledata = *it;
        // savedTable[tabledata.first].emplace_back(tabledata.second[i]);
        auto tit = find(tablenamelist[0].begin(), tablenamelist[0].end(),
                        tabledata.first);
        if (tit != tablenamelist[0].end()) {
          // cout << tabledata.first << endl;
          vector<vectortype> tmptable = tabledata.second;
          // cout << i << endl;
          // cout << tmptable[i].intvec << endl;
          insertTable(tabledata.first, tmptable[i]);
          // savedTable[tabledata.first].emplace_back(tmptable[i]);
        }
      }
    }
  }
}
