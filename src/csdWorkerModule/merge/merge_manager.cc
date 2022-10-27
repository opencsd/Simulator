#include "merge_manager.hpp"

pair<T, int> stack_charToValue(char* dest, int type, int len);
int stack_valueToChar(char* dest, int dest_type, T value, int type);
void getColOffset(const char* origin_row_data, FilterInfo filter_info,
                  int* col_offset);
int calculCase(FilterInfo filter_info, char* origin_row_data, int* col_offset,
               int l, char* dest);
int calculSubstring(FilterInfo filter_info, char* origin_row_data,
                    int* col_offset, int l, char* dest);
int calculExtract(FilterInfo filter_info, char* origin_row_data,
                  int* col_offset, int l, char* dest);
int calculPostfix(vector<string> values, vector<int> types,
                  FilterInfo filter_info, char* origin_row_data,
                  int* col_offset, char* dest, int projection_datatype);

inline std::string& rtrim_(std::string& s, const char* t = " \t\n\r\f\v\0") {
  s.erase(s.find_last_not_of(t) + 1);
  return s;
}

inline std::string& ltrim_(std::string& s, const char* t = " \t\n\r\f\v\0") {
  s.erase(0, s.find_first_not_of(t));
  return s;
}

inline std::string& trim_(std::string& s, const char* t = " \t\n\r\f\v\0") {
  return ltrim_(rtrim_(s, t), t);
}
void MergeManager::push_work(Result result) { MergeQueue.push_work(result); }
void MergeManager::Merging() {
  // cout << "<-----------  Merge Layer Running...  ----------->\n";
  while (1) {
    Result result = MergeQueue.wait_and_pop();

    // sleep(5);

    // 테스트 출력
    // cout << "---------------Filtered Block Info-----------------" << endl;
    // cout << "| work id: " << result.work_id << " | length: " << result.length
    //      << " | row count: " << result.row_count << endl;
    // // cout << "result.length: " << result.length << endl;
    // // cout << "result.row_offset: ";
    // // for(int i = 0; i < result.row_offset.size(); i++){
    // //     printf("%d ",result.row_offset[i]);
    // // }
    // // cout << "\n----------------------------------------------\n";
    // // for(int i = 0; i < result.length; i++){
    // //     printf("%02X",(u_char)result.data[i]);
    // // }
    // cout << "\n------------------------------------------------\n";

    MergeBlock(result);
  }
}

void MergeManager::MergeBlock(Result& result) {
  int origin_row_len = 0;
  int projection_type;
  pair_key key = make_pair(result.query_id, result.work_id);
  cout << "[CSD Merge] Start Merging... (FileName : " << result.sst_name << ")"
       << endl;
  // Key에 해당하는 블록버퍼가 없다면 생성
  /*cout << "[MergeManager]Total Block CNT : " << result.total_block_count
       << endl;*/
  if (m_MergeManager.find(key) == m_MergeManager.end()) {
    MergeResult mergeResult(result.query_id, result.work_id, result.csd_name,
                            result.table_name, result.sst_name);
    m_MergeManager[key] = mergeResult;
  }
  int forFilterIndex = 0;
  for (int i = 0; i < result.row_count; i++) {
    // cout << "[CSD Merge] Merging Data... (RowCNT : " << i << ")" << endl;
    if (result.filter_info.filteredIndex[forFilterIndex] == i) {
      forFilterIndex++;
      continue;
    } else {
      for (int j = 0; j < result.filter_info.columnAlias.size(); j++) {
        result.filter_info.mergedData[result.filter_info.columnAlias[j]]
            .emplace_back(
                calculPostfix(result.filter_info.column_projection[j]));
      }
    }
  }
}

typeVar MergeManager::calculPostfix(Projection projectionInfo) {
  bool inInteger = true;
  for (int i = 0; i < projectionInfo.values.size(); i++) {
    switch (projectionInfo.types[i]) {
      case /* constant-expression */:
        /* code */
        break;

      default:
        break;
    }
  }
  stack<int> stack;
  int len = post_exp.length();
  // loop to iterate through the expression
  for (int i = 0; i < len; i++) {
    // if the character is an operand we push it in the stack
    // we have considered single digits only here
    if (post_exp[i] >= '0' && post_exp[i] <= '9') {
      stack.push(post_exp[i] - '0');
    }
    // if the character is an operator we enter else block
    else {
      // we pop the top two elements from the stack and save them in two
      // integers
      int a = stack.top();
      stack.pop();
      int b = stack.top();
      stack.pop();
      // performing the operation on the operands
      switch (post_exp[i]) {
        case '+':  // addition
          stack.push(b + a);
          break;
        case '-':  // subtraction
          stack.push(b - a);
          break;
        case '*':  // multiplication
          stack.push(b * a);
          break;
        case '/':  // division
          stack.push(b / a);
          break;
        case '^':  // exponent
          stack.push(pow(b, a));
          break;
      }
    }
  }
}
int calculPostfix(vector<string> values, vector<int> types,
                  FilterInfo filter_info, char* origin_row_data,
                  int* col_offset, char* dest, int projection_datatype) {
  stack<pair<T, int>> oper_stack;
  int type, idx, offset, dest_type, col_len, return_len;
  string value;
  for (int k = 0; k < values.size(); k++) {
    type = types[k];
    value = values[k];
    if (type != OPERATOR) {  //피연산자
      switch (type) {
        case INT8:
        case INT16:
        case INT32: {
          T t;
          t.varInt = stoi(value);
          oper_stack.push(make_pair(t, INT_));
          break;
        }
        case INT64: {
          T t;
          t.varLong = stoll(value);
          oper_stack.push(make_pair(t, Long_));
          break;
        }
        case FLOAT32: {
          T t;
          t.varDouble = stof(value);  //여기바꿈 더블로
          oper_stack.push(make_pair(t, DOUBLE_));
          break;
        }
        case FLOAT64: {
          T t;
          t.varDouble = stod(value);
          oper_stack.push(make_pair(t, DOUBLE_));
          break;
        }
        case STRING: {
          T t;
          t.varString = value;
          oper_stack.push(make_pair(t, STRING_));
          break;
        }
        case COLUMN: {
          // cout << "value: " << value << endl;//테스트출력
          idx = filter_info.colindexmap[value];
          offset = col_offset[idx];
          dest_type = filter_info.table_datatype[idx];
          col_len = col_offset[idx + 1] - col_offset[idx];
          // cout << "idx: " << idx << "|offset: " << offset << "|col_len: " <<
          // col_len << endl;//테스트출력
          char dest[col_len];
          memcpy(dest, origin_row_data + offset, col_len);
          // cout << "col_data: ";//테스트출력
          // for(int k = 0; k < col_len; k++){
          //     printf("%02X ",(u_char)dest[k]);
          // }
          // cout << endl;
          pair<T, int> pair = stack_charToValue(dest, dest_type, col_len);
          oper_stack.push(pair);
          break;
        }
        case NUMERIC: {
        }
        case DATE: {
        }
      }
    } else {  //연산자
      pair<T, int> op2 = oper_stack.top();
      oper_stack.pop();
      pair<T, int> op1 = oper_stack.top();
      oper_stack.pop();

      if (value == "+") {
        if (op1.second == INT_ && op2.second == DOUBLE_) {
          double result = op1.first.varInt + op2.first.varDouble;
          // cout << op1.first.varInt << "+" << op2.first.varDouble << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else if (op1.second == DOUBLE_ && op2.second == INT_) {
          double result = op1.first.varDouble + op2.first.varInt;
          // cout << op1.first.varDouble << "+" << op2.first.varInt << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else {
          cout << "merge_m>operator>plus>else" << endl;
        }
      } else if (value == "-") {
        if (op1.second == INT_ && op2.second == DOUBLE_) {
          double result = op1.first.varInt - op2.first.varDouble;
          // cout << op1.first.varInt << "-" << op2.first.varDouble << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else if (op1.second == DOUBLE_ && op2.second == INT_) {
          double result = op1.first.varDouble - op2.first.varInt;
          // cout << op1.first.varDouble << "-" << op2.first.varInt << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else {
          cout << "merge_m>operator>minus>else" << endl;
        }
      } else if (value == "*") {
        if (op1.second == DOUBLE_ && op2.second == DOUBLE_) {
          double result = op1.first.varDouble * op2.first.varDouble;
          // cout << op1.first.varDouble << "*" << op2.first.varDouble << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else if (op1.second == DOUBLE_ && op2.second == INT_) {
          double result = op1.first.varDouble * op2.first.varInt;
          // cout << op1.first.varDouble << "*" << op2.first.varDouble << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else {
          cout << "merge_m>operator>multiple>else" << endl;
        }
      } else if (value == "/") {
        if (op1.second == INT_ && op2.second == DOUBLE_) {
          double result = op1.first.varInt / op2.first.varDouble;
          // cout << op1.first.varInt << "/" << op2.first.varDouble << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else if (op1.second == DOUBLE_ && op2.second == INT_) {
          double result = op1.first.varDouble / op2.first.varInt;
          // cout << op1.first.varDouble << "/" << op2.first.varInt << "=" <<
          // result << endl;//테스트출력
          T t;
          t.varDouble = result;
          oper_stack.push(make_pair(t, DOUBLE_));
        } else {
          cout << "merge_m>operator>divide>else" << endl;
        }
      } else if (value == "=") {
        if (op1.second == STRING_ && op2.second == STRING_) {
          // cout << "= " << "|" << op1.first.varString << "|" <<
          // op2.first.varString << "|" << endl;//테스트출력
          if (op1.first.varString == op2.first.varString) {
            // cout << "same" << endl;//테스트출력
            dest[0] = 1;
            return 1;
          } else {
            // cout << "differ" << endl;//테스트출력
            dest[0] = 0;
            return 1;
          }
        } else {
          cout << "merge_m>operator>equal>else" << endl;
        }
      } else if (value == "<>") {
        if (op1.second == STRING_ && op2.second == STRING_) {
          cout << "<>: " << op1.first.varString << " " << op2.first.varString
               << endl;  //테스트출력
          if (op1.first.varString != op2.first.varString) {
            dest[0] = 1;
            return 1;
          } else {
            dest[0] = 0;
            return 1;
          }
        } else {
          cout << "merge_m>operator>not equal>else" << endl;
        }
      } else if (value == "LIKE") {
        if (op1.second == STRING_ && op2.second == STRING_) {
          string str = op2.first.varString;
          string target = op1.first.varString;
          if (str.substr(0, 1) == "%" &&
              str.substr(str.length() - 1) == "%") {  //'%str%'
            string find = str.substr(1, str.length() - 2);
            // cout << "find: " << find << " | target: " << target <<
            // endl;//테스트출력
            if (target.find(find) != string::npos) {
              dest[0] = 1;
              return 1;
            } else {
              dest[0] = 0;
              return 1;
            }
          } else if (str.substr(0, 1) == "%") {  //'%str'
            string find = str.substr(1);
            string comp = target.substr(target.length() - find.length());
            // cout << "find: " << find << " | comp: " << comp <<
            // endl;//테스트출력
            if (find == comp) {
              dest[0] = 1;
              return 1;
            } else {
              dest[0] = 0;
              return 1;
            }
          } else if (str.substr(str.length() - 1) == "%") {  //'str%'
            string find = str.substr(0, str.length() - 1);
            string comp = target.substr(0, find.length());
            // cout << "find: " << find << " | comp: " << comp <<
            // endl;//테스트출력
            if (find == comp) {
              dest[0] = 1;
              return 1;
            } else {
              dest[0] = 0;
              return 1;
            }
          }

        } else {
          cout << "merge_m>operator>like>else" << endl;
        }
      } else {
        cout << "else: " << value << endl;
      }
    }
  }

  // convert result -> char array
  T postfix_result = oper_stack.top().first;
  int postfix_type = oper_stack.top().second;
  oper_stack.pop();
  int len = stack_valueToChar(dest, projection_datatype, postfix_result,
                              postfix_type);
  col_len = len;

  return len;
}

int calculCase(FilterInfo filter_info, char* origin_row_data, int* col_offset,
               int l, char* dest) {
  int case_len = filter_info.column_projection[l].values.size();
  vector<pair<int, int>> when_then_offset;
  int else_offset, when, then = 0;
  int dest_type = filter_info.projection_datatype[l];

  for (int i = 1; i < case_len; i++) {
    if (filter_info.column_projection[l].values[i] == "WHEN") {
      when = i;
    } else if (filter_info.column_projection[l].values[i] == "THEN") {
      then = i;
      when_then_offset.push_back({when, then});
      // cout << "when: " << when << "|then: " << then<< endl;//테스트출력
    } else if (filter_info.column_projection[l].values[i] == "ELSE") {
      else_offset = i;
      // cout << "else: " << else_offset << endl;//테스트출력
    }
  }

  vector<pair<int, int>>::iterator iter;
  for (iter = when_then_offset.begin(); iter != when_then_offset.end();
       ++iter) {
    when = (*iter).first;
    then = (*iter).second;
    int i = when + 1;
    bool passed = true;
    char passed_[1];

    vector<string> values;
    vector<int> types;
    while (i < then) {
      if (filter_info.column_projection[l].values[i] == "AND") {
        // cout << "AND" << endl;//테스트출력
        calculPostfix(values, types, filter_info, origin_row_data, col_offset,
                      passed_, 0);
        if (passed_[0] == 1) {
          // cout << "passed" << endl;//테스트출력
          values.clear();
          types.clear();
          i++;
          continue;
        } else {
          passed = false;
          break;
        }
      } else if (filter_info.column_projection[l].values[i] == "OR") {
        // cout << "OR" << endl;//테스트출력
        calculPostfix(values, types, filter_info, origin_row_data, col_offset,
                      passed_, 0);
        if (passed_[0] == 1) {
          // cout << "passed" << endl;//테스트출력
          break;
        } else {
          values.clear();
          types.clear();
          i++;
          continue;
        }
      } else {
        values.push_back(filter_info.column_projection[l].values[i]);
        types.push_back(filter_info.column_projection[l].types[i]);
      }

      i++;

      if (i == then) {
        calculPostfix(values, types, filter_info, origin_row_data, col_offset,
                      passed_, 0);
        if (passed_[0]) {
          // cout << "passed" << endl;//테스트출력
          passed = true;
        } else {
          passed = false;
        }
        values.clear();
        types.clear();
      }
    }

    i = then + 1;

    if (passed) {
      // cout << "THEN" << endl;//테스트출력
      values.clear();
      types.clear();
      while (true) {
        if (filter_info.column_projection[l].values[i] == "WHEN" ||
            filter_info.column_projection[l].values[i] == "ELSE" ||
            filter_info.column_projection[l].values[i] == "END") {
          break;
        } else {
          values.push_back(filter_info.column_projection[l].values[i]);
          types.push_back(filter_info.column_projection[l].types[i]);
        }
        i++;
      }

      if (values.size() == 1) {
        int value = stoi(values[0]);
        memcpy(dest, &value, sizeof(int));
        return 4;
      } else {
        int l = calculPostfix(values, types, filter_info, origin_row_data,
                              col_offset, dest, dest_type);
        return l;
      }
    }
  }

  if (else_offset != 0) {
    // cout << "ELSE" << endl;//테스트출력
    // int else_len = case_len - else_offset - 1;
    int result = stoi(filter_info.column_projection[l].values[else_offset + 1]);

    switch (dest_type) {
      case MySQL_NEWDECIMAL: {
        // decimal(15,2)만 고려한 상황 -> col_len = 7 (integer:6/real:1)
        for (int i = 0; i < 7; i++) {
          dest[i] = 0x00;
        }
        dest[0] = 0x80;
        return 7;
      }
      case MySQL_INT32: {
        memcpy(dest, &result, sizeof(int));
        return 4;
      }
      default: {
        cout << "else clause dest_type error" << endl;
      }
    }
  }

  // NULL값 반환
  return 0;
}

int calculSubstring(FilterInfo filter_info, char* origin_row_data,
                    int* col_offset, int l, char* dest) {
  string str = filter_info.column_projection[l].values[1];
  int type = filter_info.column_projection[l].types[1];
  int start_offset = stoi(filter_info.column_projection[l].values[2]) - 1;
  int read_length;

  if (type == COLUMN) {
    int idx = filter_info.colindexmap[str];
    int offset = col_offset[idx];
    int col_len = col_offset[idx + 1] - col_offset[idx];
    char col_data[col_len];

    if (filter_info.column_projection[l].values.size() == 4) {
      read_length = stoi(filter_info.column_projection[l].values[3]);
    } else {
      read_length = col_len - start_offset;
    }

    memcpy(col_data, origin_row_data + offset, col_len);
    memcpy(dest, col_data + start_offset, read_length);
  } else if (type == STRING) {
    int col_len = str.length();
    char col_data[col_len];

    if (filter_info.column_projection[l].values.size() == 4) {
      read_length = stoi(filter_info.column_projection[l].values[3]);
    } else {
      read_length = col_len - start_offset;
    }

    strcpy(col_data, str.c_str());
    memcpy(dest, col_data + start_offset, read_length);
  } else {
    cout << "calcul Substring > not defined : " << type << endl;
  }

  return read_length;
}

int calculExtract(FilterInfo filter_info, char* origin_row_data,
                  int* col_offset, int l, char* dest) {
  //["EXTRACT","YEAR",col_name]
  string unit = filter_info.column_projection[l].values[1];
  string col = filter_info.column_projection[l].values[2];
  int type = filter_info.column_projection[l].types[2];
  int value, result;

  if (type == COLUMN) {
    int idx = filter_info.colindexmap[col];
    int offset = col_offset[idx];
    int col_len = col_offset[idx + 1] - col_offset[idx];
    char dest[col_len + 1];
    memcpy(dest, origin_row_data + offset, col_len);
    dest[3] = 0x00;
    value = *((int*)dest);
  } else {
    cout << "calcul Extract > not defined : " << type << endl;
  }

  if (unit == "YEAR") {
    result = value / 512;
  } else if (unit == "MONTH") {
    value %= 512;
    result = value / 32;
  } else if (unit == "DAY") {
    value = value % 512;
    value %= 32;
    result = value;
  } else {
    cout << "calcul Extract > not defined : " << unit << endl;
  }

  memcpy(dest, &result, sizeof(int));
  return 4;
}

pair<T, int> stack_charToValue(char* src, int type, int len) {
  pair<T, int> result;
  switch (type) {
    case MySQL_BYTE: {
      int value = *((int8_t*)src);
      T t;
      t.varInt = value;
      result = make_pair(t, INT_);
      break;
    }
    case MySQL_INT16: {
      int value = *((int16_t*)src);
      T t;
      t.varInt = value;
      result = make_pair(t, INT_);
      break;
    }
    case MySQL_INT32: {
      int value = *((int32_t*)src);
      T t;
      t.varInt = value;
      result = make_pair(t, INT_);
      break;
    }
    case MySQL_INT64: {
      int64_t value = *((int64_t*)src);
      T t;
      t.varInt = value;
      result = make_pair(t, Long_);
      break;
    }
    case MySQL_FLOAT32: {
      float value = *((float*)src);
      T t;
      t.varFloat = value;
      result = make_pair(t, FLOAT_);
      break;
    }
    case MySQL_DOUBLE: {
      double value = *((double*)src);
      T t;
      t.varFloat = value;
      result = make_pair(t, DOUBLE_);
      break;
    }
    case MySQL_NEWDECIMAL: {
      // decimal(15,2)만 고려한 상황 -> col_len = 7 (integer:6/real:1)
      bool is_negative = false;
      if (std::bitset<8>(src[0])[7] == 0) {  //음수일때 not +1
        is_negative = true;
        for (int i = 0; i < 7; i++) {
          src[i] = ~src[i];  // not연산
        }
        // src[6] = src[6] +1;//+1
      }
      char integer[8];
      char real[1];
      memset(&integer, 0, 8);
      for (int k = 0; k < 4; k++) {
        integer[k] = src[5 - k];
      }
      real[0] = src[6];
      int64_t ivalue = *((int64_t*)integer);
      double rvalue = *((int8_t*)real);
      rvalue *= 0.01;
      double value = ivalue + rvalue;
      // cout << "ivalue:" << ivalue << " |rvalue:" << rvalue << " |value:" <<
      // value << endl; //테스트출력
      T t;
      if (is_negative) {
        value *= -1;
      }
      t.varDouble = value;
      result = make_pair(t, DOUBLE_);
      break;
    }
    case MySQL_DATE: {
      char tempbuf[len + 1];  // col_len = 3
      memcpy(tempbuf, src, len);
      tempbuf[3] = 0x00;
      int value = *((int*)tempbuf);
      T t;
      t.varInt = value;
      result = make_pair(t, INT_);
      break;
    }
    case MySQL_TIMESTAMP: {
      char tempbuf[len];  // col_len = 4
      memcpy(tempbuf, src, len);
      int value = *((int*)tempbuf);
      T t;
      t.varInt = value;
      result = make_pair(t, INT_);
      break;
    }
    case MySQL_STRING:
    case MySQL_VARSTRING: {
      char tempbuf[len + 1];
      memcpy(tempbuf, src, len);
      tempbuf[len] = '\0';
      string value(tempbuf);
      T t;
      t.varString = trim_(value);  //이렇게 하면 안되는 경우는?
      result = make_pair(t, STRING_);
      break;
    }
    default: {
      cout << "charToValue>default>Type:" << type << " Is Not Defined!!"
           << endl;
    }
  }
  return result;
}

int stack_valueToChar(char* dest, int dest_type, T value, int type) {
  switch (type) {
    case INT_: {
      int src = value.varInt;
      char out[4];
      memcpy(dest, &src, sizeof(int));
      return 4;
    }
    case Long_: {
      int64_t src = value.varLong;
      char out[8];
      memcpy(dest, &src, sizeof(int64_t));
      return 8;
    }
    case FLOAT_:
    case DOUBLE_: {
      if (dest_type == MySQL_NEWDECIMAL) {
        // cout << "### value.varDouble:" << value.varDouble <<
        // endl;//테스트출력
        bool is_negative = false;
        if (value.varDouble < 0) {
          is_negative = true;
          value.varDouble *= -1;
        }
        int64_t integer = value.varDouble / 1;
        // cout << "integer:" << integer << endl;//테스트출력
        float real_ = (value.varDouble - integer) * 100;
        int real = real_;
        // cout << "real:" << real << endl;//테스트출력
        char integer_data[8];
        char real_data[4];
        memcpy(integer_data, &integer, sizeof(int64_t));
        memcpy(real_data, &real, sizeof(int));
        for (int i = 0; i < 6; i++) {
          dest[i] = integer_data[5 - i];
        }
        // cout << "integer_data: ";//테스트출력
        // for(int k = 0; k < 8; k++){
        //     printf("%02X ",(u_char)integer_data[k]);
        // }
        // printf("real_data: %02X ",(u_char)real_data[0]);
        // cout << endl;
        dest[6] = real_data[0];
        dest[0] = 0x80;
        if (is_negative) {
          for (int i = 0; i < 7; i++) {
            dest[i] = ~dest[i];
          }
          dest[6] += 1;
        }
      } else {
        cout << "dest type: " << dest_type << endl;
      }
      return 7;
    }
    case STRING_: {
      string src = value.varString;
      int l = src.length();
      char char_array[l];
      strcpy(char_array, src.c_str());
      memcpy(dest, char_array, l);
      return l;
    }
    default: {
      cout << "valueToChar>default>Type:" << type << " Is Not Defined!!"
           << endl;
    }
  }
}

void getColOffset(const char* origin_row_data, FilterInfo filter_info,
                  int* col_offset_list) {
  int col_type, col_len, col_offset, new_col_offset = 0;
  int col_count = filter_info.table_col.size();
  int tune = 0;

  for (int i = 0; i < col_count; i++) {
    col_type = filter_info.table_datatype[i];
    col_len = filter_info.table_offlen[i];
    col_offset = filter_info.table_offset[i];

    new_col_offset = col_offset + tune;
    // cout << col_offset << "+" << tune << "=" << new_col_offset <<
    // endl;//테스트출력

    if (col_type == MySQL_VARSTRING) {
      if (col_len < 256) {  // 0~255
        char var_len[1];
        var_len[0] = origin_row_data[new_col_offset];
        uint8_t var_len_ = *((uint8_t*)var_len);
        tune += var_len_ + 1 - col_len;
      } else {  // 0~65535
        char var_len[2];
        var_len[0] = origin_row_data[new_col_offset];
        int new_col_offset_ = new_col_offset + 1;
        var_len[1] = origin_row_data[new_col_offset_];
        uint16_t var_len_ = *((uint16_t*)var_len);
        tune += var_len_ + 2 - col_len;
      }
    }

    col_offset_list[i] = new_col_offset;
  }
}
