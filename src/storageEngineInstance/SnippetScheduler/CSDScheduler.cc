#include "CSDScheduler.hpp"

vector<string> split(string str, char Delimiter);

int64_t testoffset[10] = {43673280512, 43673284512, 43673288571, 43673292610,
                          43673296610, 43673300573, 43673304610, 43673308634,
                          43673312682, 43673316768};

void Scheduler::init_scheduler(CSDManager &csdmanager) {
  //여긴 csd 기본 데이터 가져오기 ip랑 등등
  //현재 구현 모듈 없음 추후 ngd 사용 시 추가 예정

  // csdmanager_ = csdmanager;
  vector<string> tmpids = csdmanager.getCSDIDs();
  for (int i = 0; i < tmpids.size(); i++) {
    csdname_.push_back(tmpids[i]);
    CSDInfo tmpinfo = csdmanager.getCSDInfo(csdname_[i]);
    csd_.insert(make_pair(csdname_[i], tmpinfo.CSDIP));
    // vector<string> tmpvector;
    // tmpvector.push_back(csdname_[i]);
    for (int j = 0; j < tmpinfo.CSDList.size(); j++) {
      // tmpvector.push_back(tmpinfo.CSDList[j]);
      csdlist_[csdname_[i]].push_back(tmpinfo.CSDList[j]);
    }

    for (int j = 0; j < tmpinfo.SSTList.size(); j++) {
      sstcsd_.insert(make_pair(tmpinfo.SSTList[j], csdname_[i]));
    }
    csdworkblock_.insert(make_pair(csdname_[i], tmpinfo.CSDWorkingBlock));
  }

  blockcount_ = 0;
}

void Scheduler::sched(int indexdata, CSDManager &csdmanager) {
  // cout << "Find BESTCSD" << endl;
  int blockworkcount = 2;
  string bestcsd =
      BestCSD(snippetdata.sstfilelist[indexdata], blockworkcount, csdmanager);

  if (indexdata != 0 || bestcsd != "1" ||
      snippetdata.sstfilelist[indexdata].c_str() != "001533.sst") {
    std::string tmp = "";
    tmp = "Scheduling CSD List :  CSD ID : " + bestcsd + ",";

    for (int i = 0; i < csdlist_[bestcsd].size(); i++) {
      tmp += " CSD ID : ";
      tmp += csdlist_[bestcsd][i];
      tmp += ",";
    }
    // keti_log("Snippet Scheduler", "Scheduling BestCSD ...\n\t\t\t" + tmp +
    //                                   "\n\t\t\t => BestCSD : CSD" + bestcsd);
  }

  Snippet snippet(snippetdata.query_id, snippetdata.work_id,
                  snippetdata.sstfilelist[indexdata], snippetdata.table_col,
                  snippetdata.table_offset, snippetdata.table_offlen,
                  snippetdata.table_datatype, snippetdata.column_filtering,
                  snippetdata.Group_By, snippetdata.Order_By, snippetdata.Expr,
                  snippetdata.column_projection, snippetdata.returnType);
  snippet.table_filter = snippetdata.table_filter;
  // cout << "Make Snippet Done :: (" << indexdata + 1 << " / "
  //      << snippetdata.sstfilelist.size() << ")" << endl;
  StringBuffer snippetbuf;
  snippetbuf.Clear();
  Writer<StringBuffer> writer(snippetbuf);
  // cout << split(csd_[bestcsd], '+')[1] << endl;
  Serialize(writer, snippet, split(csd_[bestcsd], '+')[1], indexdata + 1,
            snippetdata.tablename, bestcsd, 0);
  // cout << "End Snippet Serialize :: (" << indexdata + 1 << " / "
  //      << snippetdata.sstfilelist.size() << ")" << endl;
  // cout << snippetbuf.GetString() << endl;
  sendsnippet(snippetbuf.GetString(), split(csd_[bestcsd], '+')[0],
              indexdata + 1);
  // cout << "Send Snippet :: (" << indexdata + 1 << " / "
  //      << snippetdata.sstfilelist.size() << ")" << endl;
  // keti_log("Snippet Scheduler",
  //          "Send Snippet to CSD Worker Module [CSD" + bestcsd + "]");
}

void Scheduler::Serialize(Writer<StringBuffer> &writer, Snippet &s,
                          string csd_ip, int s_index, string tablename,
                          string CSDName, int blockidnum) {
  writer.StartObject();
  writer.Key("queryID");
  writer.Int(s.query_id);
  writer.Key("workID");
  writer.Int(s.work_id);
  writer.Key("fileName");
  writer.String(s.sstfilename.c_str());
  writer.Key("tableName");
  writer.String(tablename.c_str());
  writer.Key("tableCol");
  // cout << "tableCol Set :: " << s_index << endl;
  writer.StartArray();
  for (int i = 0; i < s.table_col.size(); i++) {
    writer.String(s.table_col[i].c_str());
  }
  writer.EndArray();

  writer.Key("tableFilter");
  // cout << "tableFilter Set :: " << s_index << endl;
  writer.StartArray();
  if (s.table_filter.size() > 0) {
    for (int i = 0; i < s.table_filter.size(); i++) {
      writer.StartObject();
      if (s.table_filter[i].LV.type.size() > 0) {
        writer.Key("LV");
        if (s.table_filter[i].LV.type[0] == 10) {
          writer.String(s.table_filter[i].LV.value[0].c_str());
        }
      }
      writer.Key("OPERATOR");
      writer.Int(s.table_filter[i].filteroper);

      if (s.table_filter[i].filteroper == 8 ||
          s.table_filter[i].filteroper == 9) {
        writer.Key("EXTRA");
        writer.StartArray();

        for (int j = 0; j < s.table_filter[i].RV.type.size(); j++) {
          if (s.table_filter[i].RV.type[j] != 10) {
            if (s.table_filter[i].RV.type[j] == 7 ||
                s.table_filter[i].RV.type[j] == 3) {
              string tmpstr = s.table_filter[i].RV.value[j];
              int tmpint = atoi(tmpstr.c_str());
              writer.Int(tmpint);
            } else if (s.table_filter[i].RV.type[j] == 9) {
              string tmpstr = s.table_filter[i].RV.value[j];
              tmpstr = "+" + tmpstr;
              writer.String(tmpstr.c_str());
            } else if (s.table_filter[i].RV.type[j] == 4 ||
                       s.table_filter[i].RV.type[j] == 5) {
              string tmpstr = s.table_filter[i].RV.value[j];
              double tmpfloat = stod(tmpstr);
              writer.Double(tmpfloat);
            } else {
              string tmpstr = s.table_filter[i].RV.value[j];
              tmpstr = "+" + tmpstr;
              writer.String(tmpstr.c_str());
            }
          } else {
            writer.String(s.table_filter[i].RV.value[j].c_str());
          }
        }
        writer.EndArray();
      } else if (s.table_filter[i].RV.type.size() > 0) {
        writer.Key("RV");

        if (s.table_filter[i].RV.type[0] != 10) {
          if (s.table_filter[i].RV.type[0] == 7 ||
              s.table_filter[i].RV.type[0] == 3) {
            string tmpstr = s.table_filter[i].RV.value[0];
            int tmpint = atoi(tmpstr.c_str());
            writer.Int(tmpint);
          } else if (s.table_filter[i].RV.type[0] == 9) {
            string tmpstr = s.table_filter[i].RV.value[0];
            tmpstr = "+" + tmpstr;
            writer.String(tmpstr.c_str());
          } else if (s.table_filter[i].RV.type[0] == 4 ||
                     s.table_filter[i].RV.type[0] == 5) {
            string tmpstr = s.table_filter[i].RV.value[0];
            double tmpfloat = stod(tmpstr);
            writer.Double(tmpfloat);
          } else {
            string tmpstr = s.table_filter[i].RV.value[0];
            tmpstr = "+" + tmpstr;
            writer.String(tmpstr.c_str());
          }
        } else {
          writer.String(s.table_filter[i].RV.value[0].c_str());
        }
      }

      writer.EndObject();
    }
  }

  writer.EndArray();

  writer.Key("columnFiltering");
  // cout << "columnFiltering Set :: " << s_index << endl;
  writer.StartArray();
  for (int i = 0; i < s.column_filtering.size(); i++) {
    writer.String(s.column_filtering[i].c_str());
  }
  writer.EndArray();

  writer.Key("groupBy");
  // cout << "groupBy Set :: " << s_index << endl;
  writer.StartArray();

  for (int i = 0; i < s.Group_By.size(); i++) {
    writer.String(s.Group_By[i].c_str());
  }
  writer.EndArray();

  writer.Key("Order_By");
  // cout << "Order_By Set :: " << s_index << endl;
  writer.StartArray();
  for (int i = 0; i < s.Order_By.size(); i++) {
    writer.String(s.Order_By[i].c_str());
  }
  writer.EndArray();
  writer.Key("columnProjection");
  // cout << "columnProjection Set :: " << s_index << endl;
  writer.StartArray();
  for (int i = 0; i < s.column_projection.size(); i++) {
    writer.StartObject();
    for (int j = 0; j < s.column_projection[i].size(); j++) {
      if (j == 0) {
        writer.Key("selectType");
        writer.Int(atoi(s.column_projection[i][j].value.c_str()));
      } else {
        if (j == 1) {
          writer.Key("value");
          writer.StartArray();
          writer.String(s.column_projection[i][j].value.c_str());
        } else {
          writer.String(s.column_projection[i][j].value.c_str());
        }
      }
    }
    if (s.column_projection[i].size() == 1) {
      writer.Key("value");
      writer.StartArray();
    }
    writer.EndArray();
    for (int j = 1; j < s.column_projection[i].size(); j++) {
      if (j == 1) {
        writer.Key("valueType");
        writer.StartArray();
        writer.Int(s.column_projection[i][j].type);
      } else {
        writer.Int(s.column_projection[i][j].type);
      }
    }
    if (s.column_projection[i].size() == 1) {
      writer.Key("valueType");
      writer.StartArray();
    }
    writer.EndArray();
    writer.EndObject();
  }
  writer.EndArray();
  writer.Key("tableOffset");
  // cout << "tableOffset Set :: " << s_index << endl;
  writer.StartArray();
  for (int i = 0; i < s.table_offset.size(); i++) {
    writer.Int(s.table_offset[i]);
  }
  writer.EndArray();

  writer.Key("tableOfflen");
  // cout << "tableOfflen Set :: " << s_index << endl;
  writer.StartArray();

  for (int i = 0; i < s.table_offlen.size(); i++) {
    writer.Int(s.table_offlen[i]);
  }
  writer.EndArray();

  writer.Key("tableDatatype");
  // cout << "tableDatatype Set :: " << s_index << endl;
  writer.StartArray();
  for (int i = 0; i < s.table_datatype.size(); i++) {
    writer.Int(s.table_datatype[i]);
  }
  writer.EndArray();

  writer.Key("projectionDatatype");
  // cout << "projectionDatatype Set :: " << s_index << endl;
  writer.StartArray();
  for (int i = 0; i < s.returnType.size(); i++) {
    writer.Int(s.returnType[i]);
  }
  writer.EndArray();

  writer.Key("blockList");
  writer.StartArray();

  writer.EndArray();

  writer.Key("primaryKey");
  writer.Int(0);

  writer.Key("csdName");
  writer.String(CSDName.c_str());

  writer.Key("CSD IP");
  writer.String(csd_ip.c_str());
  writer.EndObject();
}

void Scheduler::sendsnippet(string snippet, string ipaddr, int s_index) {
  Document document;

  document.Parse(snippet.c_str());

  ipaddr = document["CSD IP"].GetString();
  ipaddr = split(ipaddr, ':')[0];
  string port_str = split(document["CSD IP"].GetString(), ':')[1];
  int port = stoi(port_str);
  cout << "Send Snippet to " << document["csdName"].GetString()
       << " -> (CSD IP :: " << ipaddr << ", Port ::" << port
       << ", FileName :: " << document["fileName"].GetString() << endl;

  int sock;
  struct sockaddr_in serv_addr;
  sock = socket(PF_INET, SOCK_STREAM, 0);
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(ipaddr.c_str());

  serv_addr.sin_port = htons(port);

  connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

  size_t len = strlen(snippet.c_str());
  send(sock, &len, sizeof(len), 0);
  send(sock, (char *)snippet.c_str(), strlen(snippet.c_str()), 0);
  close(sock);
}

string Scheduler::BestCSD(string sstname, int blockworkcount,
                          CSDManager &csdmanager) {
  string sstincsd = sstcsd_[sstname];
  string csdreplica = csdreaplicamap_[sstincsd];
  return sstincsd;

  if (csdworkblock_[csdreplica] > csdworkblock_[sstincsd]) {
    csdworkblock_[sstincsd] = csdworkblock_[sstincsd] + blockworkcount;
    return sstincsd;
  }
  csdworkblock_[csdreplica] = csdworkblock_[csdreplica] + blockworkcount;
  return csdreplica;
}

void Scheduler::csdworkdec(string csdname, int num) {
  csdworkblock_[csdname] = csdworkblock_[csdname] - num;
  // CSDManagerDec(csdname,num);
}

vector<string> split(string str, char Delimiter) {
  istringstream iss(str);  // istringstream에 str을 담는다.
  string buffer;  // 구분자를 기준으로 절삭된 문자열이 담겨지는 버퍼

  vector<string> result;

  // istringstream은 istream을 상속받으므로 getline을 사용할 수 있다.
  while (getline(iss, buffer, Delimiter)) {
    result.push_back(buffer);  // 절삭된 문자열을 vector에 저장
  }

  return result;
}

// void Scheduler::CSDManagerDec(string csdname, int num){
//     csdmanager_.CSDBlockDesc(csdname, num);
// }