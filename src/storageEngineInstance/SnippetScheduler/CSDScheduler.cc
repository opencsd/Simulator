#include "CSDScheduler.hpp"

void Scheduler::init_scheduler(CSDManager &csdmanager) {
  for (int i = 0; i < 8; i++) {
    CSDInfo tmpinfo = csdmanager.getCSDInfo(csdname_[i]);
    csd_.insert(make_pair(csdname_[i], tmpinfo.CSDIP));

    for (int j = 0; j < tmpinfo.SSTList.size(); j++) {
      sstcsd_.insert(make_pair(tmpinfo.SSTList[j], csdname_[i]));
    }
  }
}

void Scheduler::sched(int indexdata, CSDManager &csdmanager) {
  string currentCSD = snippetdata.sstfilelist[indexdata];
  CSDInfo csdInfo = csdmanager.getCSDInfo(currentCSD);
  string csdIP = csdInfo.CSDIP;
  Snippet snippet(snippetdata.query_id, snippetdata.work_id,
                  snippetdata.sstfilelist[indexdata], snippetdata.table_col,
                  snippetdata.table_offset, snippetdata.table_offlen,
                  snippetdata.table_datatype, snippetdata.column_filtering,
                  snippetdata.Group_By, snippetdata.Order_By, snippetdata.Expr,
                  snippetdata.column_projection, snippetdata.returnType);
  snippet.table_filter = snippetdata.table_filter;

  StringBuffer snippetbuf;
  snippetbuf.Clear();
  PrettyWriter<StringBuffer> writer(snippetbuf);

  Serialize(writer, snippet, csdIP, snippetdata.tablename, currentCSD);

  sendsnippet(snippetbuf.GetString());
}

void Scheduler::Serialize(PrettyWriter<StringBuffer> &writer, Snippet &s,
                          string csd_ip, string tablename, string CSDName) {
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
  writer.StartArray();
  for (int i = 0; i < s.column_filtering.size(); i++) {
    writer.String(s.column_filtering[i].c_str());
  }
  writer.EndArray();

  writer.Key("groupBy");
  writer.StartArray();

  for (int i = 0; i < s.Group_By.size(); i++) {
    writer.String(s.Group_By[i].c_str());
  }
  writer.EndArray();

  writer.Key("Order_By");
  writer.StartArray();
  for (int i = 0; i < s.Order_By.size(); i++) {
    writer.String(s.Order_By[i].c_str());
  }
  writer.EndArray();
  writer.Key("columnProjection");
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
  writer.StartArray();
  for (int i = 0; i < s.table_offset.size(); i++) {
    writer.Int(s.table_offset[i]);
  }
  writer.EndArray();

  writer.Key("tableOfflen");
  writer.StartArray();

  for (int i = 0; i < s.table_offlen.size(); i++) {
    writer.Int(s.table_offlen[i]);
  }
  writer.EndArray();

  writer.Key("tableDatatype");
  writer.StartArray();
  for (int i = 0; i < s.table_datatype.size(); i++) {
    writer.Int(s.table_datatype[i]);
  }
  writer.EndArray();

  writer.Key("projectionDatatype");
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

void Scheduler::sendsnippet(string snippet) {
  Document document;

  document.Parse(snippet.c_str());

  string socketPath = document["CSD IP"].GetString();

  int sock;
  struct sockaddr_un serv_addr;
  sock = socket(AF_UNIX, SOCK_STREAM, 0);

  serv_addr.sun_family = AF_UNIX;
  strcpy(serv_addr.sun_path, socketPath.c_str());
  size_t sockLen = strlen(serv_addr.sun_path) + sizeof(serv_addr.sun_family);
  if (connect(sock, (struct sockaddr *)&serv_addr, sockLen) == -1) {
    logger.fatal("Client: Error on connect call \n");
    exit(EXIT_FAILURE);
  }
  logger.info("Snippet Scheduler : Connected");
  size_t len = strlen(snippet.c_str());
  send(sock, &len, sizeof(len), 0);
  send(sock, (char *)snippet.c_str(), strlen(snippet.c_str()), 0);
  if (send(sock, (char *)snippet.c_str(), strlen(snippet.c_str()), 0) == -1) {
    logger.fatal("Client: Error on send() call \n");
    exit(EXIT_FAILURE);
  }
  close(sock);
}