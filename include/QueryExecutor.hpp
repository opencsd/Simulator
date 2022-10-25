// #include "StorageEngineInputInterface.h"
#include <stdio.h>
#include <vector>
#include <cstring>
#include <iostream>
#include <unordered_map>
#include <algorithm>

#include "TableAccess.hpp"
#include "TableManager.hpp"

using namespace std;

struct Query{
    string Left;
    int Operator;
    string Right;
    vector<string> Extra;
};

struct Aggregation{
    string Left;
    int Operator;
    string Right;
    vector<string> Extra;
};

struct Column{
    string as;
    bool CSD;
    bool Storage;
    bool Result;
};


struct SelectResult{
    string as;
    string Left;
    int Operator;
    string Right;
    vector<string> Extra;
};

struct SubQueryData{
    bool Dependency;
};

struct OrderData{
    string ColumnName;
    int OrderType; //0 = asc, 1 = desc
    int CheckNum; //0 = in table, 1 = in full query
};

struct GroupData{

};


struct TableQueryData{
    string tablename;
    string as;
    vector<TableManager::ColumnSchema> tableSchema;
    vector<Query> tableQuery;
    vector<Aggregation> tableAggregation;
    unordered_map<string,Column> tableColumn;
    vector<OrderData> orderData;
    int usedIndexNum; //-1 = not use index
    int IsOrdering; // 0 = not use index for order, 1 = use index for order
    int scantype;
};

struct SelectQueryData{
    unordered_map<string, TableQueryData> parsingdata;
    vector<SubQueryData> subQuery;
    vector<SelectResult> selectResult;
    vector<string> tablename;
    vector<OrderData> orderData;
};

struct FullQueryData{
    vector<SelectQueryData> selectquerys;
    vector<string> operators; //union 등
};


string ExecuteQuery(SelectQueryData data, TableManager tbl);

SelectQueryData ExecuteFullQuery(FullQueryData data);

int IsUsedIndex(TableQueryData data, TableManager tbl);