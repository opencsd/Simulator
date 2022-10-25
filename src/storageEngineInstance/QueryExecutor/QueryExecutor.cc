#include "QueryExecutor.hpp"

string ExecuteQuery(SelectQueryData data, TableManager tbl){
    //이전 테이블의 영향을 받는것을 알수있는 방법을 찾아야함
    //여기서도 버퍼매니저와의 통신을 통해 이전 테이블의 스캔 여부 판단해야함
    //버퍼매니저 인잇 해줘야함(select id, table count + merging table query)
    if(data.tablename.size() == 1){
        //단일 테이블 일때
        return data.tablename[0];
    }
    vector<string> ret;
    ret = tbl.get_ordered_table_by_size(data.tablename);
    return ret[0];
}

SelectQueryData ExecuteFullQuery(FullQueryData data){
    //유니온 체크와 유니온 별 실행 순서 출력
    //버퍼매니저에 인잇 해줘야함(query id, select count)
    if(data.selectquerys.size() == 1){
        //단일 쿼리일때
        return data.selectquerys[0];
    }
}

int IsUsedIndex(TableQueryData data, TableManager tbl){
    vector<vector<string>> indexlist;
    vector<int> countpoint;
    //인덱스 테이블이 없다
    if(tbl.get_IndexList(data.tablename,indexlist) == -1){
        data.usedIndexNum = -1;
        return -1;
    }

    //테이블 쿼리에 다른 컬럼이 있다 = 커버링이 아니다(조인 제외)
    for(int i = 0; i < indexlist.size(); i++){
        countpoint.push_back(0);
        bool flag = true; //커버링 확인용
        //컬럼에 포함된 정보 확인
        for(auto j = data.tableColumn.begin(); j != data.tableColumn.end(); j++){
            //만약 인덱스에 포함된 정보가 아닌데 1이 들어있을 경우 커버링이 아님
            pair<string,Column> a = *j;
            auto it = find(indexlist[i].begin(),indexlist[i].end(),a.first);
            if(it != indexlist[i].end()){
                continue;
            }
            if(a.second.CSD || a.second.Result || a.second.Storage){
                flag = false;
                break;
            }
        }
        if(flag){
            //커버링일경우 그 인덱스 사용
            data.usedIndexNum = i;
            data.scantype = 15;
            return 0;
        }
        for(auto j = data.tableColumn.begin(); j != data.tableColumn.end(); j++){
            //인덱스 포함 컬럼의 점수 계산
            pair<string,Column> a = *j;
            auto it = find(indexlist[i].begin(),indexlist[i].end(),a.first);
            if(it != indexlist[i].end()){
                // continue;
                if(a.second.CSD || a.second.Result || a.second.Storage){
                    countpoint[i]++;
                    continue;
            }
            }
            // if(a.second.CSD || a.second.Result || a.second.Storage){
            //     flag = false;
            //     break;
            // }
        }

        //order 확인 첫번째와 첫번째만 비교 이유는 주석씨가 정리했음
        // for(int j = 0; j < data.orderData.size(); j++){
        //     if(j == 1){
        //         break;
        //     }
        //     if(data.orderData[j].ColumnName == indexlist[i][0]){
        //         countpoint[i]++;
        //     }else{
        //         //order에 없는 경우
        //     }
        // }

        // for(int j = 0; j < data.tableQuery)
        
    }
    int max = *max_element(countpoint.begin(), countpoint.end());
    if(max == 0){
        data.usedIndexNum = -1;
        return -1;
    }
    int max_index = max_element(countpoint.begin(), countpoint.end()) - countpoint.begin();
    data.usedIndexNum = max_index;
    // order 확인 첫번째와 첫번째만 비교 이유는 주석씨가 정리했음
    for(int j = 0; j < data.orderData.size(); j++){
        if(j == 1){
            break;
        }
        if(data.orderData[j].ColumnName == indexlist[max_index][0]){
            data.scantype = 13;
            data.IsOrdering = 1;
        }else{
            //order에 없는 경우
            data.scantype = 12;
            data.IsOrdering = 0;
            return 1;
        }
    }

    // for(int i = 0; i < indexlist.size(); i++){
    //     for(int j = 0; j < data.orderData.size(); j++){
    //         auto it = find(indexlist[i].begin(),indexlist[i].end(),data.orderData[j].ColumnName);
    //         if(it != indexlist[i].end()){
    //             return 1;
    //         }
    //     }
    // }


    // for(int i = 0; i < indexlist.size(); i++){
    //     for(int j = 0; j < data.tableQuery.size(); j++){
    //         auto it = find(indexlist[i].begin(),indexlist[i].end(),data.tableQuery[j].Left);
    //         if(it != indexlist[i].end()){
    //             return 1;
    //         }
    //         auto it = find(indexlist[i].begin(),indexlist[i].end(),data.tableQuery[j].Right);
    //         if(it != indexlist[i].end()){
    //             return 1;
    //         }
    //     }
    // }
    return 0;
}