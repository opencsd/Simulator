#include "TableAccess.hpp"

void TableAccess::AccessTable(){
    printf("Call Table Access::Access Table method...");
}

void SEIndexScan(){
    cout << "Index Scan at Storage Engine" << endl;
    //스캔 후 컴플렉스 스캔까지 동작해야함
}

void SEFullScan(){
    cout << "Full Scan at Storage Engine" << endl;
}

void SEIndexSeek(){
    cout << "Index Seek at Storage Engine" << endl;
}

void SECoveringIndex(){
    cout << "Covering Index Used" << endl;
    //인덱스 스캔과 같이 벨류도 스캔해야함
}

void SEIndexPredicate(){

}

void IsPushdownPredicate(){

}