#pragma once
enum class KETI_WORK_TYPE{
	SCAN = 4,
	SCAN_N_FILTER = 5,
	REQ_SCANED_BLOCK = 6,
	WORK_END = 9,
    SE_FULL_SCAN = 11,
    SE_INDEX_PREDICATE = 12,
    SE_INDEX_SCAN = 13,
    SE_INDEX_SEEK = 14,
    SE_COVERING_INDEX = 15,
    CSD_FULL_SCAN = 16,
    CSD_INDEX_SEEK = 17,
    SE_MERGE_BUFFER_MANAGER = 18
};

enum class KETI_OPER_TYPE
{
    KETI_DEFAULT = 0,
    KETI_GE,      // >=
    KETI_LE,      // <=
    KETI_GT,      // >
    KETI_LT,      // <
    KETI_ET,      // ==
    KETI_NE,      // !=, <>
    KETI_LIKE,    // RV로 스트링
    KETI_BETWEEN, // RV로 배열형식 [10,20] OR [COL1,20] --> extra
    KETI_IN,      // RV로 배열형식 [10,20,30,40] + 크기 --> extra
    KETI_IS,      // IS 와 IS NOT을 구분 RV는 무조건 NULL
    KETI_ISNOT,   // IS와 구분 필요 RV는 무조건 NULL
    KETI_NOT,     // ISNOT과 관련 없음 OPERATOR 앞에 붙는 형식 --> 혼자 들어오는 oper
    KETI_AND,     // AND --> 혼자 들어오는 oper
    KETI_OR,      // OR --> 혼자 들어오는 oper
    KETI_JOIN,    // 타입 나눠야함 left, right, inner, outer
    KETI_SUBSTRING,
    KETI_SET_UNION = 100,
    KETI_SET_UNIONALL = 101,
    KETI_SET_INTERSECT = 102, //MySQL 미지원
    KETI_SET_MINUS = 103, //MySQL 미지원
    KETI_SET_PLUS = 104  //MySQL 미지원
};

enum class KETI_PARSER_TYPE{
    StrVal,
    IntVal,
    FloatVal,
    HexNum,
    HexVal,
    ValArg,
};

enum class KETI_QueryEngine_Type{
    MySQL = 1,
    Oracle = 2
};

enum class Oracle_DataType{
    Oracle_NUMBER = 1,
    Oracle_DATE,
    Oracle_TIMESTAMP,
    Oracle_CHAR,
    Oracle_FLOAT,
};

enum class MySQL_DataType{
    MySQL_BYTE = 1,
    MySQL_INT16 = 2,
    MySQL_INT32 = 3,
    MySQL_INT64 = 8,
    MySQL_FLOAT32 = 4,
    MySQL_DOUBLE = 5,
    MySQL_NEWDECIMAL = 246,
    MySQL_DATE = 14,
    MySQL_TIMESTAMP = 7,
    MySQL_STRING = 254,
    MySQL_VARSTRING = 15,
};

enum class KETI_Type{
    KETI_INT8 = 0,
    KETI_INT16,
    KETI_INT32,
    KETI_INT64,
    KETI_FLOAT32,
    KETI_FLOAT64,
    KETI_NUMERIC,
    KETI_DATE,
    KETI_TIMESTAMP,
    KETI_STRING,
    KETI_COLUMN,
    KETI_PLUS = 100,
    KETI_MINUS,
    KETI_MULTIPLE,
    KETI_DIVIDE,
};

enum class Work_Status_Type{
    QueryIDError,
    NonInitTable,
    NotFinished,
    WorkDone,
};

enum class Merge_Work_Type{
    
};


enum class KETI_Aggregation_Type{
    KETI_Column_name = 0,
    KETI_SUM,
    KETI_AVG,
    KETI_COUNT,
    KETI_COUNTALL,
    KETI_MIN,
    KETI_MAX,
    KETI_CASE,
    KETI_WHEN,
    KETI_THEN,
    KETI_ELSE,
    KETI_LIKE_SELECT,
};

enum class KETI_Projection_Type{
    PROJECTION_STRING,
    PROJECTION_INT,
    PROJECTION_FLOAT,
    PROJECTION_COL,
    PROJECTION_OPER,
};


enum class KETI_Snippet_Type{
    BASIC_SNIPPET = 0,
    AGGREGATION_SNIPPET,
    JOIN_SNIPPET,
    SUBQUERY_SNIPPET,
    DEPENDENCY_EXIST_SNIPPET,
    DEPENDENCY_NOT_EXIST_SNIPPET,
    DEPENDENCY_OPER_SNIPPET,
    DEPENDENCY_IN_SNIPPET,
    HAVING_SNIPPET,
    LEFT_OUT_JOIN_SNIPPET,
};