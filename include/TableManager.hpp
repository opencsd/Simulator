
#pragma once
#include <vector>
#include <unordered_map>
// #include "testmodule.h"

using namespace std;

class TableManager{	
	// TableManager()
public:
	struct ColumnSchema {
		std::string column_name;
		int type;
		int length;
		int offset;
	};
	
	struct DataBlockHandle {
		off64_t Offset;
		off64_t Length;
	};
	
	struct SSTFile{
		std::string filename;
		vector<struct DataBlockHandle> BlockList;
	};
	
	struct Table {
		std::string tablename;
		int tablesize;
		vector<struct ColumnSchema> Schema;
		vector<struct SSTFile> SSTList;
		vector<vector<string>> IndexList;
		vector<string> PK;
	};

	int init_TableManager();
	int init_TableManager(string filePath);
	void print_TableManager();
	int generate_req_json(std::string tablename,char *dst);
	int generate_req_json(std::string tablename,std::string &dst);
	int get_table_schema(std::string tablename,vector<struct ColumnSchema> &dst);
	vector<string> get_ordered_table_by_size(vector<string> tablenames);
	int get_IndexList(string tablename, vector<vector<string>> &dst);
	vector<string> get_sstlist(string tablename);
	
	
private:
	unordered_map<std::string,struct Table> m_TableManager;
};