#include "WalManager.hpp"
#include "keti_util.hpp"

// RapidJSON include
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h" 

// CPP REST include
#include <cpprest/http_client.h>

#include <iostream>
#include <vector>
#include <sstream>

#include <unordered_map>


using namespace web::http;
using namespace web::http::client;
using namespace rapidjson;

// std::vector<std::string> split(std::string str, char delimiter){
//     std::vector<std::string> answer;
//     std::stringstream ss(str);
//     std::string temp;
 
//     while (getline(ss, temp, delimiter)) {
//         answer.push_back(temp);
//     }
 
//     return answer;
// }

void WalManager::WalScan(string &json) {
    std::string wal_json;
    // get unflushed rows
    web::http::client::http_client client(U("http://10.0.5.121:12345/")); // wal server ip

    web::http::http_request request(web::http::methods::GET);
    request.headers().add(U("Content-Type"), U("application/json"));
    std::string req_str = "{\"tbl_name\":\"" + snippet_.table_name(0) +  "\"}";
    request.set_body(web::json::value::parse(req_str)); // tag change -> tableName

    auto requestTask = client.request(request).then([&](web::http::http_response response)
    {
		return response.extract_string();
	}).then([&](utility::string_t str)
	{
		keti_log("Wal Manager","wal rows :\n" + str);
        //std::cout << "wal rows :\n" << str << std::endl;
        wal_json = str;
		json = wal_json;
	});

	try
	{
		requestTask.wait();
	}
	catch (const std::exception &e)
	{
		printf("Error exception:%s\n", e.what());
	}

}
