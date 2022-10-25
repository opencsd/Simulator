#pragma once

#include "grpc/snippet_sample.grpc.pb.h"

using namespace std;
using snippetsample::Snippet;
using snippetsample::SnippetRequest;

class WalManager {
public:
	WalManager(const Snippet snippet) : snippet_(snippet){}
    void run(string& json){        
        WalScan(json);
    }
private:
    void WalScan(string& json); // get dirty rows
    Snippet snippet_;
};