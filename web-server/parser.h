#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <unordered_map>

using std::string;
using std::unordered_map;

struct http_request {
    bool valid;
    string method;
    string url;
    string version;
    // no http body
    unordered_map<string, string> header;
};

struct http_response {
    int status;
    string version;
    string description;
    unordered_map<string, string> header;
};

class Parser {
public:
    static bool parse_request(const string &message, http_request &request);

    static void build_response_message(const http_response &response, string &resp_msg);
};

#endif // PARSER_H