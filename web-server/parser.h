#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <unordered_map>

using std::string;
using std::unordered_map;

typedef std::pair<int, string> http_status;

struct http_request {
    bool valid;
    string method;
    string url;
    string version;
    // no request body in this project
    unordered_map<string, string> header;
};

struct http_response {
    http_status status;
    string version;
    string body;
    unordered_map<string, string> header;
};

class Parser {
public:
    static void parse_request(const string &message, http_request &request);

    static void marshal_response(const http_response &response, string &resp_msg);
};

#endif // PARSER_H