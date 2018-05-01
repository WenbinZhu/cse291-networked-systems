#include <iostream>
#include <vector>
#include <exception>
#include "parser.h"
#include "util.h"
#include "framer.h"

void Parser::parse_request(const string &message, http_request &request) {
    try {
        // split request lines
        std::vector<string> lines;
        split(message, CRLF, lines);

        // parse initial line
        std::vector<string> initial;
        split(lines.at(0), "\\ ", initial);
        if (initial.size() != 3)
            throw std::invalid_argument("request initial line format error");
        request.method = initial[0];
        request.url = initial[1];
        request.version = initial[2];

        // redirect '/' to '/index.html'
        if (request.url == "/")
            request.url.append("index.html");

        // parse header
        for (size_t i = 1; i < lines.size(); ++i) {
            std::vector<string> header;
            split_header(lines[i], header);
            request.header.emplace(strip(header[0]), strip(header[1]));
        }

        request.valid = true;
    } catch (...) {
        request.valid = false;
    }
}

void Parser::marshal_response(const http_response &response, string &resp_msg) {
    resp_msg += response.version + " " + std::to_string(response.status.first)
                + " " + response.status.second + CRLF;

    for (auto it : response.header) {
        resp_msg += it.first + ": " + it.second + CRLF;
    }

    resp_msg += CRLF;
    resp_msg += response.body;
}