#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <regex>
#include <fstream>
#include <sstream>
#include <ctime>
#include <iostream>
#include <sys/stat.h>
#include "util.h"

void die_with_error(const char *err_message) {
    perror(err_message);
    exit(1);
}

void split(const string &s, const string &delim, vector<string> &tokens) {
    std::regex regex(delim);
    std::copy(std::sregex_token_iterator(s.begin(), s.end(), regex, -1),
              std::sregex_token_iterator(),
              std::back_insert_iterator<vector<string>>(tokens));
}

void split_header(const string &s, vector<string> &tokens) {
    size_t pos = s.find(':');
    if (pos == string::npos) throw;
    tokens.push_back(s.substr(0, pos));
    tokens.push_back(s.substr(pos + 1, s.size() - pos - 1));
}

string strip(const string &s) {
    if (s.empty()) throw;

    size_t i = 0, j = s.size() - 1;
    while (i <= j && isspace(s[i])) i++;
    while (i <= j && isspace(s[j])) j--;

    return s.substr(i, j - i + 1);
}

bool valid_file_path(const string &path, const string &doc_root, string &abs_path) {
    string rel_path = doc_root + "/" + path;
    char *p_root = realpath(doc_root.c_str(), nullptr);
    char *p_path = realpath(rel_path.c_str(), nullptr);

    // realpath() will return nullptr if file
    // does not exist or any other error occurs
    if (p_root == nullptr)
        die_with_error("realpath() on doc_root failed");
    if (p_path == nullptr)
        return false;

    abs_path = string(p_path);
    string abs_root = string(p_root);
    delete p_root;
    delete p_path;

    return root_not_escaped(abs_path, abs_root);
}

bool root_not_escaped(const string &path, const string &doc_root) {
    // path and doc_root should be absolute here
    return path.find(doc_root) == 0;
}

bool has_read_permission(const string &path) {
    struct stat info;
    if (stat(path.c_str(), &info) < 0)
        die_with_error("stat() failed");

    return (info.st_mode & S_IROTH) != 0;
}

string get_content_type(const string &path) {
    vector<string> tokens;
    split(path, "\\.", tokens);

    if (tokens.empty())
        return "text/plain";

    string ext = tokens[tokens.size() - 1];
    if (ext == "html" || ext == "HTML")
        return "text/html";
    if (ext == "png" || ext == "PNG")
        return "image/png";
    if (ext == "jpg" || ext == "JPG" || ext == "jpeg" || ext == "JPEG")
        return "image/jpeg";

    return "text/plain";
}

string get_last_modified(const string &path) {
    struct stat info;
    if (stat(path.c_str(), &info) < 0)
        die_with_error("stat() failed");

    char buf[256];
    memset(buf, 0, 256);
    size_t size = strftime(buf, sizeof(buf),
                  "%a, %d %b %Y %T GMT", gmtime(&info.st_mtime));

    return string(buf, size);
}

string read_file_content(const string &path) {
    std::ifstream fs(path, std::ios::binary);
    std::stringstream ss;
    ss << fs.rdbuf();
    if (fs.fail())
        die_with_error("io error when reading file");

    return ss.str();
}
