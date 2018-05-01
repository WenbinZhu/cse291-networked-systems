#ifndef UTIL_H
#define UTIL_H

#include <string>
#include <cstring>
#include <vector>

using std::string;
using std::vector;

void die_with_error(const char *err_message);

void split(const string &s, const string &delim, vector<string> &tokens);

void split_header(const string &s, vector<string> &tokens);

string strip(const string &s);

bool valid_file_path(const string &path, const string &doc_root, string &abs_path);

bool root_not_escaped(const string &path, const string &doc_root);

bool has_read_permission(const string &path);

string get_content_type(const string &path);

string get_last_modified(const string &path);

string read_file_content(const string &path);

#endif // UTIL_H
