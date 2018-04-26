#ifndef HTTPD_H
#define HTTPD_H

#define CRLF "\r\n"
#define BUFSIZE 1024
#define MAXPENDING 5

void start_httpd(unsigned short port, std::string doc_root);

int setup_tcp_socket(unsigned short port);

int accept_tcp_connection(int serv_sock);

void handle_http_client(int clnt_sock, const std::string &doc_root);

#endif // HTTPD_H
