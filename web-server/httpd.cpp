#include <iostream>
#include <thread>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "httpd.h"
#include "util.h"

void start_httpd(unsigned short port, std::string doc_root) {
	std::cerr << "Starting server (port: " << port <<
		 ", doc_root: " << doc_root << ")" << std::endl;

    int serv_sock = setup_tcp_socket(port);
    if (serv_sock < 0) {
        die_with_error("setup_tcp_socket() failed");
    }

    for (;;) {
        int clnt_sock = accept_tcp_connection(serv_sock);
        std::thread t(handle_http_client, clnt_sock, doc_root);
        t.detach();
    }
}

int setup_tcp_socket(unsigned short port) {
    int serv_sock;
    struct sockaddr_in serv_addr;

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if ((serv_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        die_with_error("socket() failed");
    }

    if (bind(serv_sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr))) {
        die_with_error("bind() failed");
    }

    if (listen(serv_sock, MAXPENDING) < 0) {
        die_with_error("bind() failed");
    }

    return serv_sock;
}

int accept_tcp_connection(int serv_sock) {
    int clnt_sock;
    struct sockaddr_in clnt_addr;
    socklen_t clnt_len = sizeof(clnt_addr);

    if ((clnt_sock = accept(serv_sock, (struct sockaddr *) &clnt_addr, &clnt_len)) < 0) {
        die_with_error("accept() failed");
    }

    printf("Handling client %s\n", inet_ntoa(clnt_addr.sin_addr));

    return clnt_sock;
}

void handle_http_client(int clnt_sock, const std::string &doc_root) {

}
