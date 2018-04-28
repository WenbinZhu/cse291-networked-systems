#include <iostream>
#include <thread>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "httpd.h"
#include "framer.h"
#include "util.h"

void start_httpd(unsigned short port, string doc_root) {
	std::cerr << "Starting server (port: " << port <<
		 ", doc_root: " << doc_root << ")" << std::endl;

    int serv_sock = setup_tcp_socket(port);

    for (;;) {
        int clnt_sock = accept_tcp_connection(serv_sock);

        struct timeval timeout;
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;

        if (setsockopt(clnt_sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &timeout, sizeof(timeout)) < 0) {
            die_with_error("setsockopt() SO_RCVTIMEO failed");
        }
        if (setsockopt(clnt_sock, SOL_SOCKET, SO_SNDTIMEO, (char *) &timeout, sizeof(timeout)) < 0) {
            die_with_error("setsockopt() SO_SNDTIMEO failed");
        }

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

void handle_http_client(int clnt_sock, const string &doc_root) {
    ssize_t recv_len;
    char buf[BUFSIZE];
    Framer framer;

    while ((recv_len = recv(clnt_sock, buf, BUFSIZE, 0)) > 0) {
        string data(buf, buf + recv_len);
        framer.append(data);
        while (framer.has_message()) {
            string msg = framer.top_message();
            framer.pop_message();
            http_request request;
            http_response response;
            string resp_msg;
            Parser::parse_request(msg, request);
            process_request(request, response);
            Parser::marshal_response(response, resp_msg);
            send_message(clnt_sock, resp_msg);
            // TODO: handle close conn
        }
    }

    if (recv_len < 0 && errno == EWOULDBLOCK) {
        std::cerr << "closing socket due to timeout" << std::endl;
    } else if (recv_len < 0) {
        close(clnt_sock);
        die_with_error("recv() failed");
    }

    close(clnt_sock);
}

void process_request(const http_request &request, http_response &response) {

}

void send_message(int clnt_sock, const string &message) {

}