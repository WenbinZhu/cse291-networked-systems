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

    DOCROOT = doc_root;
    int serv_sock = setup_tcp_socket(port);

    for (;;) {
        int clnt_sock = accept_tcp_connection(serv_sock);

        int reuse_time= 1;
        struct timeval timeout;
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;

        if (setsockopt(serv_sock, SOL_SOCKET, SO_REUSEADDR, &reuse_time, sizeof(reuse_time)) < 0) {
            die_with_error("setsockopt() SO_REUSEADDR failed");
        }
        if (setsockopt(clnt_sock, SOL_SOCKET, SO_RCVTIMEO, (char *) &timeout, sizeof(timeout)) < 0) {
            die_with_error("setsockopt() SO_RCVTIMEO failed");
        }
        if (setsockopt(clnt_sock, SOL_SOCKET, SO_SNDTIMEO, (char *) &timeout, sizeof(timeout)) < 0) {
            die_with_error("setsockopt() SO_SNDTIMEO failed");
        }

        std::thread t(handle_http_client, clnt_sock);
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

void handle_http_client(int clnt_sock) {
    ssize_t recv_len;
    char buf[BUFSIZE];
    memset(buf, 0, BUFSIZE);
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
            if (!send_message(clnt_sock, resp_msg) || client_close_conn(request)) {
                std::cerr << "client closed socket" << std::endl;
                close(clnt_sock);
                return;
            }
        }
        memset(buf, 0, BUFSIZE);
    }

    if (recv_len < 0 && errno == EWOULDBLOCK) {
        std::cerr << "closing client socket due to timeout" << std::endl;
    } else if (recv_len < 0) {
        close(clnt_sock);
        die_with_error("recv() failed");
    }

    close(clnt_sock);
}

bool client_close_conn(const http_request &request) {
    return request.header.count("Connection") && request.header.at("Connection") == "close";
}

void process_request(const http_request &request, http_response &response) {
    string abs_path;
    response.version = "HTTP/1.1";
    response.header.emplace("Server", "localhost");

    if (!request.valid || !valid_request(request)) {
        response.status = http_status(400, "Client Error");
        response.body = "<html><body><center><h1>400. Client Error</h1></center></body></html>";
        response.header.emplace("Content-Type", "text/html");
    } else if (!valid_file_path(request.url, DOCROOT, abs_path)) {
        response.status = http_status(404, "Not Found");
        response.body = "<html><body><center><h1>404. Not Found</h1></center></body></html>";
        response.header.emplace("Content-Type", "text/html");
    } else if (!has_read_permission(abs_path)) {
        response.status = http_status(403, "Forbidden");
        response.body = "<html><body><center><h1>403. Forbidden</h1></center></body></html>";
        response.header.emplace("Content-Type", "text/html");
    } else {
        response.status = http_status(200, "OK");
        response.header.emplace("Content-Type", get_content_type(abs_path));
        response.header.emplace("Last-Modified", get_last_modified(abs_path));
        response.body = read_file_content(abs_path);
    }
    response.header.emplace("Content-Length", std::to_string(response.body.size()));
    if (client_close_conn(request)) {
        response.header.emplace("Connection", "close");
    }
}

bool valid_request(const http_request &request) {
    if (request.method.empty() || request.url.empty() || request.version.empty())
        return false;

    if (request.method != "GET" || request.url[0] != '/')
        return false;

    std::vector<string> version;
    split(request.version, "/", version);
    if (version.size() != 2 || version[0] != "HTTP" || version[1] != "1.1")
        return false;

    if (!request.header.count("Host"))
        return false;

    for (auto it : request.header) {
        if (it.first.empty() || it.second.empty()) {
            return false;
        }
    }

    return true;
}

bool send_message(int clnt_sock, const string &message) {
    size_t msg_len = message.size();
    const char *buf = message.c_str();

    while (msg_len > 0) {
        ssize_t send_len = send(clnt_sock, buf, msg_len, 0);
        if (send_len > 0) {
            msg_len -= send_len;
            buf += send_len;
        }
        if (send_len < 0 && errno == EPIPE) {
            return false;
        }
    }

    return true;
}