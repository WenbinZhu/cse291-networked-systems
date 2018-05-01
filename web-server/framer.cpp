#include "framer.h"

void Framer::append(string chars) {
    buffer.append(chars);
}

bool Framer::has_message() const {
    return buffer.find(CRLF_CRLF) != string::npos;
}

string Framer::top_message() const {
    size_t pos = buffer.find(CRLF_CRLF);

    if (pos != string::npos) {
        return buffer.substr(0, pos);
    }

    return "";
}

void Framer::pop_message() {
    size_t pos = buffer.find(CRLF_CRLF);

    if (pos != string::npos) {
        buffer.erase(0, pos + CRLF_CRLF.size());
    }
}