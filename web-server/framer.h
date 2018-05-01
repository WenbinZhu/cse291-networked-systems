#ifndef FRAMER_H
#define FRAMER_H

#include <string>

using std::string;

const string CRLF = "\r\n";
const string CRLF_CRLF = CRLF + CRLF;

class Framer {
public:
    void append(string chars);

    // Does this buffer contain at least one complete message
    bool has_message() const;

    // Returns the first message
    std::string top_message() const;

    // Removes the first message
    void pop_message();

protected:
    string buffer;
};

#endif // FRAMER_H
