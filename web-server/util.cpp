#include <cstdio>
#include <cstdlib>
#include "util.h"

void die_with_error(const char * err_message) {
    perror(err_message);
    exit(1);
}

