
CC=g++
CFLAGS=-ggdb -std=c++11 -Wall -Wextra -pedantic
DEPS = httpd.h util.h framer.h parser.h
SRCS = httpd.cpp util.cpp framer.cpp parser.cpp
MAIN_SRCS = main.c $(SRCS)
MAIN_OBJS = $(MAIN_SRCS:.c=.o)

default: httpd

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

httpd:    $(MAIN_OBJS)
	$(CC) $(CFLAGS) -o httpd $(MAIN_OBJS) -lpthread

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf httpd *.o *.dSYM
