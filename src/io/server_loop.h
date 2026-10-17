#ifndef SERVER_LOOP_H
#define SERVER_LOOP_H

#include <stdint.h>

// bind_addr: an IPv4 address to listen on, NULL for all of them
int server_run (int port, const char *bind_addr);

#endif //SERVER_LOOP_H
