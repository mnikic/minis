#ifndef SERVER_LOOP_H
#define SERVER_LOOP_H

#include <stdint.h>

// bind_addr: an IPv4 address to listen on, NULL for all of them.
// db_dir: where snapshots are saved and loaded from.
int server_run (int port, const char *bind_addr, const char *db_dir);

#endif //SERVER_LOOP_H
