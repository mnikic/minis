#ifndef SERVER_LOOP_H
#define SERVER_LOOP_H

#include <stdint.h>

// bind_addr: an IPv4 address to listen on, NULL for all of them.
// idle_timeout_sec: close a client that does nothing for that long, 0 never.
// db_dir: where snapshots are saved and loaded from.
// ready_fd: written to and closed once serving, -1 for none.
int server_run (int port, const char *bind_addr, uint64_t idle_timeout_sec,
		const char *db_dir, int ready_fd);

#endif //SERVER_LOOP_H
