#ifndef SERVER_INTERNAL_H
#define SERVER_INTERNAL_H

#include <stdint.h>

#include "cache/cache.h"
#include "io/connections.h"

void handle_connection_io (int epfd, Cache *cache, Conn *conn, uint64_t now_us);
void drain_zerocopy_errors(int file_des);

#endif //SERVER_INTERNAL_H
