#ifndef SERVER_INTERNAL_H
#define SERVER_INTERNAL_H

#include <stdint.h>

#include "cache/cache.h"
#include "io/connections.h"

enum
{
  STATE_REQ = 0,
  STATE_RES = 1,
  STATE_RES_CLOSE = 2,		// Send response then close connection
  STATE_END = 3,		// Mark the connection for deletion
};

void handle_connection_io (int epfd, Cache *cache, Conn *conn, uint64_t now_us);

#endif //SERVER_INTERNAL_H
