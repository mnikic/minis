#ifndef CONNECTION_HANDLER_H
#define  CONNECTION_HANDLER_H

#include <stdint.h>

#include "cache/cache.h"
#include "common/macros.h"
#include "io/connection.h"

HOT void
handle_connection_io (int epfd, Cache * cache, Conn * conn, uint64_t now_us,
		      uint32_t events);

#endif // CONNECTION_HANDLER_H
