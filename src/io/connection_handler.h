#ifndef CONNECTION_HANDLER_H
#define  CONNECTION_HANDLER_H

#include <stdint.h>

#include "cache/cache.h"
#include "common/macros.h"
#include "io/connection.h"

HOT void
handle_connection_io (Cache * cache, Conn * conn, uint64_t now_us,
		      IOEvent events);

// Connections draining after an error (STATE_DRAIN): the earliest
// deadline, (uint64_t) -1 if none, and the next one past its deadline,
// taken off the list, to close.
uint64_t conn_drain_next_deadline (void);
Conn *conn_drain_pop_expired (uint64_t now_us);
// Takes a closing connection off the list
void conn_drain_forget (Conn * conn);

#endif // CONNECTION_HANDLER_H
