#ifndef TRANSPORT_H
#define TRANSPORT_H

#include <sys/types.h>

#include "io/connection.h"
#include "common/macros.h"

typedef enum
{
  IO_OK,			// Operation completed or made progress
  IO_WAIT,			// Resource busy (EAGAIN/EWOULDBLOCK), need Epoll
  IO_EOF,			// Connection closed by peer
  IO_ERROR,			// Fatal error
  IO_BUF_FULL			// Cannot read even after compacting the buffer!
} IOStatus;

/*
 * Reads from the socket into the connection's Ring Buffer.
 * Handles EINTR and loop-until-EAGAIN logic.
 * * Returns: IO_OK (got data), IO_WAIT (no data yet), IO_EOF, or IO_ERROR.
 * Output: updates conn->rbuf_size directly.
 */
HOT IOStatus transport_read_buffer (Conn * conn);

HOT IOStatus transport_write_batch (Conn * conn);

#endif // TRANSPORT_H
