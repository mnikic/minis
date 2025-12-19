#ifndef TRANSPORT_H
#define TRANSPORT_H

#include <sys/types.h>
#include "connection.h"

typedef enum
{
  IO_OK,			// Operation completed or made progress
  IO_WAIT,			// Resource busy (EAGAIN/EWOULDBLOCK), need Epoll
  IO_EOF,			// Connection closed by peer
  IO_ERROR			// Fatal error
} IOStatus;

/*
 * Reads from the socket into the connection's Ring Buffer.
 * Handles EINTR and loop-until-EAGAIN logic.
 * * Returns: IO_OK (got data), IO_WAIT (no data yet), IO_EOF, or IO_ERROR.
 * Output: updates conn->rbuf_size directly.
 */
IOStatus transport_read_buffer (Conn * conn);

/*
 * Sends data from the specified slot to the socket.
 * Handles Vanilla Send vs ZeroCopy Send logic automatically.
 *
 * Returns: 
 * IO_OK:   Data sent (slot might be finished or partially sent).
 * IO_WAIT: Socket full, caller must listen for EPOLLOUT.
 * IO_ERROR: Fatal socket error.
 */
IOStatus transport_send_slot (Conn * conn, uint32_t slot_idx);

#endif // TRANSPORT_H
