#include "transport.h"
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include "common/common.h"	// For msgf, DBG_LOGF

IOStatus
transport_read_buffer (Conn *conn)
{
  size_t total_read = 0;
  while (true)
    {
      size_t cap = K_RBUF_SIZE - conn->rbuf_size;
      if (cap == 0)
	return IO_OK;		// Buffer full

      ssize_t num;
      do
	{
	  num = read (conn->fd, &conn->rbuf[conn->rbuf_size], cap);
	}
      while (num < 0 && errno == EINTR);

      if (num < 0)
	{
	  if (errno == EAGAIN)
	    {
	      // If we read *something* before blocking, it's OK.
	      return total_read > 0 ? IO_OK : IO_WAIT;
	    }
	  msgf ("transport: read error FD %d: %s", conn->fd,
		strerror (errno));
	  return IO_ERROR;
	}

      if (num == 0)
	return IO_EOF;

      conn->rbuf_size += (uint32_t) num;
      total_read += (size_t) num;
    }
}

IOStatus
transport_send_slot (Conn *conn, uint32_t slot_idx)
{
  ResponseSlot *slot = &conn->res_slots[slot_idx];

  // Logic extracted from send_current_slot...
  uint8_t *data_ptr = get_slot_data_ptr (conn, slot_idx);
  size_t remain = slot->actual_length - slot->sent;
  ssize_t sent;

  if (slot->is_zerocopy)
    {
      struct iovec iov = {.iov_base = data_ptr + slot->sent,.iov_len =
	  remain };
      struct msghdr msg = {.msg_iov = &iov,.msg_iovlen = 1 };
      sent =
	sendmsg (conn->fd, &msg, MSG_DONTWAIT | MSG_ZEROCOPY | MSG_NOSIGNAL);
    }
  else
    {
      sent =
	send (conn->fd, data_ptr + slot->sent, remain,
	      MSG_NOSIGNAL | MSG_DONTWAIT);
    }

  if (sent < 0)
    {
      if (errno == EAGAIN)
	return IO_WAIT;
      msgf ("transport: send error FD %d: %s", conn->fd, strerror (errno));
      return IO_ERROR;
    }

  slot->sent += (uint32_t) sent;

  // ZeroCopy accounting
  if (slot->is_zerocopy)
    {
      slot->pending_ops++;
    }

  return IO_OK;
}
