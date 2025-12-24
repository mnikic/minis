#include <strings.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <stddef.h>

#include "common/common.h"
#include "connection.h"
#include "transport.h"

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

// --------------------------------------------------------------------------
// V3 UPDATE: Send using IOVEC (Scatter/Gather)
// --------------------------------------------------------------------------
IOStatus
transport_send_head_slot (Conn *conn)
{
  ResponseSlot *slot = conn_get_head_slot (conn);

  // Safety: If slot is empty or fully sent, nothing to do.
  if (slot->iov_cnt == 0 || slot->total_len == 0
      || slot->sent >= slot->total_len)
    {
      return IO_OK;
    }

  // 1. Prepare msghdr
  // We use the slot's internal IOV array directly.
  struct msghdr msg = {
    .msg_name = NULL,
    .msg_namelen = 0,
    .msg_iov = slot->iov,
    .msg_iovlen = (size_t) slot->iov_cnt,
    .msg_control = NULL,
    .msg_controllen = 0,
    .msg_flags = 0
  };

  // 2. Set Flags
  int flags = MSG_DONTWAIT | MSG_NOSIGNAL;
  if (slot->is_zero_copy)
    {
      flags |= MSG_ZEROCOPY;
    }

  // 3. Syscall
  ssize_t sent;
  do
    {
      sent = sendmsg (conn->fd, &msg, flags);
    }
  while (sent < 0 && errno == EINTR);

  // 4. Handle Errors
  if (sent < 0)
    {
      if (errno == EAGAIN)
	{
	  return IO_WAIT;
	}
      msgf ("transport: sendmsg error FD %d: %s", conn->fd, strerror (errno));
      return IO_ERROR;
    }

  // 5. Update State
  slot->sent += (uint32_t) sent;

  if (slot->is_zero_copy)
    {
      slot->pending_ops++;
    }

  // 6. Handle Partial Writes (Crucial for IOVEC!)
  // If we didn't send everything, we must update the iovec pointers
  // so the NEXT call to sendmsg() starts where we left off.
  if (slot->sent < slot->total_len)
    {
      size_t bytes_to_advance = (size_t) sent;

      for (int i = 0; i < slot->iov_cnt; i++)
	{
	  struct iovec *iov = &slot->iov[i];

	  if (iov->iov_len == 0)
	    continue;		// Already exhausted this chunk

	  if (bytes_to_advance >= iov->iov_len)
	    {
	      // We fully consumed this chunk
	      bytes_to_advance -= iov->iov_len;
	      iov->iov_len = 0;
	      // We don't null base, just set len to 0. sendmsg skips 0-len iovs.
	    }
	  else
	    {
	      // We partially consumed this chunk
	      iov->iov_base = (uint8_t *) iov->iov_base + bytes_to_advance;
	      iov->iov_len -= bytes_to_advance;
	      break;		// Done advancing
	    }
	}
    }

  return IO_OK;
}
