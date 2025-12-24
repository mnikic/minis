#include <strings.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <stddef.h>

#include "common/common.h"
#include "io/connection.h"
#include "io/transport.h"

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

#include <sys/uio.h>		// for iovec, writev
#include <limits.h>		// for IOV_MAX

// Linux usually defines IOV_MAX as 1024. 
// We use a safe local limit to keep the stack frame reasonable.
#define BATCH_IOV_LIMIT 256

HOT IOStatus
transport_write_batch (Conn *conn)
{
  struct iovec batch[BATCH_IOV_LIMIT];
  int iov_count = 0;

  // 1. GATHER STEP
  // Look ahead in the ring buffer and collect IO vectors
  uint32_t current_idx = conn->read_idx;
  uint32_t slots_checked = 0;

  // Assuming pipeline_depth tracks how many slots are ready to send
  while (slots_checked < conn->pipeline_depth)
    {
      ResponseSlot *slot = &conn->res_slots[current_idx % K_SLOT_COUNT];

      // Skip empty or already-sent slots (sanity check)
      if (slot->sent >= slot->total_len)
	{
	  slots_checked++;
	  current_idx++;
	  continue;
	}

      // Add this slot's IOVs to our batch
      for (int i = 0; i < slot->iov_cnt; i++)
	{
	  if (iov_count >= BATCH_IOV_LIMIT)
	    goto flush_now;

	  // Adjust base/len for partial writes
	  struct iovec *src = &slot->iov[i];

	  // If we partially sent this slot previously, we need to offset
	  // Logic: Is 'sent' bytes consuming this specific iov?
	  // (Simplified: assuming iov[0] is the whole thing for now, 
	  //  or you handle multi-iov slots correctly in your setup)
	  batch[iov_count].iov_base = (char *) src->iov_base + slot->sent;
	  batch[iov_count].iov_len = src->iov_len - slot->sent;

	  iov_count++;
	}

      slots_checked++;
      current_idx++;
    }

flush_now:
  if (iov_count == 0)
    return IO_OK;

  // 2. SYSCALL STEP
  ssize_t n;
  do
    {
      n = writev (conn->fd, batch, iov_count);
    }
  while (n < 0 && errno == EINTR);

  if (n < 0)
    {
      if (errno == EAGAIN)
	return IO_WAIT;
      return IO_ERROR;
    }

  // 3. ACCOUNTING STEP (The Tricky Part)
  // We wrote 'n' bytes. We must now walk forward and mark slots as sent.
  size_t bytes_left = (size_t) n;

  while (bytes_left > 0 && conn->pipeline_depth > 0)
    {
      ResponseSlot *slot = conn_get_head_slot (conn);
      size_t slot_remain = slot->total_len - slot->sent;

      if (bytes_left >= slot_remain)
	{
	  // Slot fully sent
	  bytes_left -= slot_remain;
	  slot->sent = slot->total_len;	// Mark full

	  // This advances read_idx and pipeline_depth--
	  conn_release_comp_slots (conn);
	}
      else
	{
	  // Slot partially sent (ran out of write bytes)
	  slot->sent += (uint32_t) bytes_left;
	  bytes_left = 0;
	  return IO_WAIT;	// Socket full, we have leftovers
	}
    }

  return IO_OK;
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
