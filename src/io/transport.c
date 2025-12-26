#include <strings.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <stddef.h>
#include <limits.h>

#include "common/common.h"
#include "io/connection.h"
#include "io/transport.h"

// Linux usually defines IOV_MAX as 1024. 
#define BATCH_IOV_LIMIT 256

IOStatus
transport_read_buffer (Conn *conn)
{
  size_t total_read = 0;
  while (true)
    {
      size_t cap = K_RBUF_SIZE - conn->rbuf_size;
      if (cap == 0)
	conn_compact_rbuf (conn);
      cap = K_RBUF_SIZE - conn->rbuf_size;
      if (cap == 0)
	  return IO_BUF_FULL;

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

HOT IOStatus
transport_write_batch (Conn *conn)
{
  struct iovec batch[BATCH_IOV_LIMIT];
  int iov_count = 0;

  uint32_t current_idx = conn->read_idx;
  uint32_t slots_checked = 0;

  while (slots_checked < conn->pipeline_depth)
    {
      ResponseSlot *slot = &conn->res_slots[current_idx % K_SLOT_COUNT];

      // Skip fully sent slots
      if (slot->sent >= slot->total_len)
	{
	  slots_checked++;
	  current_idx++;
	  continue;
	}

      // Intelligent Offset Logic
      // We calculate offsets dynamically so we don't destroy the original IOV data
      size_t current_offset = slot->sent;

      for (int i = 0; i < slot->iov_cnt; i++)
	{
	  if (iov_count >= BATCH_IOV_LIMIT)
	    goto flush_now;

	  struct iovec *src = &slot->iov[i];
	  if (current_offset >= src->iov_len)
	    {
	      current_offset -= src->iov_len;
	      continue;
	    }

	  batch[iov_count].iov_base = (char *) src->iov_base + current_offset;
	  batch[iov_count].iov_len = src->iov_len - current_offset;
	  iov_count++;
	  current_offset = 0;
	}

      slots_checked++;
      current_idx++;
    }

flush_now:
  if (iov_count == 0)
    return IO_OK;
  ssize_t num;
  do
    {
      num = writev (conn->fd, batch, iov_count);
    }
  while (num < 0 && errno == EINTR);

  if (num < 0)
    {
      if (errno == EAGAIN)
	{
	  // Kernel buffer full. We return IO_WAIT to signal we need EPOLLOUT.
	  return IO_WAIT;
	}
      return IO_ERROR;
    }

  size_t bytes_left = (size_t) num;
  while (bytes_left > 0 && conn->pipeline_depth > 0)
    {
      ResponseSlot *slot = conn_get_head_slot (conn);
      size_t slot_remain = slot->total_len - slot->sent;

      if (bytes_left >= slot_remain)
	{
	  // Slot fully sent
	  bytes_left -= slot_remain;
	  slot->sent = slot->total_len;
	  conn_release_comp_slots (conn);	// Decrements pipeline_depth
	}
      else
	{
	  // Slot partially sent
	  slot->sent += (uint32_t) bytes_left;
	  bytes_left = 0;
	  return IO_WAIT;	// We stopped in the middle -> Socket Full
	}
    }

  return IO_OK;
}
