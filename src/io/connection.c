#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "common/common.h"
#include "io/connection.h"
#include "io/proto_defs.h"

/**
 * Finalizes a write to the ring buffer.
 * 1. Writes the Big Endian length prefix if PROTO_BIN.
 * 2. Populates the current ResponseSlot metadata.
 * 3. Advances the global wbuf_head.
 * 4. Allocates the next slot (conn_alloc_slot).
 */
void
conn_commit_write (Conn *conn, uint8_t *write_ptr, size_t content_len,
		   uint32_t gap, bool allow_zerocopy)
{
  // Calculate total frame size (Content + Header)
  uint32_t header_size = (conn->proto == PROTO_BIN) ? BIN_HEADER_SIZE : 0;
  uint32_t total_len = (uint32_t) content_len + header_size;

  // Fill Binary Header (Length Prefix)
  if (conn->proto == PROTO_BIN)
    {
      uint32_t nwlen = htonl ((uint32_t) content_len);
      memcpy (write_ptr, &nwlen, 4);
    }

  // 2. Populate ResponseSlot
  ResponseSlot *slot = &conn->res_slots[conn->write_idx];

  slot->wbuf_gap = gap;
  slot->wbuf_bytes_used = total_len;
  slot->iov[0].iov_base = write_ptr;
  slot->iov[0].iov_len = total_len;
  slot->iov_cnt = 1;

  slot->sent = 0;
  slot->pending_ops = 0;
  slot->total_len = total_len;

  // Zero Copy Logic: Only if allowed AND threshold met
  if (allow_zerocopy && conn->proto == PROTO_BIN
      && total_len > K_ZEROCPY_THRESHOLD)
    {
      slot->is_zero_copy = true;
    }
  else
    {
      slot->is_zero_copy = false;
    }

  conn->wbuf_head += total_len;
  conn_alloc_slot (conn);
}

/**
 * Prepares the ring buffer for a write of 'needed_size'.
 * * Logic:
 * 1. Checks if data fits contiguously at the current wbuf_head.
 * 2. If not, checks if it fits at the beginning (Wrap).
 * 3. Updates wbuf_head to 0 if a wrap occurs.
 * 4. Calculates the 'gap' (wasted bytes at the end) if wrapping.
 *
 * @param conn         Pointer to the Connection.
 * @param needed_size  How many bytes we intend to write.
 * @param out_gap      OUTPUT: The number of bytes skipped (gap). 
 * Assign this to slot->wbuf_gap.
 * @return             Pointer to the write location, or NULL if full.
 */
uint8_t *
conn_prepare_write_slot (Conn *conn, uint32_t needed_size, uint32_t *out_gap)
{
  // Default gap is 0 (assume contiguous write)
  *out_gap = 0;

  uint32_t space_at_end = conn->wbuf_size - conn->wbuf_head;

  // --- SCENARIO A: Fits at the end? ---
  if (needed_size <= space_at_end)
    {

      // If Tail is "in front" of Head, we are constrained by it.
      // (If Tail <= Head, the tail is "behind" us, so we are only limited by wbuf_size)
      if (conn->wbuf_tail > conn->wbuf_head)
	{
	  uint32_t available = conn->wbuf_tail - conn->wbuf_head;

	  // We strictly need (available - 1) to ensure Head never equals Tail (which implies Empty)
	  if (needed_size >= available)
	    {
	      return NULL;	// Buffer Full
	    }
	}

      // It fits. Return current pointer.
      return conn->wbuf + conn->wbuf_head;
    }

  // --- SCENARIO B: Wrap Around Required ---
  // It didn't fit at the end. We need to wrap to index 0.

  // We need 'needed_size' bytes starting from 0.
  // We must ensure we don't crash into wbuf_tail.
  // If tail is 0, we can't wrap (no space at start).
  // We generally need needed_size < wbuf_tail to leave 1 byte breathing room.
  if (conn->wbuf_tail == 0 || needed_size >= conn->wbuf_tail)
    {
      return NULL;		// Buffer Full (cannot wrap)
    }

  *out_gap = space_at_end;

  // Reset Head to 0
  // IMPORTANT: We move head to 0 immediately so the returned pointer is valid.
  // The caller is responsible for adding 'needed_size' to head after writing.
  conn->wbuf_head = 0;
  return conn->wbuf;
}

// Check if there is actual DATA that needs to be written to the socket.
// (Ignores slots that are fully sent but just waiting for ZC ACKs)
bool
conn_has_unsent_data (const Conn *conn)
{
  uint32_t idx = conn->read_idx;
  uint32_t count = conn->pipeline_depth;

  while (count > 0)
    {
      const ResponseSlot *slot = &conn->res_slots[idx];

      // We only care if we haven't pushed all bytes to the kernel yet.
      if (slot->total_len > 0 && slot->sent < slot->total_len)
	{
	  return true;
	}

      // Note: We deliberately SKIP the ZC pending_ops check here.
      // If we are waiting for ACKs, we don't need EPOLLOUT.

      idx = (idx + 1) % K_SLOT_COUNT;
      count--;
    }

  return false;
}

// Release completed slots from the ring buffer
// Returns: number of slots released
uint32_t
conn_release_comp_slots (Conn *conn)
{
  uint32_t released_count = 0;
  while (conn->pipeline_depth > 0)
    {
      ResponseSlot *slot = conn_get_head_slot (conn);
      if (!conn_is_slot_complete (slot))
	break;

      if (slot->wbuf_gap > 0)
	{
	  conn->wbuf_tail = 0;	// Skip the gap
	}

      if (slot->wbuf_bytes_used > 0)
	{
	  conn->wbuf_tail =
	    (conn->wbuf_tail + slot->wbuf_bytes_used) % conn->wbuf_size;
	}
      // Reset everything
      slot->wbuf_bytes_used = 0;
      slot->wbuf_gap = 0;
      slot->total_len = 0;
      slot->sent = 0;
      slot->iov_cnt = 0;
      slot->pending_ops = 0;

      conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
      released_count++;
      conn->pipeline_depth--;
    }
  return released_count;
}

bool
conn_is_idle (Conn *conn)
{
  if (conn->state != STATE_ACTIVE)
    {
      return false;
    }

  bool no_pending_responses = (conn->pipeline_depth == 0);
  bool no_unprocessed_reqs = !conn_has_unprocessed_data (conn);
  return no_pending_responses && no_unprocessed_reqs;
}

void
conn_reset (Conn *conn, int file_desc)
{
  conn->fd = file_desc;
  conn->state = STATE_ACTIVE;
  conn->last_events = 0;
  conn->pending_events = 0;

  // Reset Ring Buffer State
  conn->read_idx = 0;
  conn->write_idx = 0;
  conn->rbuf_size = 0;
  conn->pipeline_depth = 0;
  conn->read_offset = 0;
  conn->wbuf_head = 0;
  conn->wbuf_tail = 0;
  conn->proto = PROTO_BIN;
  memset (conn->res_slots, 0, sizeof (conn->res_slots));

  conn->idle_start = 0;		// Will be set externally
  dlist_init (&conn->idle_list);
}

bool
conn_has_pending_write (const Conn *conn)
{
  uint32_t idx = conn->read_idx;
  uint32_t count = conn->pipeline_depth;

  while (count > 0)
    {
      const ResponseSlot *slot = &conn->res_slots[idx];

      // Data waiting to be sent?
      if (slot->total_len > 0 && slot->sent < slot->total_len)
	{
	  return true;
	}

      // Zero-Copy waiting for Kernel?
      if (slot->is_zero_copy && slot->pending_ops > 0)
	{
	  return true;
	}

      idx = (idx + 1) % K_SLOT_COUNT;
      count--;
    }

  return false;
}
