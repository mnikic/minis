/*
 * connection.h
 *
 *  Created on: Jun 12, 2023
 *      Author: loshmi
 */
#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "common/macros.h"
#include "common/common.h"
#include "io/list.h"
#include "io/proto_defs.h"

// Max chunks per response (Header + Body + Wrap + Footer)
#define K_IOV_PER_SLOT 4

typedef enum
{
  IO_EVENT_READ = EPOLLIN,
  IO_EVENT_WRITE = EPOLLOUT,
  IO_EVENT_ERR = EPOLLERR,
  IO_EVENT_HUP = EPOLLHUP,
  IO_EVENT_RDHUP = EPOLLRDHUP
} IOEvent;

typedef struct
{
  // SCATTER/GATHER METADATA
  struct iovec iov[K_IOV_PER_SLOT];
  int iov_cnt;

  // RING BUFFER ACCOUNTING
  uint32_t wbuf_bytes_used;	// Bytes occupied in wbuf (to advance tail later)
  uint32_t wbuf_gap;		// Bytes skipped at the end of buffer BEFORE this slot

  // LOGICAL METADATA
  uint32_t total_len;		// Logical size (Header + Body + Footer)
  uint32_t sent;		// Bytes pushed to kernel so far
  uint32_t pending_ops;		// Zero-Copy pending ACKs
  bool is_zero_copy;
} ResponseSlot;

typedef enum
{
  STATE_ACTIVE = 0,
  STATE_FLUSH_CLOSE = 2,
  STATE_CLOSE = 3,
} ConnectionState;

typedef struct __attribute__((aligned (64))) Conn
{
  int fd;
  ConnectionState state;
  ProtoType proto;
  uint32_t last_events;
  uint32_t pending_events;

  // Circular Write Buffer
  uint8_t *wbuf;		// The shared chunk
  uint32_t wbuf_size;		// Total size
  uint32_t wbuf_head;		// WRITE pointer
  uint32_t wbuf_tail;		// READ pointer

  // The Read Buffer
  uint8_t *rbuf;
  size_t rbuf_size;
  size_t read_offset;

  // Ring Buffer Metadata
  uint32_t read_idx;		// Consumer Head
  uint32_t write_idx;		// Producer Head
  uint16_t pipeline_depth;

  ResponseSlot res_slots[K_SLOT_COUNT];

  // Management
  uint32_t index_in_active;
  uint32_t next_free_idx;
  uint64_t idle_start;
  DList idle_list;

} Conn;

_Static_assert (sizeof (Conn) % 64 == 0,
		"Conn struct must be cache-line aligned");

// Check if a slot is completely done (sent AND acked for zero_copy)
static ALWAYS_INLINE bool
conn_is_slot_complete (ResponseSlot *slot)
{
  // Changed actual_length -> total_len
  return slot->pending_ops == 0 &&
    slot->sent == slot->total_len && slot->total_len > 0;
}

static ALWAYS_INLINE void
conn_reset_rbuff (Conn *conn)
{
  conn->rbuf_size = 0;
  conn->read_offset = 0;
}

static ALWAYS_INLINE bool
conn_is_rbuff_consumed (Conn *conn)
{
  return conn->read_offset > 0 && conn->read_offset == conn->rbuf_size;
}

static ALWAYS_INLINE bool
conn_has_unprocessed_data (Conn *conn)
{
  return conn->read_offset < conn->rbuf_size;
}

static ALWAYS_INLINE bool
conn_is_res_queue_full (Conn *conn)
{
  return conn->pipeline_depth == K_SLOT_COUNT;
}

// Check if a slot is fully sent (regardless of ACK status)
static ALWAYS_INLINE bool
conn_is_slot_fully_sent (ResponseSlot *slot)
{
  // Changed actual_length -> total_len
  return slot->sent >= slot->total_len;
}

// Check if a slot is empty/available
static ALWAYS_INLINE bool
conn_is_slot_empty (ResponseSlot *slot)
{
  // Changed actual_length -> total_len
  return slot->total_len == 0;
}

static ALWAYS_INLINE void
conn_set_events (Conn *conn, uint32_t events)
{
  DBG_LOGF ("About to set events %u on the %i, old events are %u", events,
	    conn->fd, conn->last_events);
  conn->pending_events = events | EPOLLET;
  DBG_LOGF ("Set events %u on the %i", conn->pending_events, conn->fd);
}

static ALWAYS_INLINE ResponseSlot *
conn_get_head_slot (Conn *conn)
{
  return &conn->res_slots[conn->read_idx];
}

// Reserve a slot (COMMIT step). 
// Updates indices. Does NOT check for full queue (caller must do it).
static ALWAYS_INLINE ResponseSlot *
conn_alloc_slot (Conn *conn)
{
  ResponseSlot *slot = &conn->res_slots[conn->write_idx];

  conn->write_idx = (conn->write_idx + 1) % K_SLOT_COUNT;
  conn->pipeline_depth++;

  return slot;
}

// Releases the head slot and Frees physical memory in the Ring Buffer.
// connection.h (Update inline function)
static ALWAYS_INLINE void
conn_release_head_slot (Conn *conn)
{
  ResponseSlot *slot = &conn->res_slots[conn->read_idx];

  // Handle the Gap (if we wrapped early)
  if (slot->wbuf_gap > 0)
    {
      // The tail MUST be at (Size - Gap). Move it to 0.
      conn->wbuf_tail = 0;
    }

  // Free the physical bytes
  if (slot->wbuf_bytes_used > 0)
    {
      conn->wbuf_tail =
	(conn->wbuf_tail + slot->wbuf_bytes_used) % conn->wbuf_size;
    }

  // Clear Metadata
  slot->wbuf_bytes_used = 0;
  slot->wbuf_gap = 0;
  slot->iov_cnt = 0;
  slot->sent = 0;
  slot->total_len = 0;
  slot->pending_ops = 0;

  conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
  conn->pipeline_depth--;
}

// Circular Buffer: Calculate available bytes
static ALWAYS_INLINE uint32_t
conn_wbuf_free_space (const Conn *conn)
{
  if (conn->wbuf_head >= conn->wbuf_tail)
    {
      // [   T      H    ] 
      return conn->wbuf_size - (conn->wbuf_head - conn->wbuf_tail) - 1;
    }
  return conn->wbuf_tail - conn->wbuf_head - 1;
}

bool conn_has_pending_write (const Conn * conn);
uint32_t conn_release_comp_slots (Conn * conn);
bool conn_is_idle (Conn * conn);
void conn_reset (Conn * conn, int file_desc);
bool conn_has_unsent_data (const Conn * conn);

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
uint8_t *conn_prepare_write_slot (Conn * conn, uint32_t needed_size,
				  uint32_t * out_gap);

/**
 * Finalizes a write to the ring buffer.
 * * 1. Writes the Big Endian length prefix if PROTO_BIN.
 * 2. Populates the current ResponseSlot metadata.
 * 3. Advances the global wbuf_head.
 * 4. Allocates the next slot (conn_alloc_slot).
 */
void
conn_commit_write (Conn * conn, uint8_t * write_ptr, size_t content_len,
		   uint32_t gap, bool allow_zerocopy);

#endif /* CONNECTION_H_ */
