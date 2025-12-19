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
#include "list.h"
#include "common/common.h"

typedef struct
{
  uint32_t actual_length;	// The total size of the response (Header + Payload)
  uint32_t pending_ops;		// Count of sendmsg() operations waiting for completion
  uint32_t sent;		// Bytes already passed to sendmsg() (may not be confirmed yet)
  bool is_zerocopy;
} ResponseSlot;

typedef enum
{
  STATE_ACTIVE = 0,
  STATE_FLUSH_CLOSE = 2,
  STATE_CLOSE = 3,
} ConnectionState;

typedef struct Conn
{
  int fd;
  ConnectionState state;
  uint32_t last_events;		// To avoid redundant epoll_ctl

  // Ring Buffer Metadata
  uint32_t read_idx;		// Oldest slot to SEND
  uint32_t write_idx;		// Next slot to WRITE
  ResponseSlot res_slots[K_SLOT_COUNT];

  // Buffer Pointers (The "Split Brain")
  // These point to the heavy heap memory.
  uint8_t *rbuf;		// Points to malloc'd Read Buffer
  size_t rbuf_size;		// Current usage
  size_t read_offset;		// Parsing offset

  uint8_t *res_data;		// Points to malloc'd Write Buffer Block

  // Pool Management
  uint32_t index_in_active;	// For O(1) removal from active list
  uint32_t next_free_idx;	// For the Slab's free list

  // Idle Management
  uint64_t idle_start;
  DList idle_list;
} Conn;

// Check if a slot is completely done (sent AND acked for zerocopy)
static inline bool
is_slot_complete (ResponseSlot *slot)
{
  return slot->pending_ops == 0 &&
    slot->sent == slot->actual_length && slot->actual_length > 0;
}

// Reset read buffer to initial state (fully consumed)
static inline void
reset_read_buffer (Conn *conn)
{
  conn->rbuf_size = 0;
  conn->read_offset = 0;
}

// Check if read buffer is fully consumed
static inline bool
is_read_buffer_consumed (Conn *conn)
{
  return conn->read_offset > 0 && conn->read_offset == conn->rbuf_size;
}

// Check if there's unprocessed data in read buffer
static inline bool
has_unprocessed_data (Conn *conn)
{
  return conn->read_offset < conn->rbuf_size;
}

static inline uint8_t *
get_slot_data_ptr (Conn *conn, uint32_t slot_idx)
{
  return &conn->res_data[(size_t) (slot_idx * K_MAX_MSG)];
}

static inline bool
is_res_queue_full (Conn *conn)
{
  return ((conn->write_idx + 1) % K_SLOT_COUNT) == conn->read_idx;
}

// Check if a slot is fully sent (regardless of ACK status)
static inline bool
is_slot_fully_sent (ResponseSlot *slot)
{
  return slot->sent >= slot->actual_length;
}

// Check if a slot is empty/available
static inline bool
is_slot_empty (ResponseSlot *slot)
{
  return slot->actual_length == 0;
}

// Release completed slots from the ring buffer
// Returns: number of slots released
uint32_t release_completed_slots (Conn * conn);

bool is_connection_idle (Conn * conn);

#endif /* CONNECTION_H_ */
