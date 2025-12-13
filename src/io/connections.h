/*
 * connections.h
 *
 *  Created on: Jun 12, 2023
 *      Author: loshmi
 */
#ifndef CONNECTIONS_H_
#define CONNECTIONS_H_

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include "list.h"
#include "common/common.h"

typedef enum
{
  STATE_REQ = 0,
  STATE_RES = 1,
  STATE_RES_CLOSE = 2,		// Send response then close connection
  STATE_END = 3,		// Mark the connection for deletion
} ConnectionState;

typedef struct
{
  size_t actual_length;		// The total length (4-byte header + payload) of the response. 
  // Set to 0 if the slot is free.
} ResponseSlot;

typedef struct
{
  int fd;
  ConnectionState state;
  uint32_t rbuf_size;
  size_t read_offset;
  uint8_t rbuf[4 + K_MAX_MSG + 1];	// Added 1 extra for ease of string in place 0 termination.

  // Metadata for each response slot
  ResponseSlot res_slots[K_SLOT_COUNT];

  // Continuous memory for all response payloads (K_SLOT_COUNT * K_MAX_MSG)
  // The size of the memory block is now K_SLOT_COUNT * K_MAX_MSG
  uint8_t res_data[K_SLOT_COUNT * K_MAX_MSG];

  // Read index: Points to the oldest response ready to be SENT
  uint32_t read_idx;
  size_t res_sent;		// Progress tracker: How many bytes of 
  // res_slots[read_idx] have been sent.

  // Write index: Points to the next free slot ready to be WRITTEN
  uint32_t write_idx;

  uint64_t idle_start;
  uint32_t last_events;		// cache, to not EPOLL_CTL_MOD if there is no need
  DList idle_list;
} Conn;

typedef struct
{
  Conn *conn;			// The connection pointer
  uint32_t index_in_active;	// Index in the active array
} PoolEntry;

typedef struct
{
  PoolEntry **by_fd;		// Lookup table indexed by fd
  Conn **active;		// Dense array of active connections
  uint32_t *fd_bitmap;		// Bitmap of registered file descriptors
  size_t capacity;
  size_t active_count;
} ConnPool;

ConnPool *connpool_new (uint32_t capacity);
Conn *connpool_lookup (ConnPool * pool, int file_des);
void connpool_add (ConnPool * pool, Conn * connection);
void connpool_remove (ConnPool * pool, int file_des);
void connpool_free (ConnPool * pool);
void connpool_iter (ConnPool * pool, Conn *** connections, size_t *count);

#endif /* CONNECTIONS_H_ */
