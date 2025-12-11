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

// New structure for a buffer block that is IN-FLIGHT
typedef struct InFlightBuffer {
    uint8_t *data; // Pointer to the dynamically allocated response data
    size_t size;   // Total size of the response
    size_t sent;   // Bytes sent (to support chunking)
    uint32_t pending_ops; // zerocopy_pending for this specific block
    DList list_entry; // For linking to the Conn's in_flight_list
} InFlightBuffer;

typedef struct
{
  int fd;
  uint32_t state;
  uint32_t rbuf_size;
  uint8_t rbuf[4 + K_MAX_MSG + 1];
  
  // Write buffer - where responses are BUILT
  size_t wbuf_size;
  uint8_t wbuf[4 + K_MAX_MSG];
  // Linked list of buffers that have been sent with MSG_ZEROCOPY 
    // and are awaiting EPOLLERR completion.
  DList in_flight_list;  
  // Send buffer - where responses are SENT FROM
  size_t send_buf_size;
  size_t send_buf_sent;
  uint8_t send_buf[4 + K_MAX_MSG];
  
  InFlightBuffer *current_build;
  // Track pending zerocopy operations
  
  uint64_t idle_start;
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
