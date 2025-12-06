/*
 * connections.h
 *
 *  Created on: Jun 12, 2023
 *      Author: loshmi
 */
#ifndef CONNECTIONS_H_
#define CONNECTIONS_H_

#include <stdlib.h>
#include <stdint.h>
#include "list.h"
#include "common.h"

typedef struct
{
  int fd;
  uint32_t state;
  uint32_t rbuf_size;
  uint8_t rbuf[4 + K_MAX_MSG];
  size_t wbuf_size;
  size_t wbuf_sent;
  uint8_t wbuf[4 + K_MAX_MSG];
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
