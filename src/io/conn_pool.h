/*
 * conn_pool.h
 *
 *  Created on: Jun 12, 2023
 *      Author: loshmi
 */
#ifndef CONN_POOL_H_
#define CONN_POOL_H_

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include "io/connection.h"

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

#endif /* CONN_POOL_H_ */
