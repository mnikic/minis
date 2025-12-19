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
  Conn **by_fd;			// Sparse: FD -> Conn*
  Conn **active;		// Dense:  0..N -> Conn* (For fast looping)
  size_t capacity;		// Size of by_fd (Max FD)
  size_t active_count;		// Number of active clients

  // STORAGE (The Slab)
  Conn *storage;		// The giant contiguous array of Conn structs
  uint32_t max_conns;		// Physical limit of concurrent connections

  // ALLOCATION TRACKING
  uint32_t free_head;		// Head of the free list (in storage)

} ConnPool;

ConnPool *connpool_new (uint32_t max_conns);

// Replaces malloc(sizeof(Conn)) + connpool_add
Conn *connpool_get (ConnPool * pool, int file_desc);

// Replaces connpool_remove + free
void connpool_release (ConnPool * pool, Conn * conn);

// Fast lookup (unchanged interface, faster internals)
Conn *connpool_lookup (ConnPool * pool, int file_desc);

void connpool_iter (ConnPool * pool, Conn *** connections, size_t *count);

// Destroy everything
void connpool_free (ConnPool * pool);

#endif /* CONN_POOL_H_ */
