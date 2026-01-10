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
#include "common/macros.h"

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

COLD ConnPool *connpool_new (uint32_t max_conns);

// Replaces malloc(sizeof(Conn)) + connpool_add
Conn *connpool_get (ConnPool * pool, int file_desc);

// Replaces connpool_remove + free
void connpool_release (ConnPool * pool, Conn * conn);

HOT static ALWAYS_INLINE Conn *
connpool_lookup (ConnPool *pool, int file_desc)
{
  if (unlikely (file_desc < 0 || (size_t) file_desc >= pool->capacity))
    return NULL;

  return pool->by_fd[file_desc];
}

COLD void connpool_iter (ConnPool * pool, Conn *** connections,
			 size_t *count);

// Destroy everything
COLD void connpool_free (ConnPool * pool);

#endif /* CONN_POOL_H_ */
