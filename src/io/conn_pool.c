/*
 * conn_pool.c 
 *
 * Created on: Jun 12, 2023
 * Author: loshmi
 */
#include "conn_pool.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stddef.h>

#include "common/common.h"
#include "common/macros.h"

/*
 * We use this internal union to force the physical memory layout
 * of the pool to be aligned to 64-byte cache lines.
 *
 * The 'padding' member automatically calculates the gap needed
 * so that sizeof(ConnSlot) is a perfect multiple of 64.
 *
 * This ensures that when we iterate like slots[i], the address
 * jumps exactly 64, 128, 192, etc., preventing AVX alignment crashes.
 */
typedef union __attribute__((aligned (64))) ConnSlot
{
  Conn data;
  // Round up sizeof(Conn) to next multiple of 64
  char padding[((sizeof (Conn) + 63) / 64) * 64];
}

ConnSlot;

// Sanity Check: Ensure Conn is at offset 0 (Crucial for pointer casting)
_Static_assert (offsetof (ConnSlot, data) == 0,
		"Conn must be at start of ConnSlot");


// Helper: Ensure the sparse FD array is big enough
static inline void
ensure_fd_capacity (ConnPool *pool, int file_desc)
{
  if ((size_t) file_desc < pool->capacity)
    return;

  size_t new_cap = pool->capacity;
  while (new_cap <= (size_t) file_desc)
    new_cap *= 2;

  // Realloc the sparse array
  Conn **new_by_fd = realloc (pool->by_fd, new_cap * sizeof (Conn *));
  if (unlikely (!new_by_fd))
    {
      die ("OOM expanding FD table");
    }

  pool->by_fd = new_by_fd;

  // Initialize the new section to NULL
  memset (pool->by_fd + pool->capacity, 0,
	  (new_cap - pool->capacity) * sizeof (Conn *));

  pool->capacity = new_cap;
}

COLD ConnPool *
connpool_new (uint32_t max_conns)
{
  ConnPool *pool = calloc (1, sizeof (ConnPool));
  if (unlikely (!pool))
    return NULL;

  pool->max_conns = max_conns;

  // Calculate total size using the ALIGNED slot size (192 bytes, not 152)
  size_t total_size = max_conns * sizeof (ConnSlot);

  // Allocate aligned memory (Linux/C11 standard)
  void *mem = aligned_alloc (64, total_size);

  if (unlikely (!mem))
    {
      free (pool);
      return NULL;
    }

  // Cast void* to our public type. Safe because offset is 0.
  pool->storage = (Conn *) mem;
  memset (pool->storage, 0, total_size);

  // Initialize the Free List using correct stride logic
  ConnSlot *slots = (ConnSlot *) mem;
  for (uint32_t i = 0; i < max_conns - 1; i++)
    {
      slots[i].data.next_free_idx = i + 1;
    }
  slots[max_conns - 1].data.next_free_idx = UINT32_MAX;
  pool->free_head = 0;

  // Allocate Dense Active Array (Pointers do not need special alignment)
  pool->active = calloc (max_conns, sizeof (Conn *));
  if (unlikely (!pool->active))
    {
      free (pool->storage);
      free (pool);
      return NULL;
    }

  // Allocate Sparse FD Map
  pool->capacity = 1024;
  pool->by_fd = calloc (pool->capacity, sizeof (Conn *));
  if (unlikely (!pool->by_fd))
    {
      free (pool->active);
      free (pool->storage);
      free (pool);
      return NULL;
    }

  return pool;
}

Conn *
connpool_get (ConnPool *pool, int file_desc)
{
  // Check if pool is full
  if (pool->free_head == UINT32_MAX)
    return NULL;

  ensure_fd_capacity (pool, file_desc);

  // Pop from Slab Free List
  uint32_t idx = pool->free_head;
  /*
   * Casting via (void*) suppresses -Wcast-align by resetting type assumptions.
   */
  ConnSlot *slots = (ConnSlot *) ((void *)pool->storage);
  ConnSlot *slot = &slots[idx];
  Conn *conn = &slot->data;

  pool->free_head = conn->next_free_idx;

  // Initialize Identity
  conn->fd = file_desc;
  conn->next_free_idx = UINT32_MAX;	// Mark as in-use
  conn->state = STATE_ACTIVE;
  conn->last_events = 0;

  // Reset Ring Buffer State
  conn->read_idx = 0;
  conn->write_idx = 0;
  conn->rbuf_size = 0;
  conn->read_offset = 0;
  memset (conn->res_slots, 0, sizeof (conn->res_slots));
  dlist_init (&conn->idle_list);

  // LAZY ALLOCATION (Persistent): 
  // We allocate the heavy buffers only if they are NULL (first use).
  // If this slot was used before, we REUSE the existing buffers.
  if (!conn->rbuf)
    conn->rbuf = malloc (K_RBUF_SIZE);

  if (!conn->res_data)
    conn->res_data = malloc (K_WBUF_SIZE);

  if (unlikely (!conn->rbuf || !conn->res_data))
    {
      // OOM Recovery: Return to pool
      if (conn->rbuf)
	{
	  free (conn->rbuf);
	  conn->rbuf = NULL;
	}
      if (conn->res_data)
	{
	  free (conn->res_data);
	  conn->res_data = NULL;
	}

      conn->next_free_idx = pool->free_head;
      pool->free_head = idx;
      return NULL;
    }

  // Add to Active Lists
  conn->index_in_active = (uint32_t) pool->active_count;
  pool->active[pool->active_count++] = conn;
  pool->by_fd[file_desc] = conn;

  return conn;
}

void
connpool_release (ConnPool *pool, Conn *conn)
{
  if (!conn)
    return;
  int file_desc = conn->fd;

  // Remove from Active Array (Swap-with-Last O(1))
  size_t idx = conn->index_in_active;
  size_t last_idx = pool->active_count - 1;

  if (idx != last_idx)
    {
      Conn *moved = pool->active[last_idx];
      pool->active[idx] = moved;
      moved->index_in_active = (uint32_t) idx;
    }
  pool->active[last_idx] = NULL;
  pool->active_count--;

  // Remove from lookup map
  if ((size_t) file_desc < pool->capacity)
    {
      pool->by_fd[file_desc] = NULL;
    }

  // STRIDE MAGIC: Reverse calculation
  // We use pointer subtraction on ConnSlot* to get the correct index
  ConnSlot *base = (ConnSlot *) (void*) pool->storage;
  ConnSlot *target = (ConnSlot *) (void*) conn;	// Safe cast via offset 0
  uint32_t storage_idx = (uint32_t) (target - base);

  conn->next_free_idx = pool->free_head;
  pool->free_head = storage_idx;
  conn->fd = -1;
}

COLD void
connpool_iter (ConnPool *pool, Conn ***connections, size_t *count)
{
  if (!pool)
    {
      *connections = NULL;
      *count = 0;
      return;
    }

  // Return the raw pointer to the dense array.
  *connections = pool->active;
  *count = pool->active_count;
}

COLD void
connpool_free (ConnPool *pool)
{
  if (!pool)
    return;

  ConnSlot *slots = (ConnSlot *) (void*) pool->storage;
  for (uint32_t i = 0; i < pool->max_conns; i++)
    {
      if (slots[i].data.rbuf)
	free (slots[i].data.rbuf);
      if (slots[i].data.res_data)
	free (slots[i].data.res_data);
    }

  free (pool->storage);
  free (pool->active);
  free (pool->by_fd);
  free (pool);
}
