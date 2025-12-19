/*
 * connections.c
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

#include "common/common.h"

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
  if (!new_by_fd)
    {
      die ("OOM expanding FD table");
    }

  pool->by_fd = new_by_fd;

  // Initialize the new section to NULL
  memset (pool->by_fd + pool->capacity, 0,
	  (new_cap - pool->capacity) * sizeof (Conn *));

  pool->capacity = new_cap;
}

ConnPool *
connpool_new (uint32_t max_conns)
{
  ConnPool *pool = calloc (1, sizeof (ConnPool));
  if (!pool)
    return NULL;

  pool->max_conns = max_conns;
  pool->storage = calloc (max_conns, sizeof (Conn));
  if (!pool->storage)
    {
      free (pool);
      return NULL;
    }

  // storage[0].next = 1, storage[1].next = 2, ...
  for (uint32_t i = 0; i < max_conns - 1; i++)
    {
      pool->storage[i].next_free_idx = i + 1;
    }
  pool->storage[max_conns - 1].next_free_idx = UINT32_MAX;
  pool->free_head = 0;

  // Allocate Dense Active Array
  pool->active = calloc (max_conns, sizeof (Conn *));

  // Allocate Sparse FD Map (Start small)
  pool->capacity = 1024;
  pool->by_fd = calloc (pool->capacity, sizeof (Conn *));

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
  Conn *conn = &pool->storage[idx];
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

  // LAZY ALLOCATION: "Split Brain"
  // We allocate the heavy buffers here only if they are not there.
  if (!conn->rbuf)
    conn->rbuf = malloc (K_RBUF_SIZE);

  if (!conn->res_data)
    conn->res_data = malloc (K_WBUF_SIZE);

  if (!conn->rbuf || !conn->res_data)
    {
      // OOM Recovery: Return to pool
      if (conn->rbuf)
	free (conn->rbuf);
      if (conn->res_data)
	free (conn->res_data);
      conn->rbuf = NULL;
      conn->res_data = NULL;

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

  if ((size_t) file_desc < pool->capacity)
    {
      pool->by_fd[file_desc] = NULL;
    }

  uint32_t storage_idx = (uint32_t) (conn - pool->storage);
  conn->next_free_idx = pool->free_head;
  pool->free_head = storage_idx;

  conn->fd = -1;
}

Conn *
connpool_lookup (ConnPool *pool, int file_desc)
{
  if (file_desc < 0 || (size_t) file_desc >= pool->capacity)
    return NULL;
  return pool->by_fd[file_desc];
}

void
connpool_iter (ConnPool *pool, Conn ***connections, size_t *count)
{
  if (!pool)
    {
      *connections = NULL;
      *count = 0;
      return;
    }

  // Return the raw pointer to the dense array.
  // Warning: The caller must iterate backwards if they plan to modify/remove 
  // connections during iteration!
  *connections = pool->active;
  *count = pool->active_count;
}

void
connpool_free (ConnPool *pool)
{
  if (!pool)
    return;

  // Free buffers for all connections in storage
  for (uint32_t i = 0; i < pool->max_conns; i++)
    {
      if (pool->storage[i].rbuf)
	free (pool->storage[i].rbuf);
      if (pool->storage[i].res_data)
	free (pool->storage[i].res_data);
    }

  free (pool->storage);
  free (pool->active);
  free (pool->by_fd);
  free (pool);
}
