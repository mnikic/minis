/*
 * conn_pool.c 
 *
 * Created on: Jun 12, 2023
 * Author: loshmi
 *
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
#include "io/connection.h"

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

  size_t total_size = max_conns * sizeof (Conn);
  void *mem = NULL;
  int err = posix_memalign (&mem, 64, total_size);

  if (unlikely (err != 0 || !mem))
    {
      free (pool);
      return NULL;
    }
  pool->storage = (Conn *) mem;
  memset (pool->storage, 0, total_size);

  for (uint32_t i = 0; i < max_conns - 1; i++)
    {
      pool->storage[i].next_free_idx = i + 1;
    }
  pool->storage[max_conns - 1].next_free_idx = UINT32_MAX;
  pool->free_head = 0;

  pool->active = calloc (max_conns, sizeof (Conn *));
  if (unlikely (!pool->active))
    {
      free (pool->storage);
      free (pool);
      return NULL;
    }

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
  if (pool->free_head == UINT32_MAX)
    return NULL;
  ensure_fd_capacity (pool, file_desc);
  uint32_t idx = pool->free_head;
  Conn *conn = &pool->storage[idx];
  pool->free_head = conn->next_free_idx;

  conn->fd = file_desc;
  conn->next_free_idx = UINT32_MAX;

  if (!conn->rbuf)
    {
      conn->rbuf = malloc (K_RBUF_SIZE);
      conn->wbuf_size = K_WBUF_SIZE;
    }

  if (!conn->wbuf)
    conn->wbuf = malloc (K_WBUF_SIZE);

  if (unlikely (!conn->rbuf || !conn->wbuf))
    {
      // OOM Recovery: Return to pool
      if (conn->rbuf)
	{
	  free (conn->rbuf);
	  conn->rbuf = NULL;
	}
      if (conn->wbuf)
	{
	  free (conn->wbuf);
	  conn->wbuf = NULL;
	}

      conn->next_free_idx = pool->free_head;
      pool->free_head = idx;
      return NULL;
    }

  conn_reset (conn, file_desc);
  conn->index_in_active = (uint32_t) pool->active_count;
  pool->active[pool->active_count++] = conn;
  pool->by_fd[file_desc] = conn;

  return conn;
}

COLD void
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

  // STANDARD MATH:
  // Pointer subtraction automatically divides by sizeof(Conn).
  uint32_t storage_idx = (uint32_t) (conn - pool->storage);

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

  *connections = pool->active;
  *count = pool->active_count;
}

COLD void
connpool_free (ConnPool *pool)
{
  if (!pool)
    return;

  for (uint32_t i = 0; i < pool->max_conns; i++)
    {
      if (pool->storage[i].rbuf)
	free (pool->storage[i].rbuf);
      if (pool->storage[i].wbuf)
	free (pool->storage[i].wbuf);
    }

  free (pool->storage);
  free (pool->active);
  free (pool->by_fd);
  free (pool);
}
