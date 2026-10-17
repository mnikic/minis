#include "io/blocking.h"

#include "io/list.h"
#include "common/macros.h"

static DList g_waiters = {.prev = &g_waiters,.next = &g_waiters };
static size_t g_count;
static bool g_serving;

static ALWAYS_INLINE Conn *
conn_of_block_node (DList *node)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node, Conn, block_node);
#pragma GCC diagnostic pop
}

void
blocking_park (Conn *conn, uint64_t timeout_us, uint64_t now_us)
{
  if (conn->blocked)
    return;
  conn->blocked = true;
  conn->block_deadline_us = timeout_us == 0 ? 0 : now_us + timeout_us;
  dlist_insert_before (&g_waiters, &conn->block_node);
  g_count++;
}

void
blocking_unpark (Conn *conn)
{
  if (!conn->blocked)
    return;
  conn->blocked = false;
  conn->block_deadline_us = 0;
  dlist_detach (&conn->block_node);
  g_count--;
}

size_t
blocking_count (void)
{
  return g_count;
}

Conn *
blocking_rotate (void)
{
  if (dlist_empty (&g_waiters))
    return NULL;
  DList *oldest = g_waiters.next;
  dlist_detach (oldest);
  dlist_insert_before (&g_waiters, oldest);
  return conn_of_block_node (oldest);
}

// A linear scan: waiting clients are few, and this keeps parking O(1)
uint64_t
blocking_next_deadline (void)
{
  uint64_t next = (uint64_t) - 1;
  for (DList *node = g_waiters.next; node != &g_waiters; node = node->next)
    {
      uint64_t deadline = conn_of_block_node (node)->block_deadline_us;
      if (deadline != 0 && deadline < next)
	next = deadline;
    }
  return next;
}

bool
blocking_is_serving (void)
{
  return g_serving;
}

void
blocking_set_serving (bool serving)
{
  g_serving = serving;
}
