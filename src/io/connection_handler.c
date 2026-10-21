/*
 *============================================================================
 * Name             : connection_handler.c 
 * Description      : Core networking state machine (Circular Buffer Edition)
 *============================================================================
 */
#include <errno.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>

#include "io/connection_handler.h"
#include "io/connection.h"
#include "io/response_queue.h"
#include "io/transport.h"
#include "common/macros.h"
#include "common/common.h"
#include "cache/cache.h"

HOT static void
handle_in_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  while (true)
    {
      IOStatus read_status = transport_read_buffer (conn);

      if (unlikely (read_status == IO_ERROR || read_status == IO_EOF))
	{
	  conn->state = STATE_CLOSE;
	  break;
	}

      QueueStatus q_status =
	response_queue_process_buffered_data (cache, conn, now_us);

      if (q_status == QUEUE_PROGRESSED)
	continue;
      if (q_status == QUEUE_ERROR || conn->state == STATE_CLOSE)
	break;

      // The server loop serves the waiting clients and calls us again
      if (q_status == QUEUE_YIELD)
	break;

      if (unlikely (q_status == QUEUE_STALLED && read_status == IO_BUF_FULL))
	{
	  msgf
	    ("Client %d sent request larger than buffer size even after compacting.",
	     conn->fd);
	  conn->state = STATE_CLOSE;
	  return;
	}

      // Stalled: the socket is full, its next EPOLLOUT resumes us. Done:
      // what is left is part of a request, the rest comes with EPOLLIN.
      break;
    }
}

HOT static void
handle_out_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  IOStatus write_status = transport_write_batch (conn);

  if (unlikely (write_status == IO_ERROR))
    {
      conn->state = STATE_CLOSE;
      return;
    }

  // A client that pushed to waiting clients is resumed by the server
  // loop once they are served, not here
  if (conn_has_unprocessed_data (conn) && !conn->yield)
    handle_in_event (cache, conn, now_us);
}

/*
 * After an error reply the connection closes, but closing a socket with
 * unread input makes the kernel send a reset, and a client still writing
 * its request then gets ECONNRESET instead of reading the error. So once
 * the error is flushed, shut down our side and drop what the client
 * sends until it closes, for at most K_DRAIN_TIMEOUT_US.
 */
#define K_DRAIN_TIMEOUT_US 1000000ULL

// Draining connections. The timeout is fixed, so this is in deadline order.
static DList g_draining = {.prev = &g_draining,.next = &g_draining };

static ALWAYS_INLINE Conn *
conn_of_drain_node (DList *node)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node, Conn, drain_node);
#pragma GCC diagnostic pop
}

static void
drain_input (Conn *conn)
{
  while (true)
    {
      ssize_t num = read (conn->fd, conn->rbuf, K_RBUF_SIZE);
      if (num > 0 || (num < 0 && errno == EINTR))
	continue;
      if (num < 0 && errno == EAGAIN)
	return;
      // The client closed, or the socket failed
      conn->state = STATE_CLOSE;
      return;
    }
}

static void
start_drain (Conn *conn, uint64_t now_us)
{
  shutdown (conn->fd, SHUT_WR);
  conn->state = STATE_DRAIN;
  conn_reset_rbuff (conn);
  conn->drain_deadline_us = now_us + K_DRAIN_TIMEOUT_US;
  dlist_insert_before (&g_draining, &conn->drain_node);
  // Edge triggered: input already waiting raises no new event
  drain_input (conn);
}

uint64_t
conn_drain_next_deadline (void)
{
  if (dlist_empty (&g_draining))
    return (uint64_t) - 1;
  return conn_of_drain_node (g_draining.next)->drain_deadline_us;
}

Conn *
conn_drain_pop_expired (uint64_t now_us)
{
  if (dlist_empty (&g_draining))
    return NULL;
  Conn *conn = conn_of_drain_node (g_draining.next);
  if (conn->drain_deadline_us > now_us)
    return NULL;
  dlist_detach (&conn->drain_node);
  return conn;
}

void
conn_drain_forget (Conn *conn)
{
  if (dlist_is_linked (&conn->drain_node))
    dlist_detach (&conn->drain_node);
}

HOT void
handle_connection_io (Cache *cache, Conn *conn, uint64_t now_us,
		      IOEvent events)
{
  // EPOLLERR, which epoll always reports, is a failed socket
  if (unlikely (events & (EPOLLERR | IO_EVENT_HUP | IO_EVENT_RDHUP)))
    {
      conn->state = STATE_CLOSE;
      return;
    }

  if (unlikely (conn->state == STATE_DRAIN))
    {
      drain_input (conn);
      return;
    }

  if (likely (conn->state == STATE_ACTIVE && (events & IO_EVENT_READ)))
    handle_in_event (cache, conn, now_us);

  if (conn->state != STATE_CLOSE && (events & IO_EVENT_WRITE))
    handle_out_event (cache, conn, now_us);

  // An error reply is out, by now or after waiting for EPOLLOUT
  if (conn->state == STATE_FLUSH_CLOSE && conn->pipeline_depth == 0)
    start_drain (conn, now_us);
}
