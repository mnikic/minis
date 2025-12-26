/*
 *============================================================================
 * Name             : connection_handler.c 
 * Description      : Core networking state machine (Circular Buffer Edition)
 *============================================================================
 */
#include <stdint.h>

#include "io/connection_handler.h"
#include "io/connection.h"
#include "io/response_queue.h"
#include "io/transport.h"
#include "io/zero_copy.h"
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

      bool made_progress =
	response_queue_process_buffered_data (cache, conn, now_us);
      if (conn->state == STATE_CLOSE)
	break;
      if (!made_progress)
	{
	  uint32_t events = IO_EVENT_READ | IO_EVENT_ERR;
	  if (read_status == IO_OK
	      || (read_status == IO_WAIT && conn_has_pending_write (conn)))
	    events |= IO_EVENT_WRITE;

	  conn_set_events (conn, events);
	  break;
	}
    }
}

HOT static void
handle_out_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  DBG_LOGF ("FD %d: Handling IO_EVENT_WRITE event", conn->fd);

  IOStatus status = response_queue_flush (conn);

  if (unlikely (status == IO_ERROR))
    {
      conn->state = STATE_CLOSE;
      return;
    }

  if (status == IO_WAIT)
    {
      conn_set_events (conn, IO_EVENT_READ | IO_EVENT_WRITE | IO_EVENT_ERR);
      return;
    }

  if (unlikely
      (conn->state == STATE_FLUSH_CLOSE && conn->pipeline_depth == 0))
    {
      conn->state = STATE_CLOSE;
      return;
    }

  // Jump directly to data pump if advanageous
  if (status == IO_OK && conn_has_unprocessed_data (conn))
    {
      response_queue_process_buffered_data (cache, conn, now_us);
      return;
    }

  uint32_t events = IO_EVENT_READ | IO_EVENT_ERR;
  if (conn_has_unsent_data (conn))
    events |= IO_EVENT_WRITE;

  conn_set_events (conn, events);
}

static void
handle_err_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  DBG_LOGF ("FD %d: Handling IO_EVENT_ERR event", conn->fd);

  bool progress = false;
  while (zc_process_completions (conn))
    {
      progress = true;
    }

  if (progress && conn_has_unprocessed_data (conn))
    {
      handle_in_event (cache, conn, now_us);
    }
}

HOT void
handle_connection_io (Cache *cache, Conn *conn, uint64_t now_us,
		      IOEvent events)
{
  if (unlikely (events & (IO_EVENT_HUP | IO_EVENT_RDHUP)))
    {
      conn->state = STATE_CLOSE;
      return;
    }

  if (conn->state != STATE_CLOSE && (events & IO_EVENT_ERR))
    handle_err_event (cache, conn, now_us);

  if (likely (conn->state == STATE_ACTIVE && (events & IO_EVENT_READ)))
    handle_in_event (cache, conn, now_us);

  if (conn->state != STATE_CLOSE && (events & IO_EVENT_WRITE))
    handle_out_event (cache, conn, now_us);
}
