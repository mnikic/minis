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

      QueueStatus q_status =
	response_queue_process_buffered_data (cache, conn, now_us);

      if (q_status == QUEUE_PROGRESSED)
	continue;
      if (q_status == QUEUE_ERROR || conn->state == STATE_CLOSE)
	break;

      if (unlikely
	  ((q_status != QUEUE_PROGRESSED && q_status != QUEUE_DONE)
	   && read_status == IO_BUF_FULL))
	{
	  msgf
	    ("Client %d sent request larger than buffer size even after compacting.", conn->fd);
	  conn->state = STATE_CLOSE;
	  return;
	}

      if (q_status == QUEUE_STALLED)
	{
	  conn_set_events (conn,
			   IO_EVENT_READ | IO_EVENT_WRITE | IO_EVENT_ERR);
	  break;
	}

      if (q_status == QUEUE_DONE)
	{
	  uint32_t events = IO_EVENT_READ | IO_EVENT_ERR;
	  if (read_status == IO_OK || conn_has_pending_write (conn))
	    events |= IO_EVENT_WRITE;
	  conn_set_events (conn, events);
	  break;
	}
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

  if (conn_has_unprocessed_data (conn))
    {
      handle_in_event (cache, conn, now_us);
      return;			// handle_in_event set the events. We are done.
    }

  uint32_t events = IO_EVENT_READ | IO_EVENT_ERR;

  if (conn_has_pending_write (conn))
    {
      events |= IO_EVENT_WRITE;
    }

  conn_set_events (conn, events);
}

static void
handle_err_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  die ("We are in err");
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
