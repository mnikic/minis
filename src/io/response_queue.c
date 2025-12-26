#include <stdint.h>

#include "io/response_queue.h"
#include "io/protocol_handler.h"
#include "io/zero_copy.h"
#include "io/connection.h"
#include "io/transport.h"
#include "common/macros.h"
#include "cache/cache.h"

#define K_MAX_REQ_PER_TICK 1024

// Process incoming data and generate responses
HOT static bool
response_queue_process_input (Cache *cache, Conn *conn, uint64_t now_us)
{
  RequestContext ctx = {
    .cache = cache,
    .conn = conn,
    .now_us = now_us
  };

  uint_fast16_t budget = K_MAX_REQ_PER_TICK;

  while (budget > 0)
    {
      if (unlikely (conn_is_res_queue_full (conn)))
	break;

      if (!protocol_try_one_request (&ctx))
	break;

      if (unlikely (conn->state == STATE_CLOSE))
	break;

      budget--;
    }

  // If we exhausted budget, signal that more work remains
  if (budget == 0)
    return false;

  // Check if we consumed all input
  if (conn_is_rbuff_consumed (conn))
    {
      conn_reset_rbuff (conn);
    }

  return true;
}

HOT void
response_queue_process_buffered_data (Cache *cache, Conn *conn,
				      uint64_t now_us)
{
  // THE DATA PUMP: Process requests and send responses
  while (true)
    {
      if (unlikely (conn->state == STATE_CLOSE))
	return;

      bool input_consumed =
	response_queue_process_input (cache, conn, now_us);

      if (conn->state == STATE_CLOSE)
	return;

      // Try to flush responses
      if (conn->pipeline_depth > 0)
	{
	  IOStatus write_status = response_queue_flush (conn);

	  if (unlikely (write_status == IO_ERROR))
	    return;

	  if (write_status == IO_WAIT)
	    {
	      // Socket full, enable WRITE and pause processing
	      conn_set_events (conn,
			       IO_EVENT_READ | IO_EVENT_WRITE | IO_EVENT_ERR);
	      return;
	    }

	  // Check for "Soft Close" completion
	  if (unlikely
	      (conn->state == STATE_FLUSH_CLOSE && conn->pipeline_depth == 0))
	    {
	      conn->state = STATE_CLOSE;
	      return;
	    }
	}

      // Exit conditions
      if (input_consumed)
	break;
      if (conn_is_res_queue_full (conn))
	break;
    }

  // We still want to read, unless we are blocked on writing
  uint32_t events = IO_EVENT_READ | IO_EVENT_ERR;

  if (conn_has_pending_write (conn) ||
      (conn_has_unprocessed_data (conn) && conn_is_res_queue_full (conn)))
    {
      events |= IO_EVENT_WRITE;
    }

  conn_set_events (conn, events);
}

HOT IOStatus
response_queue_flush (Conn *conn)
{
  // Process ZC completions...
  while (zc_process_completions (conn))
    {
    }

  // BATCHED SENDING LOOP
  while (conn->pipeline_depth > 0)
    {
      // Check head slot for ZC block
      ResponseSlot *head = conn_get_head_slot (conn);
      if (conn_is_slot_fully_sent (head))
	{
	  if (head->is_zero_copy && head->pending_ops > 0)
	    return IO_OK;
	  conn_release_comp_slots (conn);
	  continue;
	}

      // CALL THE NEW BATCHER
      // This replaces the old 'transport_send_head_slot' loop
      IOStatus status = transport_write_batch (conn);

      if (unlikely (status == IO_ERROR))
	{
	  conn->state = STATE_CLOSE;
	  return IO_ERROR;
	}

      if (status == IO_WAIT)
	return IO_WAIT;

      // If IO_OK, we loop again to see if there is more to send
      // (or until pipeline_depth is 0)
    }

  return IO_OK;
}
