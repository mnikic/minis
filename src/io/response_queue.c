#include <stdint.h>

#include "io/response_queue.h"
#include "io/protocol_handler.h"
#include "io/connection.h"
#include "io/transport.h"
#include "io/proto_parser.h"
#include "common/macros.h"
#include "common/common.h"
#include "cache/cache.h"

#define K_MAX_REQ_PER_TICK 1024

// With pipelined requests in the buffer, the keys of the ones ahead are
// prefetched while the current one runs: the hash slot of the request
// K_PREFETCH_SLOT ahead, then, once that slot is in cache, the entry of
// the request K_PREFETCH_ENTRY ahead. Their cache misses overlap instead
// of each request waiting for its own. Each request is framed and its
// key hashed here once, and run with that frame and hash.
#define K_PREFETCH_SLOT 8
#define K_PREFETCH_ENTRY 4

typedef struct
{
  size_t offset;		// Where the next request to look at starts
  uint64_t next;		// Its number; requests are numbered as run
  ProtoMessageInfo frames[K_PREFETCH_SLOT];	// Of those looked at
  uint64_t hcodes[K_PREFETCH_SLOT];	// Their keys' hashes
} Lookahead;

// Looks ahead up to K_PREFETCH_SLOT requests past 'running', the number
// of the request about to run
static void
lookahead (Cache *cache, Conn *conn, Lookahead *la, uint64_t running)
{
  if (la->offset < conn->read_offset)
    {
      la->offset = conn->read_offset;
      la->next = running;
    }
  while (la->next < running + K_PREFETCH_SLOT)
    {
      // Framed in place: a copy of the frame, read back whole right after
      // it is written in parts, would wait for the writes to land
      size_t i = la->next % K_PREFETCH_SLOT;
      ProtoMessageInfo *m = &la->frames[i];
      proto_identify_message (&conn->rbuf[la->offset],
			      conn->rbuf_size - la->offset, m);
      if (m->status != PROTO_DECISION_OK)
	return;
      if (m->has_key)
	{
	  la->hcodes[i] =
	    str_hash (&conn->rbuf[la->offset + m->header_len + m->key_off],
		      m->key_len);
	  // The one about to run gains nothing from a prefetch
	  if (la->next != running)
	    cache_prefetch_slot (cache, la->hcodes[i]);
	}
      // The request whose slot was prefetched a while ago
      uint64_t earlier = la->next - (K_PREFETCH_SLOT - K_PREFETCH_ENTRY);
      if (la->next >= K_PREFETCH_SLOT - K_PREFETCH_ENTRY && earlier > running
	  && la->frames[earlier % K_PREFETCH_SLOT].has_key)
	cache_prefetch_entry (cache, la->hcodes[earlier % K_PREFETCH_SLOT]);
      la->offset += m->total_len;
      la->next++;
    }
}

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
  Lookahead la = {.offset = conn->read_offset };
  uint64_t running = 0;

  while (budget > 0)
    {
      if (unlikely (conn_is_res_queue_full (conn)))
	break;

      lookahead (cache, conn, &la, running);
      // Framed, unless it is incomplete or bad, which it frames itself
      bool framed = la.next > running;
      ctx.frame = framed ? &la.frames[running % K_PREFETCH_SLOT] : NULL;
      ctx.key_hcode = framed ? la.hcodes[running % K_PREFETCH_SLOT] : 0;
      if (!protocol_try_one_request (&ctx))
	break;
      running++;

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

HOT QueueStatus
response_queue_process_buffered_data (Cache *cache, Conn *conn,
				      uint64_t now_us)
{
  size_t prev_read_offset = conn->read_offset;

  // THE DATA PUMP: Process requests and send responses
  while (true)
    {
      if (unlikely (conn->state == STATE_CLOSE))
	return QUEUE_ERROR;

      // Returns true if we ran out of data or hit a partial command
      bool input_consumed =
	response_queue_process_input (cache, conn, now_us);

      if (conn->state == STATE_CLOSE)
	return QUEUE_ERROR;

      if (conn->pipeline_depth > 0)
	{
	  IOStatus write_status = response_queue_flush (conn);

	  if (unlikely (write_status == IO_ERROR))
	    return QUEUE_ERROR;

	  if (write_status == IO_WAIT)
	    {
	      // Socket is full. We are definitively stalled.
	      return QUEUE_STALLED;
	    }

	  // Error flushed, handle_connection_io takes it from here
	  if (unlikely
	      (conn->state == STATE_FLUSH_CLOSE && conn->pipeline_depth == 0))
	    return QUEUE_DONE;
	}

      if (conn->yield)
	return QUEUE_YIELD;

      if (conn_is_res_queue_full (conn))
	{
	  return QUEUE_STALLED;
	}

      // We processed everything available in the buffer. Break to check progress.
      if (input_consumed)
	{
	  break;
	}
    }

  // Did we move the read pointer at all during this call?
  if (conn->read_offset != prev_read_offset)
    {
      return QUEUE_PROGRESSED;	// Success! Loop again immediately.
    }

  return QUEUE_DONE;		// No work done (waiting for more data).
}

HOT IOStatus
response_queue_flush (Conn *conn)
{
  // BATCHED SENDING LOOP
  while (conn->pipeline_depth > 0)
    {
      ResponseSlot *head = conn_get_head_slot (conn);
      if (conn_is_slot_fully_sent (head))
	{
	  conn_release_comp_slots (conn);
	  continue;
	}

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
