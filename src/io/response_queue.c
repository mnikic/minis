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
// prefetched while the current one runs, a level at a time: the hash
// slot of a request as it is framed, its entry K_STEP requests later,
// once the slot is in cache, and for a field of a hash, the hash, the
// field's slot and the field's entry each K_STEP later again. So their
// cache misses overlap instead of each request waiting for its own. Each
// request is framed and its key hashed here once, and run with that
// frame and hash.
#define K_STEP 2
#define K_AHEAD (4 * K_STEP + 2)	// The last stage, one request ahead
#define K_RING 32
_Static_assert (K_AHEAD <= K_RING, "the ring holds those looked at");

typedef struct
{
  size_t offset;		// Where the next request to look at starts
  uint64_t next;		// Its number; requests are numbered as run
  // Of those looked at: their frames, where they start, the hashes of
  // their keys and, for a field of a hash, of the field
  ProtoMessageInfo frames[K_RING];
  size_t offsets[K_RING];
  uint64_t hcodes[K_RING];
  uint64_t fhcodes[K_RING];
  bool is_hash[K_RING];
} Lookahead;

// The stage of the request framed AGE requests before the newest one
static void
prefetch_stage (Cache *cache, Conn *conn, Lookahead *la, uint64_t running,
		unsigned age)
{
  if (la->next < age || la->next - age <= running)
    return;
  size_t i = (la->next - age) % K_RING;
  const ProtoMessageInfo *m = &la->frames[i];
  if (!m->has_key)
    return;
  switch (age / K_STEP)
    {
    case 1:
      cache_prefetch_entry (cache, la->hcodes[i]);
      break;
    case 2:
      la->is_hash[i] = m->has_field
	&& cache_prefetch_hash (cache, la->hcodes[i]);
      if (la->is_hash[i])
	la->fhcodes[i] =
	  str_hash (&conn->rbuf[la->offsets[i] + m->header_len + m->field_off],
		    m->field_len);
      break;
    case 3:
      if (la->is_hash[i])
	cache_prefetch_field_slot (cache, la->hcodes[i], la->fhcodes[i]);
      break;
    case 4:
      if (la->is_hash[i])
	cache_prefetch_field_entry (cache, la->hcodes[i], la->fhcodes[i]);
      break;
    }
}

// Looks ahead up to K_AHEAD requests past 'running', the number of the
// request about to run
static void
lookahead (Cache *cache, Conn *conn, Lookahead *la, uint64_t running)
{
  if (la->offset < conn->read_offset)
    {
      la->offset = conn->read_offset;
      la->next = running;
    }
  while (la->next < running + K_AHEAD)
    {
      // Framed in place: a copy of the frame, read back whole right after
      // it is written in parts, would wait for the writes to land
      size_t i = la->next % K_RING;
      ProtoMessageInfo *m = &la->frames[i];
      proto_identify_message (&conn->rbuf[la->offset],
			      conn->rbuf_size - la->offset, m);
      if (m->status != PROTO_DECISION_OK)
	return;
      la->offsets[i] = la->offset;
      la->is_hash[i] = false;
      if (m->has_key)
	{
	  la->hcodes[i] =
	    str_hash (&conn->rbuf[la->offset + m->header_len + m->key_off],
		      m->key_len);
	  // The one about to run gains nothing from a prefetch
	  if (la->next != running)
	    cache_prefetch_slot (cache, la->hcodes[i]);
	}
      for (unsigned age = K_STEP; age <= 4 * K_STEP; age += K_STEP)
	prefetch_stage (cache, conn, la, running, age);
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
  // The rest is only read for requests looked at, which set it
  Lookahead la;
  la.offset = conn->read_offset;
  la.next = 0;
  uint64_t running = 0;

  while (budget > 0)
    {
      if (unlikely (conn_is_res_queue_full (conn)))
	break;

      lookahead (cache, conn, &la, running);
      // Framed, unless it is incomplete or bad, which it frames itself
      bool framed = la.next > running;
      ctx.frame = framed ? &la.frames[running % K_RING] : NULL;
      ctx.key_hcode = framed ? la.hcodes[running % K_RING] : 0;
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
