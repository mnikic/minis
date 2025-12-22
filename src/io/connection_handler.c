/*
 *============================================================================
 * Name             : connection_handler.c 
 * Author           : Milos
 * Description      : Core networking state machine - epoll event loop.
 *============================================================================
 */
#include <stdbool.h>
#include <stdint.h>
#include <string.h>        // memcpy, strerror
#include <arpa/inet.h>     // htonl, ntohl

#include "connection_handler.h"
#include "connection.h"
#include "common/common.h"
#include "common/macros.h"
#include "cache/cache.h"
#include "buffer.h"
#include "out.h"
#include "transport.h"
#include "proto_parser.h"
#include "zerocopy.h"

// -----------------------------------------------------------------------------
// Forward Declarations
// -----------------------------------------------------------------------------
static void handle_in_event (Cache *cache, Conn *conn, uint64_t now_us);


// -----------------------------------------------------------------------------
// Error Handling
// -----------------------------------------------------------------------------

// Sends an error response and marks the connection to close after sending.
// WARNING: This discards any pending responses in the write buffers.
COLD static void
dump_error_and_close (Conn *conn, const int code, const char *str)
{
  DBG_LOGF ("FD %d: Sending error %d and closing: %s", conn->fd, code, str);

  // Use small stack buffer for error messages (errors should always be small)
  uint32_t w_idx = conn->write_idx;
  uint8_t *slot_mem = &conn->res_data[(size_t) (w_idx * K_MAX_MSG)];

  Buffer err_buf = buf_init (slot_mem + 4, K_MAX_MSG - 4);

  if (!out_err (&err_buf, code, str))
    {
      // If even the error won't fit, hard close.
      conn->state = STATE_CLOSE;
      return;
    }
  size_t err_payload_len = buf_len (&err_buf);
  uint32_t nwlen = htonl ((uint32_t) err_payload_len);
  memcpy (slot_mem, &nwlen, 4);

  conn->res_slots[w_idx].actual_length = 4 + (uint32_t) err_payload_len;
  conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;
  
  // Mark as flushing so we close after write
  conn->state = STATE_FLUSH_CLOSE;
  
  // We force WRITE interest here because we definitely have data
  conn_set_events (conn, IO_EVENT_READ | IO_EVENT_WRITE);
}

// Convert validation result to error message and send error response
COLD static void
handle_validation_error (Conn *conn, ValidationResult result)
{
  const char *err_msg = NULL;
  switch (result)
    {
    case VALIDATE_TOO_SHORT:
      err_msg = "Request too short for argument count";
      break;
    case VALIDATE_TOO_MANY_ARGS:
      err_msg = "Too many arguments.";
      break;
    case VALIDATE_TOO_FEW_ARGS:
      err_msg = "Must have at least one argument (the command)";
      break;
    case VALIDATE_OK:
    default:
      return;
    }
  dump_error_and_close (conn, ERR_MALFORMED, err_msg);
}

// Convert parse result to error message and send error response
COLD static ALWAYS_INLINE void
handle_parse_error (Conn *conn, ParseResult result)
{
  const char *err_msg = NULL;
  switch (result)
    {
    case PARSE_MISSING_LENGTH:
      err_msg = "Argument count mismatch: missing length header";
      break;
    case PARSE_LENGTH_OVERFLOW:
      err_msg = "Argument count mismatch: data length exceeds packet size";
      break;
    case PARSE_TRAILING_DATA:
      err_msg = "Trailing garbage in request";
      break;
    case PARSE_OK:
    default:
      return;
    }
  dump_error_and_close (conn, ERR_MALFORMED, err_msg);
}


// -----------------------------------------------------------------------------
// Request Processing
// -----------------------------------------------------------------------------

// Returns true on success, false on failure.
// On failure, dump_error_and_close has already been called.
static bool
do_request (Cache *cache, Conn *conn, uint8_t *req, uint32_t reqlen,
        Buffer *out_buf, uint64_t now_us)
{
  uint32_t arg_count = 0;
  ValidationResult val_result =
    validate_request_header (req, reqlen, &arg_count);

  if (val_result != VALIDATE_OK)
    {
      handle_validation_error (conn, val_result);
      return false;
    }

  // Parse arguments with in-place null-termination
  char *cmd[K_MAX_ARGS];
  size_t cmd_size = 0;
  RestoreState restore;
  restore_state_init (&restore);

  ParseResult parse_result = parse_arguments (req, reqlen, arg_count,
                          cmd, &cmd_size, &restore);

  if (parse_result != PARSE_OK)
    {
      restore_all_bytes (&restore);
      handle_parse_error (conn, parse_result);
      return false;
    }

  DBG_LOGF ("FD %d: Executing command with %zu arguments", conn->fd,
        cmd_size);

  bool success = TIME_EXPR ("cache_execute",
                 cache_execute (cache, cmd, cmd_size, out_buf,
                        now_us));
  restore_all_bytes (&restore);
  if (!success)
    {
      msg ("cache couldn't write message, no space.");
      dump_error_and_close (conn, ERR_UNKNOWN, "response too large");
      return false;
    }

  return true;
}

HOT static bool
try_one_request (Cache *cache, uint64_t now_us, Conn *conn)
{
  if (is_res_queue_full (conn))
    {
      DBG_LOGF ("FD %d: Response queue full, pausing parsing", conn->fd);
      return false;
    }

  size_t bytes_remain = conn->rbuf_size - conn->read_offset;
  if (bytes_remain < 4)
    return false;

  // Read request length
  uint32_t len = 0;
  memcpy (&len, &conn->rbuf[conn->read_offset], 4);
  len = ntohl (len);

  DBG_LOGF ("FD %d: Parsing request at offset %u with length %u.",
        conn->fd, conn->read_offset, len);

  // Validate request size
  if (len > K_MAX_MSG)
    {
      msgf ("request too long %u", len);
      dump_error_and_close (conn, ERR_2BIG, "request too large");
      conn->read_offset = conn->rbuf_size;
      return false;
    }

  size_t total_req_len = 4 + len;
  if (total_req_len > bytes_remain)
    return false;        // Incomplete request

  // Prepare output buffer
  uint32_t w_idx = conn->write_idx;
  uint8_t *slot_mem = get_slot_data_ptr (conn, w_idx);
  Buffer out_buf = buf_init (slot_mem + 4, K_MAX_MSG - 4);

  // Execute request
  bool success = do_request (cache, conn,
                 &conn->rbuf[conn->read_offset + 4],
                 len, &out_buf, now_us);

  if (!success)
    return false;

  // Setup response slot
  size_t reslen = buf_len (&out_buf);
  uint32_t nwlen = htonl ((uint32_t) reslen);
  memcpy (slot_mem, &nwlen, 4);

  ResponseSlot *slot = &conn->res_slots[w_idx];
  slot->actual_length = 4 + (uint32_t) reslen;
  slot->sent = 0;
  slot->pending_ops = 0;
  slot->is_zerocopy = (reslen > (size_t) K_ZEROCPY_THEASHOLD);

  conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;
  conn->read_offset += total_req_len;

  DBG_LOGF ("FD %d: Request processed. Response mode: %s (len: %u)",
        conn->fd, slot->is_zerocopy ? "ZEROCOPY" : "VANILLA",
        slot->actual_length);

  return true;
}

HOT static void
process_received_data (Cache *cache, Conn *conn, uint64_t now_us)
{
  while (true)
    {
      if (is_res_queue_full (conn))
        break;

      if (!try_one_request (cache, now_us, conn))
        break;

      if (conn->state == STATE_CLOSE)
        break;
    }

  // Reset buffer if fully consumed
  if (is_read_buffer_consumed (conn))
    {
      DBG_LOGF ("FD %d: RBuf fully consumed (%zu bytes). Resetting.",
        conn->fd, conn->rbuf_size);
      reset_read_buffer (conn);
    }
}


// -----------------------------------------------------------------------------
// Write Logic (Optimistic)
// -----------------------------------------------------------------------------

// Tries to empty the write queue. 
// Returns IO_OK if everything was sent.
// Returns IO_WAIT if socket blocked.
// Returns IO_ERROR if connection died.
HOT static IOStatus
flush_write_queue (Conn *conn)
{
  // 1. Process any pending ZeroCopy completions first
  while (zc_process_completions (conn))
    {
    }

  // 2. Loop through pending slots
  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];
      
      if (is_slot_empty (slot))
        return IO_OK; // Queue is empty!

      // Check if we are just waiting for ZC ACKs
      if (is_slot_fully_sent (slot))
        {
          if (slot->is_zerocopy && slot->pending_ops > 0)
            {
              // Blocked on kernel ACKs, not socket buffer space.
              // We return OK here because we can't write anymore, 
              // but the caller must handle the event registration (needs ERR).
              return IO_OK; 
            }
          
          // Slot done, rotate and continue
          release_completed_slots (conn);
          continue;
        }

      // Try to send data
      IOStatus status = transport_send_slot (conn, conn->read_idx);

      if (status == IO_ERROR)
        {
          conn->state = STATE_CLOSE;
          return IO_ERROR;
        }

      if (status == IO_WAIT)
        {
          // Socket is full. Stop here.
          return IO_WAIT;
        }
    }
}

// -----------------------------------------------------------------------------
// Event Handlers
// -----------------------------------------------------------------------------

static void
handle_in_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  DBG_LOGF ("FD %d: Handling IO_EVENT_READ event", conn->fd);
  IOStatus status = transport_read_buffer (conn);

  if (status == IO_ERROR || status == IO_EOF)
    {
      conn->state = STATE_CLOSE;
      return;
    }

  // 1. Process data if we have it
  if (conn->rbuf_size > conn->read_offset)
    {
      process_received_data (cache, conn, now_us);
    }

  if (conn->state == STATE_CLOSE)
    return;

  // 2. OPTIMISTIC WRITE
  if (!is_slot_empty (&conn->res_slots[conn->read_idx]))
    {
      flush_write_queue(conn);
      
      // FIX START: Check for FLUSH_CLOSE after optimistic write
      if (conn->state == STATE_CLOSE) return;

      if (conn->state == STATE_FLUSH_CLOSE)
        {
          // If we successfully flushed everything, close now!
          if (is_slot_empty(&conn->res_slots[conn->read_idx])) {
              conn->state = STATE_CLOSE;
              return;
          }
        }
      // FIX END
    }

  // 3. Determine Events
  IOEvent events = IO_EVENT_READ | IO_EVENT_ERR;
  ResponseSlot *head = &conn->res_slots[conn->read_idx];

  if (!is_slot_empty (head))
    {
      if (head->is_zerocopy && is_slot_fully_sent(head) && head->pending_ops > 0)
        {
           // Waiting for ACKs
        }
      else
        {
           // Waiting for Socket Buffer OR Partial Flush-Close
           events |= IO_EVENT_WRITE;
        }
    }
  // If we are FLUSH_CLOSE but failed to flush above (socket full), 
  // the logic above correctly adds IO_EVENT_WRITE, so we will come back later.

  conn_set_events (conn, events);
}

static void
handle_out_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  DBG_LOGF ("FD %d: Handling IO_EVENT_WRITE event", conn->fd);

  // 1. Flush the queue
  IOStatus status = flush_write_queue(conn);

  if (conn->state == STATE_CLOSE) return;

  // 2. Handle Flush-Close State
  if (conn->state == STATE_FLUSH_CLOSE)
    {
      // If queue is empty, we are done. Close it.
      if (is_slot_empty(&conn->res_slots[conn->read_idx])) {
          conn->state = STATE_CLOSE;
          return;
      }
    }

  // 3. Pipelining: If we cleared space, maybe we can process more requests?
  if (status == IO_OK && has_unprocessed_data (conn))
    {
      DBG_LOGF ("FD %d: Processing pipelined requests (%u bytes)",
        conn->fd, conn->rbuf_size - conn->read_offset);
      // We recurse into IN event. 
      // Note: handle_in_event will do its own event setting.
      handle_in_event (cache, conn, now_us);
      return;
    }

  // 4. Determine Events (Same logic as handle_in_event)
  IOEvent events = IO_EVENT_READ | IO_EVENT_ERR;
  ResponseSlot *head = &conn->res_slots[conn->read_idx];

  if (!is_slot_empty (head))
    {
      if (head->is_zerocopy && is_slot_fully_sent(head) && head->pending_ops > 0)
        {
           // Waiting for ACKs
        }
      else
        {
           // Waiting for Socket Buffer OR Partial Flush-Close
           events |= IO_EVENT_WRITE;
        }
    }

  conn_set_events (conn, events);
}

static void
handle_err_event (Cache *cache, Conn *conn, uint64_t now_us)
{
  // "Hey ZC module, do your thing."
  bool progress = false;
  while (zc_process_completions (conn))
    {
      progress = true;
    }

  // If we freed up space, maybe we can process more requests?
  if (progress && has_unprocessed_data (conn))
    {
      handle_in_event (cache, conn, now_us);
    }
}

HOT void
handle_connection_io (Cache *cache, Conn *conn, uint64_t now_us,
              IOEvent events)
{
  if (events & (IO_EVENT_HUP | IO_EVENT_RDHUP))
    {
      conn->state = STATE_CLOSE;
    }

  // Error completions (always try to reclaim memory)
  if (conn->state != STATE_CLOSE && (events & IO_EVENT_ERR))
    {
      handle_err_event (cache, conn, now_us);
    }

  // Incoming data (only if healthy)
  if (conn->state == STATE_ACTIVE && (events & IO_EVENT_READ))
    {
      handle_in_event (cache, conn, now_us);
    }

  // Outgoing data (if active OR finishing an error response)
  if (conn->state != STATE_CLOSE && (events & IO_EVENT_WRITE))
    {
      handle_out_event (cache, conn, now_us);
    }
}
