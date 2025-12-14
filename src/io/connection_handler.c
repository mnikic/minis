/*
 *============================================================================
 * Name             : event_state_machine.c
 * Author           : Milos
 * Description      : Core networking state machine - epoll event loop.
 *============================================================================
 */
#include <arpa/inet.h>
#include <error.h>
#include <errno.h>
#include <limits.h>
#include <linux/errqueue.h>
#include <linux/if_packet.h>
#include <linux/ipv6.h>
#include <linux/socket.h>
#include <linux/sockios.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <poll.h>
#include <sched.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <linux/rds.h>

#include <assert.h>
#include <stddef.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <time.h>
#include <sys/epoll.h>

#include "cache/cache.h"
#include "connections.h"
#include "buffer.h"
#include "common/common.h"
#include "connection_handler.h"
#include "out.h"

static inline uint8_t *
get_slot_data_ptr (Conn *conn, uint32_t slot_idx)
{
  return &conn->res_data[(size_t) (slot_idx * K_MAX_MSG)];
}

static inline bool
is_res_queue_full (Conn *conn)
{
  return ((conn->write_idx + 1) % K_SLOT_COUNT) == conn->read_idx;
}

// Check if a slot is fully sent (regardless of ACK status)
static inline bool
is_slot_fully_sent (ResponseSlot *slot)
{
  return slot->sent >= slot->actual_length;
}

// Check if a slot is completely done (sent AND acked for zerocopy)
static inline bool
is_slot_complete (ResponseSlot *slot)
{
  return slot->pending_ops == 0 &&
    slot->sent == slot->actual_length && slot->actual_length > 0;
}

// Check if a slot is empty/available
static inline bool
is_slot_empty (ResponseSlot *slot)
{
  return slot->actual_length == 0;
}

static inline void
conn_set_epoll_events (int epfd, Conn *conn, uint32_t events)
{
  if (conn->state == STATE_END || conn->last_events == events)
    return;

  struct epoll_event event;
  event.data.fd = conn->fd;
  event.events = events | EPOLLET;

  if (epoll_ctl (epfd, EPOLL_CTL_MOD, conn->fd, &event) == -1)
    {
      if (errno == ENOENT)
	return;
      msgf ("epoll ctl: MOD failed: %s", strerror (errno));
    }
  conn->last_events = events;
}

// Sends an error response and marks the connection to close after sending.
// WARNING: This discards any pending responses in the write buffers.
static void
dump_error_and_close (int epfd, Conn *conn, const int code, const char *str)
{
  DBG_LOGF ("FD %d: Sending error %d and closing: %s", conn->fd, code, str);

  // Use small stack buffer for error messages (errors should always be small)
  uint32_t w_idx = conn->write_idx;
  uint8_t *slot_mem = &conn->res_data[(size_t) (w_idx * K_MAX_MSG)];

  Buffer err_buf = buf_init (slot_mem + 4, K_MAX_MSG - 4);

  if (!out_err (&err_buf, code, str))
    {
      // If even the error won't fit, hard close.
      conn->state = STATE_END;
      return;
    }
  size_t err_payload_len = buf_len (&err_buf);
  uint32_t nwlen = htonl ((uint32_t) err_payload_len);
  memcpy (slot_mem, &nwlen, 4);

  conn->res_slots[w_idx].actual_length = 4 + (uint32_t) err_payload_len;
  conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;
  conn->state = STATE_RES_CLOSE;
  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT);
}

typedef enum
{
  VALIDATE_OK = 0,
  VALIDATE_TOO_SHORT,
  VALIDATE_TOO_MANY_ARGS,
  VALIDATE_TOO_FEW_ARGS
} ValidationResult;

// Validate the basic request structure
// Returns error code, or VALIDATE_OK on success
static ValidationResult
validate_request_header (const uint8_t *req, uint32_t reqlen,
			 uint32_t *out_arg_count)
{
  if (reqlen < 4)
    return VALIDATE_TOO_SHORT;

  uint32_t num = 0;
  memcpy (&num, &req[0], 4);
  num = ntohl (num);

  if (num > K_MAX_ARGS)
    return VALIDATE_TOO_MANY_ARGS;

  if (num < 1)
    return VALIDATE_TOO_FEW_ARGS;

  *out_arg_count = num;
  return VALIDATE_OK;
}

// State for in-place null-termination trick
typedef struct
{
  uint8_t *restore_ptrs[K_MAX_ARGS];
  uint8_t restore_chars[K_MAX_ARGS];
  size_t restore_count;
} RestoreState;

// Initialize restoration state
static inline void
restore_state_init (RestoreState *state)
{
  state->restore_count = 0;
}

// Temporarily null-terminate a string in the buffer and track for restoration
static inline void
save_and_nullterm (RestoreState *state, uint8_t *location,
		   uint8_t original_char)
{
  assert (state->restore_count < K_MAX_ARGS);
  state->restore_ptrs[state->restore_count] = location;
  state->restore_chars[state->restore_count] = original_char;
  state->restore_count++;
  *location = '\0';
}

// Restore all modified bytes
static inline void
restore_all_bytes (RestoreState *state)
{
  for (size_t i = 0; i < state->restore_count; i++)
    {
      *state->restore_ptrs[i] = state->restore_chars[i];
    }
}

typedef enum
{
  PARSE_OK = 0,
  PARSE_MISSING_LENGTH,
  PARSE_LENGTH_OVERFLOW,
  PARSE_TRAILING_DATA
} ParseResult;

// Parse arguments from request buffer into cmd array
// Uses in-place null-termination with automatic restoration tracking
static ParseResult
parse_arguments (uint8_t *req, uint32_t reqlen, uint32_t arg_count,
		 char **cmd, size_t *out_cmd_size, RestoreState *restore)
{
  size_t pos = 4;
  size_t cmd_size = 0;

  for (uint32_t i = 0; i < arg_count; i++)
    {
      // Check for length header
      if (pos + 4 > reqlen)
	return PARSE_MISSING_LENGTH;

      // Read argument length
      uint32_t arg_len = 0;
      memcpy (&arg_len, &req[pos], 4);
      arg_len = ntohl (arg_len);

      // Check if argument data fits
      if (pos + 4 + arg_len > reqlen)
	return PARSE_LENGTH_OVERFLOW;

      // Get pointer to null-termination location
      uint8_t *null_term_loc = &req[pos + 4 + arg_len];
      uint8_t original_char = *null_term_loc;

      // Save and null-terminate
      save_and_nullterm (restore, null_term_loc, original_char);

      // Store pointer to argument string
      cmd[cmd_size++] = (char *) &req[pos + 4];

      // Advance position
      pos += 4 + arg_len;
    }

  // Verify no trailing data
  if (pos != reqlen)
    return PARSE_TRAILING_DATA;

  *out_cmd_size = cmd_size;
  return PARSE_OK;
}

// Convert validation result to error message and send error response
static void
handle_validation_error (int epfd, Conn *conn, ValidationResult result)
{
  switch (result)
    {
    case VALIDATE_TOO_SHORT:
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Request too short for argument count");
      break;
    case VALIDATE_TOO_MANY_ARGS:
      dump_error_and_close (epfd, conn, ERR_MALFORMED, "Too many arguments.");
      break;
    case VALIDATE_TOO_FEW_ARGS:
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Must have at least one argument (the command)");
      break;
    case VALIDATE_OK:		// Not an error, nothing to do
      // Fallthrough
    default:
      break;
    }
}

// Convert parse result to error message and send error response
static inline void
handle_parse_error (int epfd, Conn *conn, ParseResult result)
{
  switch (result)
    {
    case PARSE_MISSING_LENGTH:
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Argument count mismatch: missing length header");
      break;
    case PARSE_LENGTH_OVERFLOW:
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Argument count mismatch: data length exceeds packet size");
      break;
    case PARSE_TRAILING_DATA:
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Trailing garbage in request");
      break;
    case PARSE_OK:		// Not an error, so nothing to do.
      // Fallsthrough
    default:
      break;
    }
}

// Returns true on success, false on failure.
// On failure, dump_error_and_close has already been called.
static bool
do_request (int epfd, Cache *cache, Conn *conn, uint8_t *req, uint32_t reqlen,
	    Buffer *out_buf, uint64_t now_us)
{
  uint32_t arg_count = 0;
  ValidationResult val_result =
    validate_request_header (req, reqlen, &arg_count);

  if (val_result != VALIDATE_OK)
    {
      handle_validation_error (epfd, conn, val_result);
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
      handle_parse_error (epfd, conn, parse_result);
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
      dump_error_and_close (epfd, conn, ERR_UNKNOWN, "response too large");
      conn->state = STATE_RES_CLOSE;
      return false;
    }

  return true;
}

static inline bool
try_one_request (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  if (is_res_queue_full (conn))
    {
      DBG_LOGF ("FD %d: Response queue full. Pausing request parsing.",
		conn->fd);
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
      return false;
    }

  size_t bytes_remain = conn->rbuf_size - conn->read_offset;

  // Check for length prefix (4 bytes)
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
      dump_error_and_close (epfd, conn, ERR_2BIG,
			    "request too large; connection closed.");
      conn->read_offset = conn->rbuf_size;	// Consume buffer
      return false;
    }

  size_t total_req_len = 4 + len;
  if (total_req_len > bytes_remain)
    return false;		// Incomplete request

  // Prepare output buffer in the current write slot
  uint32_t w_idx = conn->write_idx;
  uint8_t *slot_mem = get_slot_data_ptr (conn, w_idx);	// Using helper!
  Buffer out_buf = buf_init (slot_mem + 4, K_MAX_MSG - 4);

  // Execute the request
  bool success = do_request (epfd, cache, conn,
			     &conn->rbuf[conn->read_offset + 4],
			     len, &out_buf, now_us);

  if (!success)
    {
      msg ("request failed, stopping processing.");
      return false;
    }

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

  // Transition to STATE_RES if needed
  if (conn->state == STATE_REQ)
    {
      conn->state = STATE_RES;
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
    }

  DBG_LOGF ("FD %d: Request processed. Response mode: %s (len: %u)",
	    conn->fd, slot->is_zerocopy ? "ZEROCOPY" : "VANILLA",
	    slot->actual_length);

  return (conn->state == STATE_REQ || conn->state == STATE_RES);
}

static ssize_t
read_data_from_socket (Conn *conn, size_t cap)
{
  ssize_t ret_val;
  do
    {
      ret_val = read (conn->fd, &conn->rbuf[conn->rbuf_size], cap);
    }
  while (ret_val < 0 && errno == EINTR);

  if (ret_val < 0)
    {
      if (errno == EAGAIN)
	return -1;

      msgf ("read() error: %s", strerror (errno));
      conn->state = STATE_END;
      return -2;
    }

  if (ret_val == 0)
    {
      DBG_LOGF ("FD %d: EOF received, setting state to STATE_END.", conn->fd);
      conn->state = STATE_END;
      return -2;
    }

  DBG_LOGF ("FD %d: Read %zd bytes from socket.", conn->fd, ret_val);
  return ret_val;
}

static size_t
try_fill_buffer (Conn *conn)
{
  size_t bytes_read_total = 0;

  while (true)
    {
      // Calculate available capacity in the read buffer
      size_t cap = sizeof (conn->rbuf) - conn->rbuf_size;

      // Check if the buffer is full
      if (cap == 0)
	break;			// Stop reading if no space is available

      // Perform a single read operation (which loops internally on EINTR)
      ssize_t bytes_read = read_data_from_socket (conn, cap);

      // Check for stopping conditions:
      if (bytes_read == -2)	// Fatal error or EOF (conn->state is now STATE_END)
	return bytes_read_total;	// Return what we've accumulated so far

      if (bytes_read == -1)	// EAGAIN/EWOULDBLOCK (EPOLLET pattern dictates stopping here)
	break;			// Exit the loop cleanly

      // Accumulate the successful read:
      conn->rbuf_size += (uint32_t) bytes_read;
      bytes_read_total += (size_t) bytes_read;

      // Continue the loop to try another non-blocking read
    }

  return bytes_read_total;
}

static void
process_received_data (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  while (true)
    {
      if (is_res_queue_full (conn))
	{
	  break;
	}

      // Try to parse one request. Break if it fails (EOF, error, or not enough data).
      if (!try_one_request (epfd, cache, now_us, conn))
	{
	  break;
	}
      // If an error occurred during request execution, we stop processing.
      if (conn->state != STATE_REQ)
	{
	  break;
	}
    }

  if (conn->read_offset > 0 && conn->read_offset == conn->rbuf_size)
    {
      DBG_LOGF
	("FD %d: RBuf fully consumed (%zu bytes). Resetting buffer state.",
	 conn->fd, conn->rbuf_size);

      // Reset the buffer state to allow reading into the beginning again.
      conn->rbuf_size = 0;
      conn->read_offset = 0;
    }

  // Transition to STATE_RES if there are responses ready to send.
  if (conn->res_slots[conn->read_idx].actual_length > 0)
    {
      if (conn->state == STATE_REQ)
	{
	  conn->state = STATE_RES;
	  // Ensure EPOLLOUT is set now that we have responses
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
	}
    }
}

static void
state_req (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  DBG_LOGF ("FD %d: Entering STATE_REQ handler.", conn->fd);
  size_t bytes_read_total = try_fill_buffer (conn);
  if (conn->state == STATE_END)
    return;

  if (conn->rbuf_size > conn->read_offset || bytes_read_total > 0)
    {
      process_received_data (epfd, cache, conn, now_us);
    }

  DBG_LOGF ("FD %d: Exiting STATE_REQ handler. New state: %d.",
	    conn->fd, conn->state);
}

static uint32_t
process_one_zerocopy_notification (int file_desc)
{
  struct msghdr msg = { 0 };
  struct sock_extended_err *serr;
  struct cmsghdr *cms;
  char control[100];

  msg.msg_control = control;
  msg.msg_controllen = sizeof (control);

  int ret = (int) recvmsg (file_desc, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

  if (ret < 0)
    {
      if (errno == EAGAIN || errno == ENOMSG)
	return 0;

      msgf ("recvmsg(MSG_ERRQUEUE) error: %s", strerror (errno));
      return 0;
    }

  // Scan for zerocopy completion
  for (cms = CMSG_FIRSTHDR (&msg); cms; cms = CMSG_NXTHDR (&msg, cms))
    {
      if (cms->cmsg_level != SOL_IP && cms->cmsg_level != SOL_IPV6)
	continue;

      if (cms->cmsg_type != IP_RECVERR && cms->cmsg_type != IPV6_RECVERR)
	continue;

      serr = (void *) CMSG_DATA (cms);

      if (serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY)
	{
	  uint32_t range_start = serr->ee_info;
	  uint32_t range_end = serr->ee_data;
	  uint32_t completed_ops = range_end - range_start + 1;

	  DBG_LOGF ("FD %d: ZEROCOPY completion range [%u-%u] (%u ops).",
		    file_desc, range_start, range_end, completed_ops);

	  return completed_ops;
	}
    }

  return 0;			// Got something, but not a zerocopy completion
}

// Update slot's pending_ops count based on completions
// Returns: true if the slot became fully complete (sent AND acked)
// Corrected signature and body
static bool
apply_zerocopy_completion (int file_desc, uint32_t slot_idx,
			   ResponseSlot *slot, uint32_t completed_ops)
{
  (void) file_desc, (void) slot_idx;	// Need the for logging, but GCC complains!
  if (completed_ops == 0)
    return false;

  if (slot->pending_ops == 0)
    {
      DBG_LOGF
	("FD %d: WARNING: Completion received for %u ops but slot %u has 0 pending.",
	 file_desc, completed_ops, slot_idx);
      return false;
    }

  uint32_t ops_before = slot->pending_ops;
  (void) ops_before;		// make the compiler happy!
  if (completed_ops > slot->pending_ops)
    {
      DBG_LOGF
	("FD %d: WARNING: Completion overflow. Got %u ops, only %u pending. Setting pending to 0.",
	 file_desc, completed_ops, slot->pending_ops);
      slot->pending_ops = 0;
    }
  else
    {
      slot->pending_ops -= completed_ops;
    }

  // 4. Log the state change
  DBG_LOGF
    ("FD %d: Slot %u completion applied. Pending ops: %u -> %u. (%u ops acknowledged).",
     file_desc, slot_idx, ops_before, slot->pending_ops, completed_ops);

  // 5. Check for full completion
  // This uses the logic from the `is_slot_complete` helper you defined earlier.
  return (slot->pending_ops == 0 &&
	  slot->sent == slot->actual_length && slot->actual_length > 0);
}

// Release completed slots from the ring buffer
// Returns: number of slots released
static uint32_t
release_completed_slots (Conn *conn)
{
  uint32_t released_count = 0;

  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];

      // Check if this slot is fully complete
      if (!is_slot_complete (slot))	// Using helper!
	break;

      // Release this slot
      slot->actual_length = 0;
      slot->sent = 0;

      DBG_LOGF ("FD %d: Released slot %u from ring buffer.",
		conn->fd, conn->read_idx);

      conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
      released_count++;
    }

  return released_count;
}

// Reset read buffer to initial state (fully consumed)
static inline void
reset_read_buffer (Conn *conn)
{
  conn->rbuf_size = 0;
  conn->read_offset = 0;
}

// Check if read buffer is fully consumed
static inline bool
is_read_buffer_consumed (Conn *conn)
{
  return conn->read_offset > 0 && conn->read_offset == conn->rbuf_size;
}

// Check if there's unprocessed data in read buffer
static inline bool
has_unprocessed_data (Conn *conn)
{
  return conn->read_offset < conn->rbuf_size;
}

// Main zerocopy completion handler
//
// DESIGN NOTE: This function is called on EPOLLERR events. The kernel sends
// EPOLLERR when a zerocopy send operation completes and the buffer memory
// can be safely reused. This is SEPARATE from EPOLLOUT (which signals "send 
// buffer has space") and happens much later (after DMA to NIC completes).
//
// The completion may report a range of operations (ee_info to ee_data) if
// the kernel batched multiple notifications together.
static bool
try_check_zerocopy_completion (Conn *conn)
{
  ResponseSlot *current_slot = &conn->res_slots[conn->read_idx];

  if (!current_slot->is_zerocopy)
    return false;

  // Sanity check: is the slot even active?
  if (is_slot_empty (current_slot) && current_slot->pending_ops == 0)
    {
      DBG_LOGF ("FD %d: WARNING: EPOLLERR but slot %u is clear.",
		conn->fd, conn->read_idx);
    }

  // Try to get one completion notification
  uint32_t completed_ops = process_one_zerocopy_notification (conn->fd);

  if (completed_ops == 0)
    return false;		// No completion processed

  // Apply the completion to the current slot
  bool slot_complete =
    apply_zerocopy_completion (conn->fd, conn->read_idx, current_slot,
			       completed_ops);

  // If the slot is complete, try to release it and any following complete slots
  if (slot_complete)
    {
      release_completed_slots (conn);
    }

  return true;			// We processed a completion
}

// Check if we should handle pipelined requests
static void
handle_pipeline_transition (Conn *conn)
{
  if (conn->state == STATE_RES && conn->read_offset < conn->rbuf_size)
    {
      DBG_LOGF
	("FD %d: Cleared ACKs, %u bytes in RBuf. Switching to STATE_REQ for pipelining.",
	 conn->fd, conn->rbuf_size - conn->read_offset);
      conn->state = STATE_REQ;
    }
}

// Try to send data for the current slot
// Returns: 0 = need EPOLLOUT, 1 = success/progress, -1 = error
static int
send_current_slot (int epfd, Conn *conn, ResponseSlot *slot,
		   uint32_t slot_idx)
{
  // Check if slot is fully sent
  if (is_slot_fully_sent (slot))
    {				// Using helper!
      if (slot->is_zerocopy && slot->pending_ops > 0)
	{
	  // Waiting for zerocopy ACKs
	  DBG_LOGF
	    ("FD %d: Slot %u fully sent, waiting for %u ZC completions.",
	     conn->fd, slot_idx, slot->pending_ops);
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
	  return 0;		// Need to wait
	}

      // Slot is complete - release it
      DBG_LOGF ("FD %d: Slot %u completed (%s mode).",
		conn->fd, slot_idx,
		slot->is_zerocopy ? "ZEROCOPY" : "VANILLA");
      slot->actual_length = 0;
      slot->sent = 0;
      conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
      return 1;			// Made progress
    }

  // Send more data
  uint8_t *data_start = get_slot_data_ptr (conn, slot_idx);	// Using helper!
  size_t remain = slot->actual_length - slot->sent;
  ssize_t err;

  if (slot->is_zerocopy)
    {
      struct iovec iov = {
	.iov_base = data_start + slot->sent,
	.iov_len = remain
      };
      struct msghdr message = { 0 };
      message.msg_iov = &iov;
      message.msg_iovlen = 1;

      err =
	sendmsg (conn->fd, &message,
		 MSG_DONTWAIT | MSG_ZEROCOPY | MSG_NOSIGNAL);
    }
  else
    {
      err = send(conn->fd, data_start + slot->sent, remain, MSG_NOSIGNAL | MSG_DONTWAIT);
    }

  if (err < 0)
    {
      if (errno == EAGAIN)
	{
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
	  return 0;		// Need EPOLLOUT
	}
      msgf ("send/sendmsg() error: %s", strerror (errno));
      conn->state = STATE_END;
      return -1;		// Fatal error
    }

  // Update slot state
  slot->sent += (uint32_t) err;

  if (slot->is_zerocopy)
    {
      slot->pending_ops++;
      DBG_LOGF
	("FD %d: Sent %zd bytes on slot %u with ZEROCOPY (sent: %u/%u, pending: %u).",
	 conn->fd, err, slot_idx, slot->sent, slot->actual_length,
	 slot->pending_ops);

      // If fully sent, stop asking for EPOLLOUT
      if (is_slot_fully_sent (slot))
	{
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
	}
    }
  else
    {
      DBG_LOGF
	("FD %d: Sent %zd bytes on slot %u with VANILLA (sent: %u/%u).",
	 conn->fd, err, slot_idx, slot->sent, slot->actual_length);
    }

  return 1;			// Made progress
}

// Handle state transitions after all slots are processed
static void
handle_send_completion (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  if (conn->state == STATE_RES_CLOSE)
    {
      conn->state = STATE_END;
      return;
    }

  if (conn->state == STATE_RES)
    {
      conn->state = STATE_REQ;
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
      return;
    }

  if (conn->state == STATE_REQ)
    {
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
      state_req (epfd, cache, now_us, conn);
    }
}

static bool
try_flush_buffer (int epfd, Conn *conn, Cache *cache, uint64_t now_us)
{
  // Process all pending zerocopy completions
  while (try_check_zerocopy_completion (conn))
    {
    }

  handle_pipeline_transition (conn);

  // Send data for all pending slots
  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];

      if (slot->actual_length == 0)
	break;

      int result = send_current_slot (epfd, conn, slot, conn->read_idx);

      if (result <= 0)
	return false;
    }

  handle_send_completion (epfd, cache, conn, now_us);

  return !(conn->state == STATE_RES || conn->state == STATE_RES_CLOSE);
}

static void
try_final_opportunistic_read (int epfd, Cache *cache, Conn *conn,
			      uint64_t now_us)
{
  if (conn->state != STATE_REQ || conn->read_offset != conn->rbuf_size)
    return;			// Not at a clean state

  // One final non-blocking read
  size_t bytes_read = try_fill_buffer (conn);

  if (bytes_read > 0 || conn->rbuf_size > conn->read_offset)
    {
      DBG_LOGF ("FD %d: Opportunistic read caught %zu bytes.", conn->fd,
		bytes_read);
      process_received_data (epfd, cache, conn, now_us);
    }
}

static inline void
state_res (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  while (true)
    {
      bool finished = try_flush_buffer (epfd, conn, cache, now_us);

      if (conn->state == STATE_END || !finished)
	return;

      if (conn->state == STATE_RES_CLOSE)
	{
	  conn->state = STATE_END;
	  return;
	}

      // Handle pipelined requests
      if (has_unprocessed_data (conn))
	{			// Using helper!
	  DBG_LOGF ("FD %d: Pipeline data detected (%u bytes).",
		    conn->fd, conn->rbuf_size - conn->read_offset);

	  state_req (epfd, cache, now_us, conn);

	  if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE)
	    continue;		// Go back to flush new responses
	}

      // Try one final read before going back to epoll
      try_final_opportunistic_read (epfd, cache, conn, now_us);
      return;
    }
}

void
handle_connection_io (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  if (conn->state == STATE_REQ)
    state_req (epfd, cache, now_us, conn);

  if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE)
    state_res (epfd, cache, now_us, conn);
}

void
drain_zerocopy_errors (int file_des)
{
  struct msghdr msg = { 0 };
  char control[100];
  msg.msg_control = control;
  msg.msg_controllen = sizeof (control);

  // Loop until recvmsg returns -1 with EAGAIN or ENOMSG
  while (true)
    {
      DBG_LOGF ("FD %d: in loop to drain errors.", file_des);
      errno = 0;
      int ret = (int) recvmsg (file_des, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

      if (ret < 0)
	{
	  // EAGAIN or ENOMSG means the queue is empty. Success.
	  if (errno == EAGAIN || errno == ENOMSG)
	    {
	      return;
	    }
	  // Other error (e.g., EBADF if connection is already half-closed), stop.
	  DBG_LOGF
	    ("FD %d: recvmsg(MSG_ERRQUEUE) error during close drain: %s",
	     file_des, strerror (errno));
	  return;
	}
    }
}
