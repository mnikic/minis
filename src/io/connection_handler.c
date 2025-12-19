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
#include "connection.h"
#include "buffer.h"
#include "common/common.h"
#include "connection_handler.h"
#include "out.h"

static inline void
conn_set_epoll_events (int epfd, Conn *conn, uint32_t events)
{
  if (conn->state == STATE_CLOSE || conn->last_events == events)
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
      conn->state = STATE_CLOSE;
      return;
    }
  size_t err_payload_len = buf_len (&err_buf);
  uint32_t nwlen = htonl ((uint32_t) err_payload_len);
  memcpy (slot_mem, &nwlen, 4);

  conn->res_slots[w_idx].actual_length = 4 + (uint32_t) err_payload_len;
  conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;
  conn->state = STATE_FLUSH_CLOSE;
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
      return false;
    }

  return true;
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
      conn->state = STATE_CLOSE;
      return -2;
    }

  if (ret_val == 0)
    {
      DBG_LOGF ("FD %d: EOF received, setting state to STATE_CLOSE.",
		conn->fd);
      conn->state = STATE_CLOSE;
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
      size_t cap = K_RBUF_SIZE - conn->rbuf_size;
      if (cap == 0)
	break;

      // Perform a single read operation (which loops internally on EINTR)
      ssize_t bytes_read = read_data_from_socket (conn, cap);

      // Check for stopping conditions:
      if (bytes_read == -2)	// Fatal error or EOF
	return bytes_read_total;	// Return what we've accumulated so far

      if (bytes_read == -1)	// EAGAIN/EWOULDBLOCK
	break;			// Exit the loop cleanly

      // Accumulate the successful read:
      conn->rbuf_size += (uint32_t) bytes_read;
      bytes_read_total += (size_t) bytes_read;

      // Continue the loop to try another non-blocking read
    }

  return bytes_read_total;
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

  return true;
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
      err =
	send (conn->fd, data_start + slot->sent, remain,
	      MSG_NOSIGNAL | MSG_DONTWAIT);
    }

  if (err < 0)
    {
      if (errno == EAGAIN)
	{
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
	  return 0;		// Need EPOLLOUT
	}
      msgf ("send/sendmsg() error: %s", strerror (errno));
      conn->state = STATE_CLOSE;
      return -1;
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


static bool
try_one_request (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
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
      dump_error_and_close (epfd, conn, ERR_2BIG, "request too large");
      conn->read_offset = conn->rbuf_size;
      return false;
    }

  size_t total_req_len = 4 + len;
  if (total_req_len > bytes_remain)
    return false;		// Incomplete request

  // Prepare output buffer
  uint32_t w_idx = conn->write_idx;
  uint8_t *slot_mem = get_slot_data_ptr (conn, w_idx);
  Buffer out_buf = buf_init (slot_mem + 4, K_MAX_MSG - 4);

  // Execute request
  bool success = do_request (epfd, cache, conn,
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

static void
process_received_data (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  while (true)
    {
      if (is_res_queue_full (conn))
	break;

      if (!try_one_request (epfd, cache, now_us, conn))
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

static void
handle_in_event (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  DBG_LOGF ("FD %d: Handling EPOLLIN event", conn->fd);

  // Read data from socket
  size_t bytes_read = try_fill_buffer (conn);

  if (conn->state == STATE_CLOSE)
    return;			// EOF or error

  // Process requests if we have data
  if (conn->rbuf_size > conn->read_offset || bytes_read > 0)
    {
      process_received_data (epfd, cache, conn, now_us);
    }

  if (conn->state == STATE_CLOSE)
    return;

  // Update epoll interests
  uint32_t events = EPOLLIN | EPOLLERR;

  // If we have responses queued, request EPOLLOUT
  if (!is_slot_empty (&conn->res_slots[conn->read_idx]))
    {
      events |= EPOLLOUT;
    }

  conn_set_epoll_events (epfd, conn, events);
}

static void
handle_out_event (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  DBG_LOGF ("FD %d: Handling EPOLLOUT event", conn->fd);

  while (try_check_zerocopy_completion (conn))
    {
    }

  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];

      if (is_slot_empty (slot))
	break;

      int result = send_current_slot (epfd, conn, slot, conn->read_idx);
      if (result == 0)
	return;			// Need to wait (EAGAIN or ZC completion)

      if (result < 0)
	{
	  conn->state = STATE_CLOSE;
	  return;
	}
    }

  if (conn->state == STATE_FLUSH_CLOSE)
    {
      conn->state = STATE_CLOSE;
      return;
    }

  if (has_unprocessed_data (conn))
    {
      DBG_LOGF ("FD %d: Processing pipelined requests (%u bytes)",
		conn->fd, conn->rbuf_size - conn->read_offset);
      handle_in_event (epfd, cache, conn, now_us);
      return;
    }

  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
}

static void
handle_err_event (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  DBG_LOGF ("FD %d: Handling EPOLLERR event", conn->fd);

  // Process all pending zerocopy completions
  bool processed_any = false;
  while (try_check_zerocopy_completion (conn))
    {
      processed_any = true;
    }

  if (!processed_any)
    {
      DBG_LOGF ("FD %d: EPOLLERR but no zerocopy completions found",
		conn->fd);
      return;
    }

  ResponseSlot *slot = &conn->res_slots[conn->read_idx];

  // All responses done?
  if (is_slot_empty (slot))
    {
      if (has_unprocessed_data (conn))
	{
	  DBG_LOGF ("FD %d: Slots freed, processing blocked pipelined data",
		    conn->fd);
	  handle_in_event (epfd, cache, conn, now_us);
	  return;
	}
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
      return;
    }

  // Still waiting for ACKs on a fully sent slot?
  if (is_slot_fully_sent (slot) && slot->pending_ops > 0)
    {
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
      return;
    }

  // Otherwise, try to send more (either slot not fully sent, or fully sent+acked)
  handle_out_event (epfd, cache, conn, now_us);
}

void
handle_connection_io (int epfd, Cache *cache, Conn *conn, uint64_t now_us,
		      uint32_t events)
{
  if (events & (EPOLLHUP | EPOLLRDHUP))
    {
      conn->state = STATE_CLOSE;
    }

  // Error completions (always try to reclaim memory)
  if (conn->state != STATE_CLOSE && (events & EPOLLERR))
    {
      handle_err_event (epfd, cache, conn, now_us);
    }

  // Incoming data (only if healthy)
  if (conn->state == STATE_ACTIVE && (events & EPOLLIN))
    {
      handle_in_event (epfd, cache, conn, now_us);
    }

  // Outgoing data (if active OR finishing an error response)
  if (conn->state != STATE_CLOSE && (events & EPOLLOUT))
    {
      handle_out_event (epfd, cache, conn, now_us);
    }
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
