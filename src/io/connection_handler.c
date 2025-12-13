/*
 *============================================================================
 * Name             : event_state_machine.c
 * Author           : Milos
 * Description      : Core networking state machine - epoll event loop.
 *============================================================================
 */

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <stdbool.h>
#include <time.h>
#include <netinet/ip.h>
#include <sys/epoll.h>

#include "cache/cache.h"
#include "connections.h"
#include "buffer.h"
#include "common/common.h"
#include "connection_handler.h"
#include "out.h"

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

static inline bool
is_res_queue_full (Conn *conn)
{
  return ((conn->write_idx + 1) % K_SLOT_COUNT) == conn->read_idx;
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

  conn->res_slots[w_idx].actual_length = 4 + err_payload_len;
  conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;
  conn->state = STATE_RES_CLOSE;
  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT);
}

// Returns true on success, false on failure.
// On failure, dump_error_and_close has already been called.
static bool
do_request (int epfd, Cache *cache, Conn *conn, uint8_t *req, uint32_t reqlen,
	    Buffer *out_buf, uint64_t now_us)
{
  if (reqlen < 4)
    {
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Request too short for argument count");
      return false;
    }

  uint32_t num = 0;
  memcpy (&num, &req[0], 4);
  num = ntohl (num);

  if (num > K_MAX_ARGS)
    {
      DBG_LOG ("too many arguments");
      dump_error_and_close (epfd, conn, ERR_MALFORMED, "Too many arguments.");
      return false;
    }

  if (num < 1)
    {
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Must have at least one argument (the command)");
      return false;
    }

  // NON-COPY/IN-PLACE PARSING
  //
  // We will mess up the header of a next string temporarily because the next stage (cache)
  // doesn't care about headers (it only cares about the actual strings).
  // We will restore the messed up bytes at the end to avoid messing up buffer compaction etc.

  // Allocate the array of pointers on the heap (small, size 'num'), but NOT the strings.
  // The strings will be temporarily null-terminated inside the 'req' buffer.
  char *cmd[K_MAX_ARGS];
  // Stack arrays to store pointers and original characters for restoration
  uint8_t *restore_ptrs[K_MAX_ARGS];
  uint8_t restore_chars[K_MAX_ARGS];
  size_t restore_count = 0;

  size_t cmd_size = 0;
  bool success = false;
  size_t pos = 4;

  while (num--)
    {
      if (pos + 4 > reqlen)
	{
	  dump_error_and_close (epfd, conn, ERR_MALFORMED,
				"Argument count mismatch: missing length header");
	  goto CLEANUP;
	}

      uint32_t siz = 0;
      memcpy (&siz, &req[pos], 4);
      siz = ntohl (siz);

      if (pos + 4 + siz > reqlen)
	{
	  dump_error_and_close (epfd, conn, ERR_MALFORMED,
				"Argument count mismatch: data length exceeds packet size");
	  goto CLEANUP;
	}

      if (restore_count >= K_MAX_ARGS)
	{
	  dump_error_and_close (epfd, conn, ERR_MALFORMED,
				"Too many arguments detected.");
	  goto CLEANUP;
	}

      // Get the address of the byte immediately following the argument data.
      uint8_t *null_term_loc = &req[pos + 4 + siz];

      // Save the pointer and original character for restoration.
      restore_ptrs[restore_count] = null_term_loc;
      restore_chars[restore_count] = *null_term_loc;
      restore_count++;

      // Temporarily null-terminate the string in the read buffer.
      *null_term_loc = '\0';

      // Store the pointer to the argument data.
      cmd[cmd_size] = (char *) &req[pos + 4];

      cmd_size++;
      pos += 4 + siz;
    }

  if (pos != reqlen)
    {
      dump_error_and_close (epfd, conn, ERR_MALFORMED,
			    "Trailing garbage in request");
      goto CLEANUP;
    }

  DBG_LOGF ("FD %d: Executing command with %zu arguments", conn->fd,
	    cmd_size);
  success =
    TIME_EXPR ("cache_execute",
	       cache_execute (cache, cmd, cmd_size, out_buf, now_us));
  if (!success)
    {
      msg ("cache couldn't write message, no space.");
      dump_error_and_close (epfd, conn, ERR_UNKNOWN, "response too large");
      conn->state = STATE_RES_CLOSE;
      goto CLEANUP;
    }

  success = true;

CLEANUP:
  for (size_t i = 0; i < restore_count; i++)
    {
      // Restore the original character at the location we overwrote with '\0'
      *restore_ptrs[i] = restore_chars[i];
    }

  return success;
}

// Executes the request and buffers the response directly into output buffers 
static bool
execute_and_buffer_response (int epfd, Cache *cache, uint64_t now_us,
			     Conn *conn, uint8_t *req_data, uint32_t req_len)
{
  // Identify current slot memory location
  uint32_t w_idx = conn->write_idx;
  uint8_t *slot_mem = &conn->res_data[(size_t) (w_idx * K_MAX_MSG)];

  // Reserve 4 bytes for the header
  Buffer out_buf = buf_init (slot_mem + 4, K_MAX_MSG - 4);
  if (!TIME_EXPR ("do_request",
		  do_request (epfd, cache, conn, req_data, req_len,
			      &out_buf, now_us)))
    // do_request failed and cleaned up 
    return false;

  // Backfill that header.
  size_t payload_len = buf_len (&out_buf);
  uint32_t nwlen = htonl ((uint32_t) payload_len);
  memcpy (slot_mem, &nwlen, 4);

  // House keeping. 
  conn->res_slots[w_idx].actual_length = 4 + payload_len;
  conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;

  DBG_LOGF ("FD %d: Response (len %zu) written to slot %u. write_idx now %u.",
	    conn->fd, 4 + payload_len, w_idx, conn->write_idx);

  return true;
}

static bool
try_one_request (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  if (is_res_queue_full (conn))
    {
      DBG_LOGF ("FD %d: Response queue full. Pausing request parsing.",
		conn->fd);
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT);
      return false;
    }

  size_t bytes_remain = conn->rbuf_size - conn->read_offset;
  if (bytes_remain < 4)
    return false;

  uint32_t len = 0;
  memcpy (&len, &conn->rbuf[conn->read_offset], 4);
  len = ntohl (len);

  if (len > K_MAX_MSG)
    {
      msgf ("request too long %u", len);
      dump_error_and_close (epfd, conn, ERR_2BIG, "request too large");
      conn->read_offset = conn->rbuf_size;	// Discard all data
      return false;
    }

  size_t total_req_len = 4 + len;
  if (total_req_len > bytes_remain)
    return false;

  bool success = execute_and_buffer_response (epfd, cache, now_us, conn,
					      &conn->rbuf[conn->read_offset +
							  4],
					      len);
  conn->read_offset += total_req_len;
  return success;
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

static void
process_received_data (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  while (true)
    {
      if (is_res_queue_full (conn))
	{
	  break;		// Backpressure: Stop parsing if no space for response.
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
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT);
	}
    }
}

static bool
try_fill_buffer (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  while (true)
    {
      size_t cap = sizeof (conn->rbuf) - conn->rbuf_size;
      if (cap == 0)
	{
	  if (conn->read_offset == conn->rbuf_size)
	    {
	      process_received_data (epfd, cache, conn, now_us);
	      continue;
	    }
	  DBG_LOGF ("FD %d: RBuf full, transitioning to STATE_RES.",
		    conn->fd);
	  conn->state = STATE_RES;
	  break;
	}

      ssize_t bytes_read = read_data_from_socket (conn, cap);
      if (bytes_read == -2)
	return true;
      if (bytes_read == -1)
	return false;

      conn->rbuf_size += (uint32_t) bytes_read;
      process_received_data (epfd, cache, conn, now_us);
      if (conn->state != STATE_REQ)
	break;
    }
  return true;
}

static bool
try_flush_buffer (int epfd, Conn *conn)
{
  if (conn->res_slots[conn->read_idx].actual_length == 0)
    {
      return true;
    }

  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];
      uint8_t *data_start =
	&conn->res_data[(size_t) (conn->read_idx * K_MAX_MSG)];

      size_t total_to_send = slot->actual_length;
      size_t remain = total_to_send - conn->res_sent;

      ssize_t ret_val =
	send (conn->fd, &data_start[conn->res_sent], remain, MSG_NOSIGNAL);

      if (ret_val < 0)
	{
	  if (errno == EINTR)
	    continue;
	  if (errno == EAGAIN)
	    {
	      // Socket is full. Register for EPOLLOUT and stop looping.
	      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT);
	      return false;
	    }
	  conn->state = STATE_END;
	  return false;
	}

      conn->res_sent += (size_t) ret_val;

      if (conn->res_sent == total_to_send)
	{
	  slot->actual_length = 0;
	  conn->res_sent = 0;
	  conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;

	  // If the next slot is empty, we are fully caught up.
	  if (conn->res_slots[conn->read_idx].actual_length == 0)
	    {
	      return true;
	    }
	}
      else
	{
	  // Partial write: the kernel accepted some data but not all.
	  // This is effectively the same as EAGAIN for our loop.
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT);
	  return false;
	}
    }
}

static void
state_res (int epfd, Conn *conn)
{
  while (true)
    {
      // We call the flush. It returns true only if the buffer is now empty.
      bool finished =
	TIME_EXPR ("try_flush_buffer", try_flush_buffer (epfd, conn));

      // If we hit a terminal state (END), stop.
      if (conn->state == STATE_END || !finished)
	{
	  return;
	}

      // If we finished flushing everything, handle transitions.
      if (conn->state == STATE_RES_CLOSE)
	{
	  conn->state = STATE_END;
	  return;
	}
      if (conn->state == STATE_RES)
	{
	  conn->state = STATE_REQ;
	  conn_set_epoll_events (epfd, conn, EPOLLIN);
	  return;
	}

      // If we are still in STATE_RES but finished is true, it might be 
      // because we just cleared one slot and more are coming, but 
      // usually the internal while(true) in try_flush_buffer handles that.
      return;
    }
}

static void
state_req (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  DBG_LOGF ("FD %d: Entering STATE_REQ handler.", conn->fd);
  while (try_fill_buffer (epfd, cache, now_us, conn))
    {
      if (conn->state != STATE_REQ)
	break;
    }
  DBG_LOGF ("FD %d: Exiting STATE_REQ handler. New state: %d.",
	    conn->fd, conn->state);
}

void
handle_connection_io (int epfd, Cache *cache, Conn *conn, uint64_t now_us)
{
  if (conn->state == STATE_REQ)
    state_req (epfd, cache, now_us, conn);

  if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE)
    state_res (epfd, conn);
}
