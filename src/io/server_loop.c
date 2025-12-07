#define _GNU_SOURCE
/*
 *============================================================================
 * Name          : server_loop.c
 * Author        : Milos
 * Description   : Core networking logic (epoll event loop, connection handling).
 *============================================================================
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <stdbool.h>
#include <time.h>
#include <netinet/ip.h>
#include <sys/epoll.h>
#include <signal.h>

#include "cache/cache.h"
#include "connections.h"
#include "buffer.h"
#include "common/common.h"
#include "out.h"
#include "server_loop.h"
#include "list.h"

#define MAX_EVENTS 10000
#define MAX_CHUNKS 16
#define K_IDLE_TIMEOUT_US 5000000

enum
{
  RES_OK = 0, RES_ERR = 1, RES_NX = 2,
};

enum
{
  STATE_REQ = 0, STATE_RES = 1, STATE_RES_CLOSE = 2, STATE_END = 3,	// mark the connection for deletion
};

static struct
{
  // a map of all client connections, keyed by fd
  ConnPool *fd2conn;
  // timers for idle connections
  DList idle_list;
  int epfd;
  // FLAG: Set by signal handler to exit the main loop gracefully
  volatile sig_atomic_t terminate_flag;
} g_data;

// Signal handler function
static void
sigint_handler (int sig)
{
  (void) sig;
  g_data.terminate_flag = 1;
}

// Sets up handlers for graceful server termination signals.
static void
setup_signal_handlers (void)
{
  struct sigaction sig_action = { 0 };
  sig_action.sa_handler = sigint_handler;
  sig_action.sa_flags = 0;
  sigaction (SIGINT, &sig_action, NULL);
  sigaction (SIGTERM, &sig_action, NULL);
  sigaction (SIGQUIT, &sig_action, NULL);
}

static void
fd_set_nb (int file_des)
{
  errno = 0;
  int flags = fcntl (file_des, F_GETFL, 0);
  if (errno)
    {
      die ("fcntl error");
    }

  flags |= O_NONBLOCK;

  errno = 0;
  (void) fcntl (file_des, F_SETFL, flags);
  if (errno)
    {
      die ("fcntl error");
    }
}

static int32_t
accept_new_conn (int file_des)
{
  struct sockaddr_in client_addr = { 0 };
  socklen_t socklen = sizeof (client_addr);
  int connfd;

  // Optimization: use accept4 with SOCK_NONBLOCK to set non-blocking atomically
  // Fallback to accept + fcntl if accept4 is not available.
#if defined(__linux__) && defined(__GLIBC__) && (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 10)
  connfd = accept4 (file_des, (struct sockaddr *) &client_addr, &socklen,
		    SOCK_NONBLOCK);
#else
  connfd = accept (file_des, (struct sockaddr *) &client_addr, &socklen);
  if (connfd >= 0)
    {
      fd_set_nb (connfd);
    }
#endif

  if (connfd < 0)
    {
      if (errno == EAGAIN)
	{
	  DBG_LOG ("No more connections to accept (EAGAIN).");
	  return -1;
	}
      msgf ("accept() error: %s", strerror (errno));
      return -2;		// Distinguish between no connection (-1) and critical error (-2)
    }

  Conn *conn = (Conn *) malloc (sizeof (Conn));
  if (!conn)
    {
      close (connfd);
      die ("Out of memory for connection");
      return -2;
    }

  conn->fd = connfd;
  conn->state = STATE_REQ;
  conn->rbuf_size = 0;
  conn->wbuf_size = 0;
  conn->wbuf_sent = 0;
  conn->idle_start = get_monotonic_usec ();
  // The 'close_after_sending' flag is now handled by the STATE_RES_CLOSE enum value.
  dlist_insert_before (&g_data.idle_list, &conn->idle_list);

  connpool_add (g_data.fd2conn, conn);
  DBG_LOGF ("Accepted new connection: FD %d", connfd);

  return connfd;
}

// Dynamically sets the epoll events for a connection.
static void
conn_set_epoll_events (Conn *conn, uint32_t events)
{
  if (conn->state == STATE_END)
    return;

  struct epoll_event event;
  event.data.fd = conn->fd;
  // Always use EPOLLET (Edge Triggered)
  event.events = events | EPOLLET;

  // Use epoll_ctl for MOD; this is the slow path we want to minimize
  if (epoll_ctl (g_data.epfd, EPOLL_CTL_MOD, conn->fd, &event) == -1)
    {
      if (errno == ENOENT)
	return;
      msgf ("epoll ctl: MOD failed: %s", strerror (errno));
    }
}

// Helper to append a length-prefixed data chunk (TLV response format) to the connection's write buffer.
static void
buffer_response_chunk (Conn *conn, const uint8_t *data, size_t data_len)
{
  assert (conn->wbuf_size + 4 + data_len <= sizeof conn->wbuf);

  uint32_t nwlen = htonl ((uint32_t) data_len);
  // Append length header
  memcpy (&conn->wbuf[conn->wbuf_size], &nwlen, 4);
  // Append data
  memcpy (&conn->wbuf[conn->wbuf_size + 4], data, data_len);

  conn->wbuf_size += 4 + data_len;
}


static void
dump_error_and_close (Conn *conn, int code, const char *str)
{
  DBG_LOGF ("FD %d: Sending error %d and closing: %s", conn->fd, code, str);
  Buffer *err = buf_new ();
  out_err (err, code, str);
  size_t errlen = buf_len (err);

  // Can it fit into the write buffer?
  if (4 + errlen <= sizeof (conn->wbuf))
    {
      // Reset the buffer state so the error response overwrites any existing
      // partial data and starts at index 0.
      conn->wbuf_size = 0;
      conn->wbuf_sent = 0;

      // Use the common helper to buffer the error response.
      buffer_response_chunk (conn, buf_data (err), errlen);

      // Set the state to respond and then close
      conn->state = STATE_RES_CLOSE;
      conn_set_epoll_events (conn, EPOLLIN | EPOLLOUT);
    }
  else
    {
      // If even the error won't fit → hard close
      conn->state = STATE_END;
    }

  buf_free (err);
}

// returns true on success, false otherwise (must write error to 'out' on failure)
static bool
do_request (Cache *cache, Conn *conn, const uint8_t *req, uint32_t reqlen,
	    Buffer *out)
{
  if (reqlen < 4)
    {
      dump_error_and_close (conn, ERR_MALFORMED,
			    "Request too short for argument count");
      return false;
    }
  uint32_t num = 0;
  memcpy (&num, &req[0], 4);
  num = ntohl (num);

  if (num > K_MAX_ARGS)
    {
      DBG_LOG ("too many arguments");
      dump_error_and_close (conn, ERR_MALFORMED, "Too many arguments.");
      return false;
    }
  if (num < 1)
    {
      dump_error_and_close (conn, ERR_MALFORMED,
			    "Must have at least one argument (the command)");
      return false;
    }

  char **cmd = calloc (num, sizeof (char *));
  if (!cmd)
    die ("Out of memory cmd");
  size_t cmd_size = 0;

  bool success = false;
  size_t pos = 4;
  while (num--)
    {
      if (pos + 4 > reqlen)
	{
	  dump_error_and_close (conn, ERR_MALFORMED,
				"Argument count mismatch: missing length header");
	  goto CLEANUP;
	}
      uint32_t siz = 0;
      memcpy (&siz, &req[pos], 4);
      siz = ntohl (siz);

      if (pos + 4 + siz > reqlen)
	{
	  dump_error_and_close (conn, ERR_MALFORMED,
				"Argument count mismatch: data length exceeds packet size");
	  goto CLEANUP;
	}
      cmd[cmd_size] = (char *) (calloc (siz + 1, sizeof (char)));
      if (!cmd[cmd_size])
	die ("Out of memory");
      memcpy (cmd[cmd_size], &req[pos + 4], siz);
      cmd[cmd_size][siz] = '\0';	// Ensure it is null-terminated
      cmd_size++;
      pos += 4 + siz;
    }

  if (pos != reqlen)
    {
      dump_error_and_close (conn, ERR_MALFORMED,
			    "Trailing garbage in request");
      goto CLEANUP;
    }

  DBG_LOGF ("FD %d: Executing command with %zu arguments", conn->fd,
	    cmd_size);
  // If we reach here, the packet structure is valid, execute the command
  cache_execute (cache, cmd, cmd_size, out);
  success = true;

CLEANUP:
  for (size_t i = 0; i < cmd_size; i++)
    {
      if (cmd[i])
	{
	  free (cmd[i]);
	}
    }
  free (cmd);

  return success;
}

// Executes the request, prepares the response packet (length prefix + data),
// and appends it to conn->wbuf.
// Returns the success status of the request execution (do_request).
// The response data length is passed back via out_wlen.
static bool
execute_and_buffer_response (Cache *cache, Conn *conn,
			     const uint8_t *req_data, uint32_t req_len,
			     size_t *out_wlen)
{
  Buffer *out = buf_new ();
  bool success = do_request (cache, conn, req_data, req_len, out);

  // If do_request failed, dump_error_and_close has already filled
  // conn->wbuf and set state to STATE_RES_CLOSE/STATE_END. We must exit now
  // to avoid corrupting the error buffer.
  if (!success)
    {
      buf_free (out);
      *out_wlen = 0;
      return false;
    }

  size_t wlen = buf_len (out);
  size_t needed = 4 + wlen;	// 4 bytes for length + data length

  size_t avail = sizeof conn->wbuf - conn->wbuf_size;

  // Robust Buffer Overflow Check & Error Handling (Expensive Path)
  if (needed > avail)
    {
      DBG_LOGF ("FD %d: Response (len %zu) too large for buffer (avail %zu).",
		conn->fd, needed, avail);
      // The response is too large for the connection's write buffer.
      // Generate a small "server busy" error and send that instead.
      // Note: This path keeps the connection alive (STATE_RES).

      Buffer *errb = buf_new ();
      out_err (errb, ERR_UNKNOWN, "server busy: response truncated");
      size_t errlen = buf_len (errb);

      // Check if even the small error response will fit
      if (conn->wbuf_size + 4 + errlen <= sizeof conn->wbuf)
	{
	  // Append length header and error data using the new helper
	  buffer_response_chunk (conn, buf_data (errb), errlen);

	  // Transition to writing the error immediately (keeping connection open)
	  conn->state = STATE_RES;
	  conn_set_epoll_events (conn, EPOLLIN | EPOLLOUT);

	  goto CLEANUP;
	}

      conn->state = STATE_END;
    CLEANUP:
      // If even the small error doesn't fit, we have to drop the connection
      buf_free (errb);
      buf_free (out);
      *out_wlen = 0;
      return false;
    }
  // Robust Buffer Overflow Check & Error Handling

  // Append length header (4 bytes) and response data (wlen) to wbuf
  buffer_response_chunk (conn, buf_data (out), wlen);
  DBG_LOGF ("FD %d: Buffered response of length %zu. wbuf_size now %zu.",
	    conn->fd, wlen, conn->wbuf_size);

  buf_free (out);
  *out_wlen = wlen;		// Pass the response length back out

  return success;
}

static bool
try_one_request (Cache *cache, Conn *conn, uint32_t *start_index)
{
  // try to parse a request from the buffer
  if (conn->rbuf_size < *start_index + 4)
    {
      // not enough data in the buffer. Will retry in the next iteration
      return false;
    }
  uint32_t len = 0;
  memcpy (&len, &conn->rbuf[*start_index], 4);
  len = ntohl (len);

  DBG_LOGF ("FD %d: Parsing request starting at index %u with length %u.",
	    conn->fd, *start_index, len);

  // Check against the server's maximum message size. K_MAX_MSG is the correct, permanent limit.
  if (len > K_MAX_MSG)
    {
      msgf ("request too long %i", len);
      dump_error_and_close (conn, ERR_2BIG,
			    "request too large; connection closed.");
      *start_index = conn->rbuf_size;

      // Stop processing further requests from this buffer segment.
      return false;
    }

  if (4 + len + *start_index > conn->rbuf_size)
    {
      // not enough data in the buffer. Will retry in the next iteration
      return false;
    }

  size_t wlen = 0;		// wlen is needed for the state transition check below
  bool success =
    execute_and_buffer_response (cache, conn, &conn->rbuf[*start_index + 4],
				 len, &wlen);

  if (!success)
    {
      // If execute_and_buffer_response returned false, it means:
      // 1. Malformed request -> dump_error_and_close set STATE_RES_CLOSE/STATE_END.
      // 2. Response too large -> set STATE_RES with error, or STATE_END if error didn't fit.
      // In all failure cases, we must stop processing the *rest* of the requests
      // in the buffer, as the connection is now focused on sending the error response.

      // Update the start_index to consume the failed request
      *start_index += 4 + len;

      msg
	("request failed, or could not buffer response, stopping processing for now");
      // Stop the processing loop immediately
      return false;
    }

  // Request succeeded
  *start_index += 4 + len;
  DBG_LOGF ("FD %d: Request processed, consumed %u bytes.", conn->fd,
	    4 + len);

  if (*start_index >= conn->rbuf_size)
    {
      // we read all data from the client, try to send!
      conn->state = STATE_RES;
      DBG_LOGF ("FD %d: RBuf fully consumed, transitioning to STATE_RES.",
		conn->fd);
      // Add EPOLLOUT when we transition to STATE_RES
      conn_set_epoll_events (conn, EPOLLIN | EPOLLOUT);
      // We set conn->state to STATE_RES, so we must stop the processing loop
      return false;
    }

  // Continue processing requests if the connection is still in the request state
  return (conn->state == STATE_REQ);
}

// Helper function to read data from the socket with error handling.
// Returns: > 0 (bytes read), -1 (EAGAIN/drained), -2 (error/EOF)
static ssize_t
read_data_from_socket (Conn *conn, size_t cap)
{
  ssize_t err;

  do
    {
      err = read (conn->fd, &conn->rbuf[conn->rbuf_size], cap);
    }
  while (err < 0 && errno == EINTR);

  if (err < 0)
    {
      if (errno == EAGAIN)
	{
	  return -1;		// Socket drained
	}
      msgf ("read() error: %s", strerror (errno));
      conn->state = STATE_END;
      return -2;
    }

  if (err == 0)
    {
      DBG_LOGF ("FD %d: EOF received, setting state to STATE_END.", conn->fd);
      conn->state = STATE_END;
      // We return -2 to signal the outer loop (try_fill_buffer) that
      // processing must stop immediately and the connection is done.
      return -2;
    }

  DBG_LOGF ("FD %d: Read %zd bytes from socket.", conn->fd, err);
  // Success: err > 0
  return err;
}

// Helper function to process newly received data (request parsing and buffer compaction).
// Returns: true if the outer draining loop should stop (due to state change or MAX_CHUNKS), false otherwise.
static bool
process_received_data (Cache *cache, Conn *conn, uint32_t bytes_read)
{
  uint32_t start_index = conn->rbuf_size;
  conn->rbuf_size += bytes_read;
  assert (conn->rbuf_size <= sizeof (conn->rbuf));

  int processed = 0;
  while (try_one_request (cache, conn, &start_index))
    {
      // MAX_CHUNKS acts as flow control to prevent one client from starving the thread.
      if (++processed >= MAX_CHUNKS)
	break;
    }

  // Buffer Compaction: Correctly handles overlapping regions.
  uint32_t remain = conn->rbuf_size - start_index;
  if (remain)
    {
      memmove (conn->rbuf, &conn->rbuf[start_index], remain);
    }
  conn->rbuf_size = remain;

  DBG_LOGF
    ("FD %d: Processed %d requests. RBuf remaining: %u. New state: %d.",
     conn->fd, processed, remain, conn->state);

  // State Transition/Flush Check
  if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE
      || conn->state == STATE_END)
    {
      // Do NOT call state_res() from here. The transition to STATE_RES/STATE_RES_CLOSE
      // armed EPOLLOUT, which the main event loop will handle it cleanly.
      return true;
    }

  // Stop draining if we hit the chunk limit
  if (processed >= MAX_CHUNKS)
    {
      return true;
    }

  // Continue draining if more capacity is available
  return false;
}

static bool
try_fill_buffer (Cache *cache, Conn *conn)
{
  // Outer loop to drain the socket in ET mode
  while (1)
    {
      size_t cap = sizeof (conn->rbuf) - conn->rbuf_size;

      if (cap == 0)
	{
	  // Client read buffer is full, transition to response state
	  DBG_LOGF ("FD %d: RBuf full, transitioning to STATE_RES.",
		    conn->fd);
	  conn->state = STATE_RES;
	  break;
	}

      ssize_t bytes_read = read_data_from_socket (conn, cap);

      if (bytes_read == -2)
	{
	  // Critical error or EOF, connection terminated (STATE_END already set)
	  return true;
	}

      if (bytes_read == -1)
	{
	  // EAGAIN -> socket drained, no more data for now
	  return false;
	}

      // bytes_read > 0, process data.
      if (process_received_data (cache, conn, (uint32_t) bytes_read))
	{
	  // The inner processor requested a stop (MAX_CHUNKS hit or state changed)
	  return true;
	}
    }

  // If we broke out due to cap == 0 or processing finished,
  // The state transition and EPOLLOUT arming will handle the rest via the main loop.
  return true;
}

static bool
try_flush_buffer (Conn *conn)
{
  DBG_LOGF ("FD %d: Flushing WBuf (size %zu, sent %zu).", conn->fd,
	    conn->wbuf_size, conn->wbuf_sent);
  while (conn->wbuf_sent < conn->wbuf_size)
    {
      size_t remain = conn->wbuf_size - conn->wbuf_sent;
      ssize_t err =
	send (conn->fd, &conn->wbuf[conn->wbuf_sent], remain, MSG_NOSIGNAL);

      if (err < 0)
	{
	  if (errno == EINTR)
	    continue;
	  if (errno == EAGAIN)
	    {
	      DBG_LOGF ("FD %d: Send blocked (EAGAIN).", conn->fd);
	      return false;
	    }
	  msgf ("write() error: %s", strerror (errno));
	  conn->state = STATE_END;
	  return false;
	}
      if (err == 0)
	{
	  msg ("write returned 0 unexpectedly");
	  conn->state = STATE_END;
	  return false;
	}

      conn->wbuf_sent += (size_t) err;
      DBG_LOGF ("FD %d: Sent %zd bytes. Total sent %zu.", conn->fd, err,
		conn->wbuf_sent);
    }

  // response was fully sent
  conn->wbuf_sent = 0;
  conn->wbuf_size = 0;

  // Check if we were in the "Respond and Close" state
  if (conn->state == STATE_RES_CLOSE)
    {
      DBG_LOGF ("FD %d: Response sent, transitioning to STATE_END for close.",
		conn->fd);
      conn->state = STATE_END;	// Transition to END for cleanup
      return false;		// Stop flushing loop, connection marked for cleanup
    }

  // If the state was STATE_RES, transition back to STATE_REQ for next request
  if (conn->state == STATE_RES)
    {
      DBG_LOGF ("FD %d: Response sent, transitioning back to STATE_REQ.",
		conn->fd);
      conn->state = STATE_REQ;
      // Remove EPOLLOUT when we transition back to STATE_REQ
      conn_set_epoll_events (conn, EPOLLIN);
    }

  return false;			// nothing left to write right now (or STATE_END)
}

static void
state_req (Cache *cache, Conn *conn)
{
  DBG_LOGF ("FD %d: Entering STATE_REQ handler.", conn->fd);
  // Keep draining/processing until try_fill_buffer indicates EAGAIN (false)
  // or a state change happened (true, which breaks the loop inside try_fill_buffer)
  while (try_fill_buffer (cache, conn))
    {
      if (conn->state != STATE_REQ)
	break;
    }
  DBG_LOGF ("FD %d: Exiting STATE_REQ handler. New state: %d.", conn->fd,
	    conn->state);
}

static void
state_res (Conn *conn)
{
  // Draining loop for edge-triggered EPOLLOUT event
  while (!try_flush_buffer (conn))
    {
      if (conn->state != STATE_RES && conn->state != STATE_RES_CLOSE)
	break;
    }
}

static void
conn_done (Conn *conn)
{
  DBG_LOGF ("Cleaning up and closing connection: FD %d", conn->fd);
  // This is the single, authoritative place to free the connection structure.
  // best-effort; ignore epoll errors
  (void) epoll_ctl (g_data.epfd, EPOLL_CTL_DEL, conn->fd, NULL);	// Remove from epoll
  connpool_remove (g_data.fd2conn, conn->fd);
  close (conn->fd);
  dlist_detach (&conn->idle_list);
  free (conn);
}

static void
process_timers (Cache *cache)
{
  uint64_t now_us = get_monotonic_usec ();

  while (!dlist_empty (&g_data.idle_list))
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Conn *next = container_of (g_data.idle_list.next, Conn, idle_list);
#pragma GCC diagnostic pop

      uint64_t next_us = next->idle_start + K_IDLE_TIMEOUT_US;

      // If the next connection's expiry time is strictly in the future,
      // we stop checking the list as the rest of the list (being newer)
      // will also not be ready.
      if (next_us > now_us)
	{
	  break;
	}

      msgf ("Removing idle connection: %d", next->fd);
      conn_done (next);
    }

  cache_evict (cache, now_us);
}

static void
connection_io (Cache *cache, Conn *conn)
{
  // waked up by epoll, update the idle timer
  // by moving conn to the end of the list.
  conn->idle_start = get_monotonic_usec ();
  dlist_detach (&conn->idle_list);
  dlist_insert_before (&g_data.idle_list, &conn->idle_list);

  if (conn->state == STATE_REQ)
    {
      // If EPOLLIN fired, we prioritize state_req until EAGAIN.
      state_req (cache, conn);
    }
  // If connection is in STATE_RES or STATE_RES_CLOSE, it means it has data to write.
  // We only proceed to state_res if the state hasn't changed *out* of those states
  if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE)
    {
      state_res (conn);
    }
  // We rely on the caller to check conn->state == STATE_END after this function returns.
}

// Drains the listening socket and registers all newly accepted connections
// with the epoll instance.
static void
handle_listener_event (int listen_fd)
{
  int epfd = g_data.epfd;	// Get epfd from global data

  while (1)
    {
      int conn_fd = accept_new_conn (listen_fd);
      if (conn_fd < 0)
	{
	  // -1 means EAGAIN/EWOULDBLOCK (no more connections), stop draining
	  // -2 means critical error (OOM or failed accept)
	  if (conn_fd == -1)
	    {
	      break;
	    }
	  break;
	}

      // Successfully accepted, now register with epoll
      struct epoll_event event;
      event.data.fd = conn_fd;
      // Only register for EPOLLIN (read events) initially.
      // EPOLLOUT will be added dynamically when data is ready to be sent.
      event.events = EPOLLIN | EPOLLET;
      if (epoll_ctl (epfd, EPOLL_CTL_ADD, event.data.fd, &event) == -1)
	{
	  msgf ("epoll ctl: new conn registration failed: %s",
		strerror (errno));
	  close (conn_fd);	// ensure we don't leak FD if epoll_ctl fails
	}
    }				// end while(1) drain loop
}

// Handles an epoll event for an active client connection (not the listener).
static void
handle_connection_event (Cache *cache, struct epoll_event *event)
{
  Conn *conn = connpool_lookup (g_data.fd2conn, event->data.fd);
  if (!conn)
    {
      return;			// Already cleaned up or invalid fd
    }

  DBG_LOGF ("FD %d: Handling epoll event (events: 0x%x). State: %d",
	    event->data.fd, event->events, conn->state);

  // Check for half-close (RDHUP), full hangup (HUP), or error (ERR) (Issue 12)
  if (event->events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP))
    {
      DBG_LOGF ("FD %d: Hangup/Error detected (0x%x). Closing.",
		event->data.fd, event->events);
      // Mark for termination immediately, perform cleanup, and return.
      conn->state = STATE_END;
      conn_done (conn);
      return;
    }

  // If there is an event and the connection isn't marked for close, process it.
  // EPOLLOUT events are only armed when conn->state is STATE_RES or STATE_RES_CLOSE.
  if (event->events & (EPOLLIN | EPOLLOUT))
    {
      connection_io (cache, conn);
    }

  // Cleanup if any I/O operation (like read or write) transitioned it to STATE_END.
  // This is the single, authoritative check for I/O triggered connection termination.
  if (conn->state == STATE_END)
    {
      conn_done (conn);
    }
}

// Dispatches epoll events to the appropriate handler (listener or connection).
static void
process_active_events (Cache *cache, struct epoll_event *events, int count,
		       int listen_fd)
{
  for (int i = 0; i < count; ++i)
    {
      if (events[i].data.fd == listen_fd)
	{
	  handle_listener_event (listen_fd);
	}
      else
	{
	  handle_connection_event (cache, &events[i]);
	}
    }
}

// Performs graceful shutdown, closes all active connections, and cleans up resources.
static void
cleanup_server_resources (Cache *cache, int listen_fd, int epfd)
{
  msg ("\nServer shutting down gracefully. Cleaning up resources...");
  Conn **connections = NULL;
  size_t count = 0;
  // connpool_iter returns a pointer to the internal array (no new allocation)
  connpool_iter (g_data.fd2conn, &connections, &count);

  // Close all active client connections
  for (size_t i = count; i-- > 0;)
    {
      Conn *conn = connections[i];
      msgf ("Forcing cleanup of active connection: %d", conn->fd);
      conn_done (conn);
    }

  connpool_free (g_data.fd2conn);
  cache_free (cache);

  // Close server file descriptors
  close (listen_fd);
  close (epfd);
  msg ("Cleanup complete.");
}

// Initializes the core server components (socket, bind, listen, epoll).
// Returns 0 on success, -1 on failure. Sets listen_fd and epfd on success.
static int
initialize_server_core (uint16_t port, int *listen_fd, int *epfd)
{
  struct sockaddr_in addr = { 0 };
  int err, val = 1;
  struct epoll_event event;

  // Create Listening Socket
  *listen_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (*listen_fd < 0)
    {
      die ("socket()");
    }

  setsockopt (*listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

  // Bind
  addr.sin_family = AF_INET;
  addr.sin_port = htons (port);
  addr.sin_addr.s_addr = htonl (0);
  err = bind (*listen_fd, (const struct sockaddr *) &addr, sizeof (addr));
  if (err)
    {
      close (*listen_fd);
      die ("bind()");
    }

  // Listen
  err = listen (*listen_fd, SOMAXCONN);
  if (err)
    {
      close (*listen_fd);
      die ("listen()");
    }
  msgf ("The server is listening on port %i.", port);

  // Initialize connection pool and set non-blocking
  g_data.fd2conn = connpool_new (10);
  fd_set_nb (*listen_fd);	// Listen FD must be non-blocking

  // Create Epoll Instance
  *epfd = epoll_create1 (0);
  if (*epfd == -1)
    {
      close (*listen_fd);
      connpool_free (g_data.fd2conn);
      die ("epoll_create1");
    }
  g_data.epfd = *epfd;

  // Register Listener with Epoll
  // Listener only needs EPOLLIN
  event.events = EPOLLIN | EPOLLET;
  event.data.fd = *listen_fd;
  if (epoll_ctl (*epfd, EPOLL_CTL_ADD, *listen_fd, &event) == -1)
    {
      close (*listen_fd);
      close (*epfd);
      connpool_free (g_data.fd2conn);
      die ("epoll ctl: listen_sock!");
    }

  return 0;			// Success
}

static uint32_t
next_timer_ms (Cache *cache)
{
  uint64_t now_us = get_monotonic_usec ();
  uint64_t next_us = (uint64_t) - 1;
  uint32_t final_timeout;

  // idle timers
  if (!dlist_empty (&g_data.idle_list))
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Conn *next = container_of (g_data.idle_list.next, Conn, idle_list);
#pragma GCC diagnostic pop

      next_us = next->idle_start + K_IDLE_TIMEOUT_US;
    }

  uint64_t from_cache = cache_next_expiry (cache);

  if (from_cache != (uint64_t) - 1 && from_cache < next_us)
    {
      next_us = from_cache;
    }

  if (next_us == (uint64_t) - 1)
    {
      final_timeout = 10000;	// no timer, the value doesn't matter
      return final_timeout;
    }

  if (next_us <= now_us)
    {
      // missed?
      final_timeout = 0;
      return final_timeout;
    }

  final_timeout = (uint32_t) ((next_us - now_us) / 1000);
  // Ensure a minimum timeout of 1ms to prevent continuous, tight loops in epoll_wait
  if (final_timeout == 0)
    {
      final_timeout = 1;
    }
  return final_timeout;
}

int
server_run (uint16_t port)
{
  struct epoll_event events[MAX_EVENTS];
  int listen_fd = -1;
  int epfd = -1;
  g_data.terminate_flag = 0;	// Initialize termination flag

  // Setup signal handler for graceful shutdown
  setup_signal_handlers ();

  dlist_init (&g_data.idle_list);
  Cache *cache = cache_init ();

  // Server Initialization
  if (initialize_server_core (port, &listen_fd, &epfd) != 0)
    {
      return -1;
    }

  // Main Event Loop
  while (!g_data.terminate_flag)	// Use the flag to control the loop
    {
      int timeout_ms = (int) next_timer_ms (cache);
      DBG_LOGF ("Epoll wait for %dms...", timeout_ms);
      // epoll for active fds
      int enfd_count = epoll_wait (epfd, events, MAX_EVENTS, timeout_ms);
      if (g_data.terminate_flag)
	{
	  break;		// Re-check flag after potential long wait
	}
      if (enfd_count < 0)
	{
	  if (errno == EINTR)
	    {
	      continue;		// Signal interrupted, but we handled it above
	    }
	  die ("epoll_wait");
	}
      DBG_LOGF ("Processing %d events.", enfd_count);
      // process active connections (listener and client events)
      if (enfd_count > 0)
	{
	  process_active_events (cache, events, enfd_count, listen_fd);
	}

      process_timers (cache);
    }

  // Graceful shutdown and resource cleanup
  cleanup_server_resources (cache, listen_fd, epfd);

  return 0;
}
