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

#include "cache.h"
#include "connections.h"
#include "buffer.h"
#include "common.h"
#include "out.h"
#include "server_loop.h"

#define MAX_EVENTS 10000
#define MAX_CHUNKS 16
#define K_IDLE_TIMEOUT_US 5000000;

enum
{
  RES_OK = 0, RES_ERR = 1, RES_NX = 2,
};

enum
{
  STATE_REQ = 0, STATE_RES = 1, STATE_END = 2,	// mark the connection for deletion
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
  struct sigaction sa = { 0 };
  sa.sa_handler = sigint_handler;
  sigaction (SIGINT, &sa, NULL);
  sigaction (SIGTERM, &sa, NULL);
  sigaction (SIGQUIT, &sa, NULL);
}

static void
fd_set_nb (int fd)
{
  errno = 0;
  int flags = fcntl (fd, F_GETFL, 0);
  if (errno)
    {
      die ("fcntl error");
      return;
    }

  flags |= O_NONBLOCK;

  errno = 0;
  (void) fcntl (fd, F_SETFL, flags);
  if (errno)
    {
      die ("fcntl error");
    }
}

static int32_t
accept_new_conn (int fd)
{
  struct sockaddr_in client_addr = { 0 };
  socklen_t socklen = sizeof (client_addr);
  int connfd = accept (fd, (struct sockaddr *) &client_addr, &socklen);

  if (connfd < 0)
    {
      if (errno == EAGAIN)
	{
	  // No more connections to accept
	  return -1;
	}
      msg ("accept() error");
      return -2;		// Distinguish between no connection (-1) and critical error (-2)
    }

  fd_set_nb (connfd);
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
  dlist_insert_before (&g_data.idle_list, &conn->idle_list);

  connpool_add (g_data.fd2conn, conn);

  return connfd;
}

static void
conn_done (Conn *conn)
{
  epoll_ctl (g_data.epfd, EPOLL_CTL_DEL, conn->fd, NULL);	// Remove from epoll
  connpool_remove (g_data.fd2conn, conn->fd);
  close (conn->fd);
  dlist_detach (&conn->idle_list);
  free (conn);
}

static void state_req (Cache * cache, Conn * conn);
static void state_res (Conn * conn);

// returns true on success, false otherwise (must write error to 'out' on failure)
static bool
do_request (Cache *cache, const uint8_t *req, uint32_t reqlen, Buffer *out)
{
  if (reqlen < 4)
    {
      out_err (out, ERR_MALFORMED, "Request too short for argument count");
      return false;
    }
  uint32_t n = 0;
  memcpy (&n, &req[0], 4);
  n = ntohl (n);

  if (n > K_MAX_ARGS)
    {
      out_err (out, ERR_UNKNOWN, "Too many arguments");
      return false;
    }
  if (n < 1)
    {
      out_err (out, ERR_MALFORMED,
	       "Must have at least one argument (the command)");
      return false;
    }

  char **cmd = calloc (n, sizeof (char *));
  if (!cmd)
    die ("Out of memory cmd");
  size_t cmd_size = 0;

  bool success = false;
  size_t pos = 4;
  while (n--)
    {
      if (pos + 4 > reqlen)
	{
	  out_err (out, ERR_MALFORMED,
		   "Argument count mismatch: missing length header");
	  goto CLEANUP;
	}
      uint32_t sz = 0;
      memcpy (&sz, &req[pos], 4);
      sz = ntohl (sz);

      if (pos + 4 + sz > reqlen)
	{
	  out_err (out, ERR_MALFORMED,
		   "Argument count mismatch: data length exceeds packet size");
	  goto CLEANUP;
	}
      cmd[cmd_size] = (char *) (calloc (sz + 1, sizeof (char)));
      if (!cmd[cmd_size])
	die ("Out of memory");
      memcpy (cmd[cmd_size], &req[pos + 4], sz);
      cmd_size++;
      pos += 4 + sz;
    }

  if (pos != reqlen)
    {
      out_err (out, ERR_MALFORMED, "Trailing garbage in request");
      goto CLEANUP;
    }

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
  bool success = do_request (cache, req_data, req_len, out);

  size_t wlen = buf_len (out);
  uint32_t nwlen = htonl ((uint32_t) wlen);

  // Append length header (4 bytes) and response data (wlen) to wbuf
  memcpy (&conn->wbuf[conn->wbuf_size], &nwlen, 4);
  memcpy (&conn->wbuf[conn->wbuf_size + 4], buf_data (out), wlen);

  conn->wbuf_size += 4 + wlen;

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

  if (len > K_MAX_MSG)
    {
      msg ("too long");
      conn->state = STATE_END;
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
  *start_index += 4 + len;

  if (!success)
    {
      msg ("bad req, closing connection");
      conn->state = STATE_END;
    }
  else if (*start_index >= conn->rbuf_size
	   || (conn->wbuf_size + wlen + 4) > K_MAX_MSG)
    {
      // we read it all, try to send!
      conn->state = STATE_RES;
    }

  // Continue processing requests if the connection is still in the request state
  return (conn->state == STATE_REQ);
}

static bool
try_fill_buffer (Cache *cache, Conn *conn)
{
  // try to fill the buffer
  assert (conn->rbuf_size < sizeof (conn->rbuf));
  ssize_t rv = 0;
  do
    {
      size_t cap = sizeof (conn->rbuf) - conn->rbuf_size;
      rv = read (conn->fd, &conn->rbuf[conn->rbuf_size], cap);
    }
  while (rv < 0 && errno == EINTR);
  if (rv < 0 && errno == EAGAIN)
    {
      return false;
    }
  if (rv < 0)
    {
      msg ("read() error");
      conn->state = STATE_END;
      return false;
    }
  if (rv == 0)
    {
      if (conn->rbuf_size > 0)
	{
	  msg ("unexpected EOF");
	}
      conn->state = STATE_END;
      return false;
    }

  uint32_t start_index = conn->rbuf_size;
  conn->rbuf_size += (uint32_t) rv;
  assert (conn->rbuf_size <= sizeof (conn->rbuf));

  int processed = 0;
  while (try_one_request (cache, conn, &start_index))
    {
      if (++processed >= MAX_CHUNKS)
	break;
    }

  uint32_t remain = conn->rbuf_size - start_index;
  if (remain)
    {
      memmove (conn->rbuf, &conn->rbuf[start_index], remain);
    }
  conn->rbuf_size = remain;

  // If we ended up in STATE_RES due to a finished read or bad request, transition to state_res
  if (conn->state == STATE_RES || conn->state == STATE_END)
    {
      state_res (conn);
    }
  return (conn->state == STATE_REQ);
}

static void
state_req (Cache *cache, Conn *conn)
{
  while (try_fill_buffer (cache, conn))
    {
    }
}

static bool
try_flush_buffer (Conn *conn)
{
  ssize_t rv = 0;
  do
    {
      size_t remain = conn->wbuf_size - conn->wbuf_sent;
      rv = write (conn->fd, &conn->wbuf[conn->wbuf_sent], remain);
    }
  while (rv < 0 && errno == EINTR);
  if (rv < 0 && errno == EAGAIN)
    {
      return false;
    }
  if (rv < 0)
    {
      msg ("write() error");
      conn->state = STATE_END;
      return false;
    }
  conn->wbuf_sent += (size_t) rv;
  assert (conn->wbuf_sent <= conn->wbuf_size);
  if (conn->wbuf_sent == conn->wbuf_size)
    {
      // response was fully sent, change state back or finalize
      conn->wbuf_sent = 0;
      conn->wbuf_size = 0;

      if (conn->state == STATE_END)
	{
	  return false;		// Done writing the error response, let the loop clean up
	}

      conn->state = STATE_REQ;
      return false;
    }
  // still got some data in wbuf, could try to write again
  return true;
}

static void
state_res (Conn *conn)
{
  while (try_flush_buffer (conn))
    {
    }
}

static void
process_timers (Cache *cache)
{
  uint64_t now_us = get_monotonic_usec ();

  while (!dlist_empty (&g_data.idle_list))
    {
      Conn *next = container_of (g_data.idle_list.next, Conn, idle_list);

      uint64_t next_us = next->idle_start + K_IDLE_TIMEOUT_US;

      // If the next connection's expiry time is strictly in the future,
      // we stop checking the list as the rest of the list (being newer)
      // will also not be ready.
      if (next_us > now_us)
	{
	  break;
	}

      fprintf (stderr, "removing idle connection: %d\n", next->fd);
      // conn_done removes 'next' from all structures and frees the memory.
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
      state_req (cache, conn);
    }
  else if (conn->state == STATE_RES)
    {
      state_res (conn);
    }
  else
    {
      assert (0);
    }
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
      struct epoll_event ev;
      ev.data.fd = conn_fd;
      ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
      if (epoll_ctl (epfd, EPOLL_CTL_ADD, ev.data.fd, &ev) == -1)
	{
	  msg ("epoll ctl: new conn registration failed");
	  close (conn_fd);	// ensure we don't leak FD if epoll_ctl fails
	}
    }				// end while(1) drain loop
}

// Handles an epoll event for an active client connection (not the listener).
static void
handle_connection_event (Cache *cache, struct epoll_event *ev)
{
  Conn *conn = connpool_lookup (g_data.fd2conn, ev->data.fd);
  if (!conn)
    {
      return;			// Already cleaned up or invalid fd
    }

  if (ev->events & (EPOLLHUP | EPOLLERR))
    {
      // Mark for termination, but allow any pending I/O to run if available
      conn->state = STATE_END;
    }

  if (conn->state == STATE_END)
    {
      conn_done (conn);
      return;
    }

  if (ev->events & (EPOLLIN | EPOLLOUT))
    {
      connection_io (cache, conn);
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
  Conn **connections;
  size_t count;
  connpool_iter (g_data.fd2conn, &connections, &count);

  // Close all active client connections
  for (size_t i = count - 1; i < count; i--)
    {
      Conn *conn = connections[i];
      fprintf (stderr, "Forcing cleanup of active connection: %d\n",
	       conn->fd);
      conn_done (conn);
    }

  connpool_free (g_data.fd2conn);

  // TODO: implemnt this please :)
  //cache_free(cache);

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
  int rv, val = 1;
  struct epoll_event event;

  // Create Listening Socket
  *listen_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (*listen_fd < 0)
    {
      die ("socket()");
      return -1;
    }

  setsockopt (*listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

  addr.sin_family = AF_INET;
  addr.sin_port = htons (port);
  addr.sin_addr.s_addr = htonl (0);
  rv = bind (*listen_fd, (const struct sockaddr *) &addr, sizeof (addr));
  if (rv)
    {
      die ("bind()");
      close (*listen_fd);
      return -1;
    }

  rv = listen (*listen_fd, SOMAXCONN);
  if (rv)
    {
      die ("listen()");
      close (*listen_fd);
      return -1;
    }
  fprintf (stderr, "The server is listening on port %i.\n", port);

  // Initialize connection pool and set non-blocking
  g_data.fd2conn = connpool_new (10);
  fd_set_nb (*listen_fd);

  // Create Epoll Instance
  *epfd = epoll_create1 (0);
  if (*epfd == -1)
    {
      die ("epoll_create1");
      close (*listen_fd);
      return -1;
    }
  g_data.epfd = *epfd;

  // Register Listener with Epoll
  event.events = EPOLLIN | EPOLLOUT | EPOLLET;
  event.data.fd = *listen_fd;
  if (epoll_ctl (*epfd, EPOLL_CTL_ADD, *listen_fd, &event) == -1)
    {
      close (*listen_fd);
      close (*epfd);
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
      Conn *next = container_of (g_data.idle_list.next, Conn, idle_list);
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
