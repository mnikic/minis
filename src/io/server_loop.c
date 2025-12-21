#define _GNU_SOURCE
/*
 *============================================================================
 * Name             : server_loop.c
 * Author           : Milos
 * Description      : Server loop, connection handling.
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
#include <signal.h>

#include "cache/cache.h"
#include "connection_handler.h"
#include "conn_pool.h"
#include "server_loop.h"
#include "common/common.h"
#include "common/macros.h"
#include "list.h"
#include "zerocopy.h"

#define MAX_EVENTS 10000
#define K_IDLE_TIMEOUT_SEC 300UL	// in seconds

static struct
{
  ConnPool *fd2conn;
  DList idle_list;
  int epfd;
  volatile sig_atomic_t terminate_flag;
} g_data;

static ALWAYS_INLINE uint64_t
get_monotonic_usec (void)
{
  struct timespec tvs = { 0, 0 };
  clock_gettime (CLOCK_MONOTONIC, &tvs);
  return (uint64_t) ((tvs.tv_sec * 1000000) + (tvs.tv_nsec / 1000));
}

// Signal handler function
static void
sigint_handler (int sig)
{
  (void) sig;
  g_data.terminate_flag = 1;
}

// Signal handler function (async-signal-safe)
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

static ALWAYS_INLINE void
fd_set_nb (int file_des)
{
  errno = 0;
  int flags = fcntl (file_des, F_GETFL, 0);
  if (unlikely (errno))
    die ("fcntl error");

  flags |= O_NONBLOCK;

  errno = 0;
  (void) fcntl (file_des, F_SETFL, flags);
  if (unlikely (errno))
    die ("fcntl error");
}

static void
handle_listener_event (int listen_fd, uint64_t now_us)
{
  int epfd = g_data.epfd;

  while (true)
    {
      // 1. Accept Loop Logic
      struct sockaddr_in client_addr = { 0 };
      socklen_t socklen = sizeof (client_addr);
      int connfd;

#if defined(__linux__) && defined(__GLIBC__) && (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 10)
      connfd = accept4 (listen_fd, (struct sockaddr *) &client_addr, &socklen,
			SOCK_NONBLOCK);
#else
      connfd = accept (listen_fd, (struct sockaddr *) &client_addr, &socklen);
      if (connfd >= 0)
	fd_set_nb (connfd);
#endif

      // 2. Handle Accept Failures
      if (unlikely (connfd < 0))
	{
	  if (errno == EAGAIN)
	    break;

	  msgf ("accept() error: %s", strerror (errno));
	  break;		// Stop looping on fatal errors too
	}

      int sndbuf = 2 * 1024 * 1024;
      setsockopt (connfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof (sndbuf));

      int val = 1;
      if (unlikely
	  (setsockopt (connfd, SOL_SOCKET, SO_ZEROCOPY, &val, sizeof (val))))
	{
	  msgf ("SO_ZEROCOPY not set. error: %s");
	}

      Conn *conn = connpool_get (g_data.fd2conn, connfd);

      if (unlikely (!conn))
	{
	  close (connfd);
	  DBG_LOG ("Dropped connection: Pool full or OOM.");
	  continue;
	}

      conn->idle_start = now_us;
      dlist_insert_before (&g_data.idle_list, &conn->idle_list);

      struct epoll_event event;
      event.data.fd = conn->fd;
      event.events = EPOLLIN | EPOLLET | EPOLLERR;
      conn->last_events = event.events;
      if (unlikely
	  (epoll_ctl (epfd, EPOLL_CTL_ADD, event.data.fd, &event) == -1))
	{
	  msgf ("epoll ctl: add failed: %s", strerror (errno));
	  // If we fail to add to epoll, we must release/close immediately
	  // or we leak the connection.
	  connpool_release (g_data.fd2conn, conn);
	  close (connfd);
	}
    }
}

static void
conn_done (Conn *conn)
{
  DBG_LOGF ("Cleaning up and closing connection: FD %d", conn->fd);

  // Safety check
  if (unlikely (conn->fd == -1))
    {
      return;
    }

  int file_desc = conn->fd;	// Save FD for operations

  // Remove from the idle list so the timer doesn't touch it
  if (dlist_is_linked (&conn->idle_list))
    {
      dlist_detach (&conn->idle_list);
    }

  zc_drain_errors (file_desc);
  close (file_desc);
  connpool_release (g_data.fd2conn, conn);
  // The Conn struct lives in the Slab and is recycled.
}

static ALWAYS_INLINE bool
conn_has_pending_write (const Conn *conn)
{
  uint32_t idx = conn->read_idx;

  // Loop through all slots starting from the oldest (read_idx) up to the newest (write_idx)
  while (idx != conn->write_idx)
    {
      const ResponseSlot *slot = &conn->res_slots[idx];

      // Check for any data waiting to be sent
      if (slot->sent < slot->actual_length)
	{
	  // Found data that still needs a successful write() or sendmsg()
	  return true;
	}

      // Check for any Zero-Copy operation waiting for kernel ACK
      if (slot->is_zerocopy && slot->pending_ops > 0)
	{
	  // Found a Zero-Copy buffer the kernel still owns
	  return true;
	}

      idx = (idx + 1) % K_SLOT_COUNT;
    }

  // If the loop finishes, all outstanding slots are fully sent and fully ACK'd.
  return false;
}

static void
process_timers (Cache *cache, uint64_t now_us)
{
  while (!dlist_empty (&g_data.idle_list))
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Conn *next = container_of (g_data.idle_list.next, Conn, idle_list);
#pragma GCC diagnostic pop

      uint64_t next_us = next->idle_start + (K_IDLE_TIMEOUT_SEC * 1000000);

      if (next_us > now_us)
	break;

      if (conn_has_pending_write (next))
	{
	  next->idle_start = now_us;

	  dlist_detach (&next->idle_list);
	  dlist_insert_before (&g_data.idle_list, &next->idle_list);

	  DBG_LOGF ("FD %d: Pending I/O. Resetting idle timer and skipping close.",	// Changed log
		    next->fd);
	  continue;		// Skip close and go to next item in the list
	}
      msgf ("Removing idle connection: %d", next->fd);
      conn_done (next);
    }

  cache_evict (cache, now_us);
}

static HOT void
connection_io (Cache *cache, Conn *conn, uint64_t now_us, uint32_t events)
{
  handle_connection_io (g_data.epfd, cache, conn, now_us, events);

  if (conn->state == STATE_CLOSE)
    {
      return;
    }

  if (dlist_is_linked (&conn->idle_list))
    {
      dlist_detach (&conn->idle_list);
    }

  if (is_connection_idle (conn))
    {
      conn->idle_start = now_us;
      dlist_insert_before (&g_data.idle_list, &conn->idle_list);
    }
}

static HOT void
handle_connection_event (Cache *cache, struct epoll_event *event,
			 uint64_t now_us)
{
  Conn *conn = connpool_lookup (g_data.fd2conn, event->data.fd);
  if (unlikely (!conn))
    return;
  TIME_STMT ("connection_io",
	     connection_io (cache, conn, now_us, event->events));
  if (conn->state == STATE_CLOSE)
    conn_done (conn);
}

static ALWAYS_INLINE void
process_active_events (Cache *cache, struct epoll_event *events, int count,
		       int listen_fd, uint64_t now_us)
{
  for (int i = 0; i < count; ++i)
    {
      if (events[i].data.fd == listen_fd)
	handle_listener_event (listen_fd, now_us);
      else
	handle_connection_event (cache, &events[i], now_us);
    }
}

static void COLD
cleanup_server_resources (Cache *cache, int listen_fd, int epfd)
{
  msg ("\nServer shutting down gracefully. Cleaning up resources...");

  Conn **connections = NULL;
  size_t count = 0;
  connpool_iter (g_data.fd2conn, &connections, &count);

  for (size_t i = count; i-- > 0;)
    {
      Conn *conn = connections[i];
      msgf ("Forcing cleanup of active connection: %d", conn->fd);
      conn_done (conn);
    }

  connpool_free (g_data.fd2conn);
  cache_free (cache);
  close (listen_fd);
  close (epfd);
  msg ("Cleanup complete.");
}

static bool COLD
initialize_server_core (uint16_t port, int *listen_fd, int *epfd)
{
  struct sockaddr_in addr = { 0 };
  int err, val = 1;
  struct epoll_event event;

  *listen_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (unlikely (*listen_fd < 0))
    die ("socket()");

  setsockopt (*listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

  addr.sin_family = AF_INET;
  addr.sin_port = htons (port);
  addr.sin_addr.s_addr = htonl (0);

  err = bind (*listen_fd, (const struct sockaddr *) &addr, sizeof (addr));
  if (unlikely (err))
    {
      close (*listen_fd);
      die ("bind()");
    }

  err = listen (*listen_fd, SOMAXCONN);
  if (unlikely (err))
    {
      close (*listen_fd);
      die ("listen()");
    }

  msgf ("The server is listening on port %i.", port);

  g_data.fd2conn = connpool_new (MAX_CONNECTIONS);
  fd_set_nb (*listen_fd);

  *epfd = epoll_create1 (0);
  if (unlikely (*epfd == -1))
    {
      close (*listen_fd);
      connpool_free (g_data.fd2conn);
      die ("epoll_create1");
    }

  g_data.epfd = *epfd;

  event.events = EPOLLIN | EPOLLET;
  event.data.fd = *listen_fd;

  if (unlikely (epoll_ctl (*epfd, EPOLL_CTL_ADD, *listen_fd, &event) == -1))
    {
      close (*listen_fd);
      close (*epfd);
      connpool_free (g_data.fd2conn);
      die ("epoll ctl: listen_sock!");
    }

  return true;
}

static uint32_t
next_timer_ms (Cache *cache, uint64_t now_us)
{
  uint64_t next_us = (uint64_t) - 1;

  if (!dlist_empty (&g_data.idle_list))
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Conn *next = container_of (g_data.idle_list.next, Conn, idle_list);
#pragma GCC diagnostic pop
      next_us = next->idle_start + (K_IDLE_TIMEOUT_SEC * 1000000);
    }

  uint64_t from_cache = cache_next_expiry (cache);
  if (from_cache != (uint64_t) - 1 && from_cache < next_us)
    next_us = from_cache;

  if (next_us == (uint64_t) - 1)
    return 10000;

  if (next_us <= now_us)
    return 0;

  uint32_t final_timeout = (uint32_t) ((next_us - now_us) / 1000);
  if (final_timeout == 0)
    final_timeout = 1;

  return final_timeout;
}

int
server_run (uint16_t port)
{
  struct epoll_event events[MAX_EVENTS];
  int listen_fd = -1;
  int epfd = -1;

  g_data.terminate_flag = 0;
  setup_signal_handlers ();

  dlist_init (&g_data.idle_list);
  Cache *cache = cache_init ();

  if (!initialize_server_core (port, &listen_fd, &epfd) != 0)
    return -1;

  while (!g_data.terminate_flag)
    {
      uint64_t now_us = get_monotonic_usec ();
      int timeout_ms = (int) next_timer_ms (cache, now_us);
      DBG_LOGF ("Epoll wait for %dms...", timeout_ms);

      int enfd_count = epoll_wait (epfd, events, MAX_EVENTS, timeout_ms);
      // We might have slept for quite a while!
      now_us = get_monotonic_usec ();
      if (unlikely (g_data.terminate_flag))
	break;

      if (unlikely (enfd_count < 0))
	{
	  if (errno == EINTR)
	    continue;
	  die ("epoll_wait");
	}

      DBG_LOGF ("Processing %d events.", enfd_count);

      if (enfd_count > 0)
	process_active_events (cache, events, enfd_count, listen_fd, now_us);

      process_timers (cache, now_us);
    }

  cleanup_server_resources (cache, listen_fd, epfd);
  dump_stats ();
  return 0;
}
