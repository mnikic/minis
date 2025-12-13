#define _GNU_SOURCE
/*
 *============================================================================
 * Name             : server_loop.c
 * Author           : Milos
 * Description      : Server loop, connection handling.
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
#include <signal.h>

#include "cache/cache.h"
#include "connection_handler.h"
#include "connections.h"
#include "common/common.h"
#include "server_loop.h"
#include "list.h"

#define MAX_EVENTS 10000
#define MAX_CHUNKS 16
#define K_IDLE_TIMEOUT_US 5000000

static struct
{
  ConnPool *fd2conn;
  DList idle_list;
  int epfd;
  volatile sig_atomic_t terminate_flag;
} g_data;

static inline uint64_t
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
    die ("fcntl error");

  flags |= O_NONBLOCK;

  errno = 0;
  (void) fcntl (file_des, F_SETFL, flags);
  if (errno)
    die ("fcntl error");
}

static int32_t
accept_new_conn (int file_des, uint64_t now_us)
{
  struct sockaddr_in client_addr = { 0 };
  socklen_t socklen = sizeof (client_addr);
  int connfd;

#if defined(__linux__) && defined(__GLIBC__) && (__GLIBC__ >= 2) && (__GLIBC_MINOR__ >= 10)
  connfd = accept4 (file_des, (struct sockaddr *) &client_addr, &socklen,
		    SOCK_NONBLOCK);
#else
  connfd = accept (file_des, (struct sockaddr *) &client_addr, &socklen);
  if (connfd >= 0)
    fd_set_nb (connfd);
#endif

  if (connfd < 0)
    {
      if (errno == EAGAIN)
	{
	  DBG_LOG ("No more connections to accept (EAGAIN).");
	  return -1;
	}
      msgf ("accept() error: %s", strerror (errno));
      return -2;
    }

  Conn *conn = calloc (1, sizeof (Conn));
  if (!conn)
    {
      close (connfd);
      die ("Out of memory for connection");
    }

  conn->fd = connfd;
  conn->state = STATE_REQ;
  conn->rbuf_size = 0;
  conn->write_idx = 0;
  conn->read_idx = 0;
  conn->last_events = 0;
  conn->idle_start = now_us;
  dlist_insert_before (&g_data.idle_list, &conn->idle_list);

  connpool_add (g_data.fd2conn, conn);
  DBG_LOGF ("Accepted new connection: FD %d", connfd);

  return connfd;
}

static inline void
conn_done (Conn *conn)
{
  DBG_LOGF ("Cleaning up and closing connection: FD %d", conn->fd);
  (void) epoll_ctl (g_data.epfd, EPOLL_CTL_DEL, conn->fd, NULL);
  connpool_remove (g_data.fd2conn, conn->fd);
  close (conn->fd);
  dlist_detach (&conn->idle_list);
  free (conn);
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

      uint64_t next_us = next->idle_start + K_IDLE_TIMEOUT_US;

      if (next_us > now_us)
	break;

      msgf ("Removing idle connection: %d", next->fd);
      conn_done (next);
    }

  cache_evict (cache, now_us);
}

static void
connection_io (Cache *cache, Conn *conn, uint64_t now_us)
{
  conn->idle_start = now_us;
  dlist_detach (&conn->idle_list);
  dlist_insert_before (&g_data.idle_list, &conn->idle_list);

  handle_connection_io (g_data.epfd, cache, conn, now_us);
}

static void
handle_listener_event (int listen_fd, uint64_t now_us)
{
  int epfd = g_data.epfd;

  while (true)
    {
      int conn_fd = accept_new_conn (listen_fd, now_us);

      if (conn_fd < 0)
	{
	  if (conn_fd == -1)
	    break;
	  break;
	}

      struct epoll_event event;
      event.data.fd = conn_fd;
      event.events = EPOLLIN | EPOLLET;

      if (epoll_ctl (epfd, EPOLL_CTL_ADD, event.data.fd, &event) == -1)
	{
	  msgf ("epoll ctl: new conn registration failed: %s",
		strerror (errno));
	  close (conn_fd);
	}
    }
}

static void
handle_connection_event (Cache *cache, struct epoll_event *event,
			 uint64_t now_us)
{
  Conn *conn = connpool_lookup (g_data.fd2conn, event->data.fd);
  if (!conn)
    return;

  DBG_LOGF ("FD %d: Handling epoll event (events: 0x%x). State: %d",
	    event->data.fd, event->events, conn->state);

  if (event->events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP))
    {
      DBG_LOGF ("FD %d: Hangup/Error detected (0x%x). Closing.",
		event->data.fd, event->events);
      conn->state = STATE_END;
      conn_done (conn);
      return;
    }

  if (event->events & (EPOLLIN | EPOLLOUT))
    TIME_STMT ("connection_io", connection_io (cache, conn, now_us));

  if (conn->state == STATE_END)
    conn_done (conn);
}

static void
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

static void
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

static int
initialize_server_core (uint16_t port, int *listen_fd, int *epfd)
{
  struct sockaddr_in addr = { 0 };
  int err, val = 1;
  struct epoll_event event;

  *listen_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (*listen_fd < 0)
    die ("socket()");

  setsockopt (*listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

  addr.sin_family = AF_INET;
  addr.sin_port = htons (port);
  addr.sin_addr.s_addr = htonl (0);

  err = bind (*listen_fd, (const struct sockaddr *) &addr, sizeof (addr));
  if (err)
    {
      close (*listen_fd);
      die ("bind()");
    }

  err = listen (*listen_fd, SOMAXCONN);
  if (err)
    {
      close (*listen_fd);
      die ("listen()");
    }

  msgf ("The server is listening on port %i.", port);

  g_data.fd2conn = connpool_new (10);
  fd_set_nb (*listen_fd);

  *epfd = epoll_create1 (0);
  if (*epfd == -1)
    {
      close (*listen_fd);
      connpool_free (g_data.fd2conn);
      die ("epoll_create1");
    }

  g_data.epfd = *epfd;

  event.events = EPOLLIN | EPOLLET;
  event.data.fd = *listen_fd;

  if (epoll_ctl (*epfd, EPOLL_CTL_ADD, *listen_fd, &event) == -1)
    {
      close (*listen_fd);
      close (*epfd);
      connpool_free (g_data.fd2conn);
      die ("epoll ctl: listen_sock!");
    }

  return 0;
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
      next_us = next->idle_start + K_IDLE_TIMEOUT_US;
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

  if (initialize_server_core (port, &listen_fd, &epfd) != 0)
    return -1;

  while (!g_data.terminate_flag)
    {
      uint64_t now_us = get_monotonic_usec ();
      int timeout_ms = (int) next_timer_ms (cache, now_us);
      DBG_LOGF ("Epoll wait for %dms...", timeout_ms);

      int enfd_count = epoll_wait (epfd, events, MAX_EVENTS, timeout_ms);
      // We might have slept for quite a while!
      now_us = get_monotonic_usec ();
      if (g_data.terminate_flag)
	break;

      if (enfd_count < 0)
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
