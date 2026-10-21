#define _GNU_SOURCE
/*
 *============================================================================
 * Name             : server_loop.c
 * Author           : Milos
 * Description      : Server loop, connection handling.
 *============================================================================
 */
#include <sys/wait.h>
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
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <linux/rds.h>

#include <assert.h>
#include <stddef.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <time.h>
#include <sys/epoll.h>
#include <signal.h>
#include <sys/eventfd.h>

#include "cache/cache.h"
#include "io/connection_handler.h"
#include "io/blocking.h"
#include "io/conn_pool.h"
#include "io/server_loop.h"
#include "io/list.h"
#include "io/zero_copy.h"
#include "common/common.h"
#include "common/macros.h"

#define MAX_EVENTS 256
#define K_IDLE_TIMEOUT_SEC 300ULL	// in seconds

static struct
{
  ConnPool *fd2conn;
  DList idle_list;
  int epfd;
  uint64_t last_snapshot_time;
  uint64_t next_snapshot_check;	// When the save rules are next checked
  pid_t snapshot_child_pid;
  uint64_t last_snapshot_mod_count;
  uint64_t last_snapshot_mod_count_at_fork;
  volatile sig_atomic_t terminate_flag;
  const char *db_dir;		// Snapshot directory
  // Written by the signal handler to wake epoll_wait, -1 until set up
  volatile sig_atomic_t wake_fd;
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

  // The flag alone is not enough: if the signal lands between the loop's
  // flag check and epoll_wait, or on a thread pool thread, the loop
  // would sleep until its timeout. The eventfd makes the wait return.
  if (g_data.wake_fd >= 0)
    {
      int saved_errno = errno;
      uint64_t one = 1;
      ssize_t ret = write (g_data.wake_fd, &one, sizeof (one));
      (void) ret;
      errno = saved_errno;
    }
}

static void
setup_signal_handlers (void)
{
  struct sigaction sa_term = { 0 };
  sa_term.sa_handler = sigint_handler;	// Sets terminate_flag = 1
  sa_term.sa_flags = 0;
  sigemptyset (&sa_term.sa_mask);

  sigaction (SIGINT, &sa_term, NULL);
  sigaction (SIGTERM, &sa_term, NULL);
  sigaction (SIGQUIT, &sa_term, NULL);
  sigaction (SIGHUP, &sa_term, NULL);	// Terminal closed

  struct sigaction sa_ign = { 0 };
  sa_ign.sa_handler = SIG_IGN;
  sa_ign.sa_flags = 0;
  sigemptyset (&sa_ign.sa_mask);

  sigaction (SIGPIPE, &sa_ign, NULL);

  // Background snapshots are waited for. An ignored SIGCHLD, which the
  // daemon setup leaves behind and a parent can pass down, would reap
  // them first: waitpid then fails, the snapshot never counts as done
  // and the same changes are saved again at every check.
  struct sigaction sa_dfl = { 0 };
  sa_dfl.sa_handler = SIG_DFL;
  sigemptyset (&sa_dfl.sa_mask);
  sigaction (SIGCHLD, &sa_dfl, NULL);
}

static void
rdb_save_background (Cache *cache, uint64_t now_us)
{
  if (g_data.snapshot_child_pid != 0)
    return;			// Already saving

  g_data.last_snapshot_time = now_us;
  g_data.last_snapshot_mod_count_at_fork = cache_dirty_count (cache);

  pid_t child_pid = fork ();

  if (child_pid == -1)
    {
      msgf ("Error: fork() failed: %s", strerror (errno));
      return;
    }

  if (child_pid == 0)
    {
      bool status = cache_save (cache, g_data.db_dir, now_us);
      msgf ("Background saving terminated with success in %" PRIu64 " ms.",
	    (get_monotonic_usec () - g_data.last_snapshot_time) / 1000);
      _exit (status ? 0 : 1);
    }
  // PARENT PROCESS
  g_data.snapshot_child_pid = child_pid;
  msgf ("Background saving started by pid %d", child_pid);
}

static void
check_snapshot_child_status (void)
{
  if (g_data.snapshot_child_pid == 0)
    return;

  int status;
  // WNOHANG is crucial: it checks status but doesn't block if child is still running
  pid_t res = waitpid (g_data.snapshot_child_pid, &status, WNOHANG);

  if (res == g_data.snapshot_child_pid)
    {
      // Child finished
      if (WIFEXITED (status) && WEXITSTATUS (status) == 0)
	{
	  g_data.last_snapshot_mod_count =
	    g_data.last_snapshot_mod_count_at_fork;
	}
      else
	{
	  msgf ("Background saving error");
	}
      g_data.snapshot_child_pid = 0;
    }
  else if (res == -1)
    {
      msgf ("waitpid error: %s", strerror (errno));
      g_data.snapshot_child_pid = 0;
    }
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

      if (unlikely (connfd < 0))
	{
	  if (errno == EAGAIN)
	    break;

	  msgf ("accept() error: %s", strerror (errno));
	  break;
	}

      int sndbuf = 2 * 1024 * 1024;
      setsockopt (connfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof (sndbuf));

      int val = 1;
      if (unlikely
	  (setsockopt (connfd, SOL_SOCKET, SO_ZEROCOPY, &val, sizeof (val))))
	{
	  msgf ("SO_ZEROCOPY not set. error: %s", strerror (errno));
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

  blocking_unpark (conn);
  conn_drain_forget (conn);

  zc_drain_errors (file_desc);
  close (file_desc);
  connpool_release (g_data.fd2conn, conn);
  // The Conn struct lives in the Slab and is recycled.
}

static Conn*
next_idle_connection (void)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  Conn *next = container_of (g_data.idle_list.next, Conn, idle_list);
#pragma GCC diagnostic pop
  return next;
}

static void
process_timers (Cache *cache, uint64_t now_us)
{
  while (!dlist_empty (&g_data.idle_list))
    {
      Conn *next = next_idle_connection ();
      uint64_t next_us = next->idle_start + (K_IDLE_TIMEOUT_SEC * 1000000);

      if (next_us > now_us)
	break;

      if (conn_has_pending_write (next))
	{
	  next->idle_start = now_us;

	  dlist_detach (&next->idle_list);
	  dlist_insert_before (&g_data.idle_list, &next->idle_list);

	  DBG_LOGF
	    ("FD %d: Pending I/O. Resetting idle timer and skipping close.",
	     next->fd);
	  continue;
	}
      msgf ("Removing idle connection: %d", next->fd);
      conn_done (next);
    }

  // Clients that never closed after an error
  Conn *drained;
  while ((drained = conn_drain_pop_expired (now_us)) != NULL)
    conn_done (drained);

  cache_evict (cache, now_us);
}

static size_t sys_calls = 0;

// Helper: Commit state changes to Kernel (Batch MOD)
static NOINLINE void
flush_dirty_conns (int epfd, Conn **conns, int count)
{
  for (int i = 0; i < count; i++)
    {
      Conn *conn = conns[i];
      struct epoll_event event;

      event.data.fd = conn->fd;
      event.events = conn->pending_events;
      sys_calls++;
      (void) sys_calls;
      DBG_LOGF ("Calling CTL_MOD on fd: %i for event: %u, call number: %zu",
		conn->fd, conn->pending_events, sys_calls);
      epoll_ctl (epfd, EPOLL_CTL_MOD, conn->fd, &event);
      conn->last_events = conn->pending_events;
    }
}

// Helper: Cleanup dead connections (Batch DELETE)
static NOINLINE void
flush_dead_conns (Conn **conns, int count)
{
  for (int i = 0; i < count; i++)
    {
      conn_done (conns[i]);
    }
}

// Helper: Manage Idle List State
static ALWAYS_INLINE void
update_idle_state (Conn *conn, uint64_t now_us)
{
  // If we touched it, it's active. Remove from idle list.
  if (dlist_is_linked (&conn->idle_list))
    {
      dlist_detach (&conn->idle_list);
    }

  // If it's effectively waiting for a new request, mark it idle.
  if (conn_is_idle (conn))
    {
      conn->idle_start = now_us;
      dlist_insert_before (&g_data.idle_list, &conn->idle_list);
    }
}


// Clients that failed while being served for BLPOP. They are closed after
// the event batch, so that no batch entry points at a released Conn.
static Conn *g_close_later[MAX_CONNECTIONS];
static size_t g_close_later_count;

static void
close_later (Conn *conn)
{
  if (g_close_later_count < MAX_CONNECTIONS)
    g_close_later[g_close_later_count++] = conn;
}

static void
flush_close_later (void)
{
  for (size_t i = 0; i < g_close_later_count; i++)
    conn_done (g_close_later[i]);
  g_close_later_count = 0;
}

// Runs a waiting client's buffered requests again, its BLPOP first, as
// if it had become readable
static void
serve_one (Cache *cache, Conn *conn, uint64_t now_us)
{
  if (conn->state != STATE_ACTIVE)
    return;

  handle_connection_io (cache, conn, now_us, IO_EVENT_READ);
  if (conn->state == STATE_CLOSE)
    {
      close_later (conn);
      return;
    }

  update_idle_state (conn, now_us);
  if (conn->pending_events != conn->last_events)
    flush_dirty_conns (g_data.epfd, &conn, 1);
}

// Serves waiting clients oldest first: all of them after a push, or with
// only_expired those whose timeout is over. Served clients may push in
// turn, so go on until no push happens.
static void
serve_blocked (Cache *cache, uint64_t now_us, bool only_expired)
{
  blocking_set_serving (true);
  uint64_t pushes;
  do
    {
      pushes = cache_list_pushes (cache);
      size_t count = blocking_count ();
      for (size_t i = 0; i < count; i++)
	{
	  Conn *conn = blocking_rotate ();
	  if (!conn)
	    break;
	  if (only_expired && (conn->block_deadline_us == 0
			       || conn->block_deadline_us > now_us))
	    continue;
	  serve_one (cache, conn, now_us);
	}
      only_expired = false;
    }
  while (blocking_count () > 0 && cache_list_pushes (cache) != pushes);
  blocking_set_serving (false);
}

static HOT ALWAYS_INLINE void
process_active_events (Cache *cache, struct epoll_event *events, int count,
		       int listen_fd, uint64_t now_us)
{
  Conn *dirty_conns[MAX_EVENTS];
  int dirty_count = 0;

  Conn *dead_conns[MAX_EVENTS];
  int dead_count = 0;

  for (int i = 0; i < count; ++i)
    {
      int file_desc = events[i].data.fd;
      if (file_desc == listen_fd)
	{
	  handle_listener_event (listen_fd, now_us);
	  continue;
	}
      if (unlikely (file_desc == g_data.wake_fd))
	{
	  // Only wakes the loop, terminate_flag says why
	  uint64_t wakeups;
	  ssize_t ret = read (file_desc, &wakeups, sizeof (wakeups));
	  (void) ret;
	  continue;
	}
      Conn *conn = connpool_lookup (g_data.fd2conn, file_desc);
      // Closed while being served for BLPOP, it waits in close_later
      if (unlikely (!conn || conn->state == STATE_CLOSE))
	continue;

      TIME_STMT ("connection_io",
		 handle_connection_io (cache, conn, now_us,
				       events[i].events));

      // It pushed with clients waiting: they go first, then the rest of
      // its requests
      while (unlikely (conn->yield))
	{
	  conn->yield = false;
	  serve_blocked (cache, now_us, false);
	  if (conn->state != STATE_ACTIVE)
	    break;
	  handle_connection_io (cache, conn, now_us, IO_EVENT_READ);
	}
      if (unlikely (conn->state == STATE_CLOSE))
	{
	  dead_conns[dead_count++] = conn;
	}
      else
	{
	  update_idle_state (conn, now_us);
	  if (conn->pending_events != conn->last_events)
	    {
	      dirty_conns[dirty_count++] = conn;
	    }
	}
    }

  if (dirty_count > 0)
    flush_dirty_conns (g_data.epfd, dirty_conns, dirty_count);

  if (unlikely (dead_count > 0))
    flush_dead_conns (dead_conns, dead_count);

  flush_close_later ();
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
  if (g_data.wake_fd >= 0)
    {
      close (g_data.wake_fd);
      g_data.wake_fd = -1;
    }
  msgf ("Cleanup complete. %zu calls to epoll_ctl", sys_calls);
}

static bool COLD
initialize_server_core (int port, const char *bind_addr, int *listen_fd,
			int *epfd)
{
  struct sockaddr_in addr = { 0 };
  int err, val = 1;
  struct epoll_event event;

  *listen_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (unlikely (*listen_fd < 0))
    die ("socket()");

  setsockopt (*listen_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

  addr.sin_family = AF_INET;
  addr.sin_port = htons ((__uint16_t) port);
  addr.sin_addr.s_addr = htonl (INADDR_ANY);
  if (bind_addr && inet_pton (AF_INET, bind_addr, &addr.sin_addr) != 1)
    {
      close (*listen_fd);
      msgf ("Invalid bind address: %s", bind_addr);
      die ("inet_pton()");
    }

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

  msgf ("The server is listening on %s port %i.",
	bind_addr ? bind_addr : "all addresses,", port);

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

  int wake_fd = eventfd (0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (unlikely (wake_fd == -1))
    die ("eventfd");
  event.events = EPOLLIN;
  event.data.fd = wake_fd;
  if (unlikely (epoll_ctl (*epfd, EPOLL_CTL_ADD, wake_fd, &event) == -1))
    die ("epoll ctl: wake fd");
  g_data.wake_fd = wake_fd;

  event.events = EPOLLIN | EPOLLET;
  event.data.fd = *listen_fd;

  if (unlikely (epoll_ctl (*epfd, EPOLL_CTL_ADD, *listen_fd, &event) == -1))
    {
      close (*listen_fd);
      close (*epfd);
      connpool_free (g_data.fd2conn);
      die ("epoll ctl: listen_sock!");
    }
  // No need to make a snapshot immidiatelly afer we start!
  g_data.last_snapshot_time = get_monotonic_usec ();
  g_data.next_snapshot_check =
    g_data.last_snapshot_time + SNAPSHOT_INTERVAL_US;
  return true;
}

static uint32_t
next_timer_ms (Cache *cache, uint64_t now_us)
{
  uint64_t next_us = (uint64_t) - 1;

  if (!dlist_empty (&g_data.idle_list))
    {
      Conn *next = next_idle_connection ();
      next_us = next->idle_start + (K_IDLE_TIMEOUT_SEC * 1000000);
    }

  uint64_t from_cache = cache_next_expiry (cache);
  if (from_cache != (uint64_t) - 1 && from_cache < next_us)
    next_us = from_cache;

  uint64_t block_deadline = blocking_next_deadline ();
  if (block_deadline < next_us)
    next_us = block_deadline;

  uint64_t drain_deadline = conn_drain_next_deadline ();
  if (drain_deadline < next_us)
    next_us = drain_deadline;
  // Only schedule if interval is set and no child is currently running
  if (SNAPSHOT_INTERVAL_US > 0 && g_data.snapshot_child_pid == 0)
    {
      if (g_data.next_snapshot_check < next_us)
	{
	  next_us = g_data.next_snapshot_check;
	}
    }

  if (next_us == (uint64_t) - 1)
    return 10000;

  if (next_us <= now_us)
    return 0;

  uint32_t final_timeout = (uint32_t) ((next_us - now_us) / 1000);
  if (final_timeout == 0)
    final_timeout = 1;

  return final_timeout;
}

static Cache *
init_cache (uint64_t now_us)
{
  Cache *cache = cache_init ();

  if (cache_load (cache, g_data.db_dir, now_us))
    {
      msgf ("[Minis] DB loaded from directory %s in %" PRIu64 " ms.",
	    g_data.db_dir,
	    (get_monotonic_usec () - now_us) / 1000);
    }
  else
    {
      if (errno != ENOENT)
	{
	  msgf ("[Minis] Warning: Failed to load %s (Starting empty).",
		g_data.db_dir);
	}
      else
	{
	  msgf ("[Minis] No existing DB found in %s. Starting fresh.",
		g_data.db_dir);
	}
    }
  return cache;
}

static void
process_snapshots (Cache *cache, uint64_t now_us)
{
  check_snapshot_child_status ();

  if (g_data.snapshot_child_pid != 0)
    return;

  // Check the rules every SNAPSHOT_INTERVAL_US. The check time must move
  // on even when no snapshot is taken, or once it is due every epoll
  // wait gets a zero timeout and the idle server spins.
  if (now_us < g_data.next_snapshot_check)
    return;
  g_data.next_snapshot_check = now_us + SNAPSHOT_INTERVAL_US;

  uint64_t dirty_diff =
    cache_dirty_count (cache) - g_data.last_snapshot_mod_count;
  uint64_t time_diff_sec = (now_us - g_data.last_snapshot_time) / 1000000;

  // Optimization: If nothing changed, don't check rules
  if (dirty_diff == 0)
    return;

  // Check Rules
  bool trigger = false;
  int num_rules = sizeof (save_params) / sizeof (SaveParam);

  for (int i = 0; i < num_rules; i++)
    {
      if (time_diff_sec >= save_params[i].seconds &&
	  dirty_diff >= save_params[i].changes)
	{
	  msgf
	    ("Snapshot triggered: rule %" PRIu64 "/%" PRIu64 " with %" PRIu64
	     " changes in %" PRIu64 " seconds",
	     save_params[i].changes, save_params[i].seconds, dirty_diff,
	     time_diff_sec);
	  trigger = true;
	  break;
	}
    }

  if (trigger)
    {
      rdb_save_background (cache, now_us);
    }
}

static void
save_sync (Cache *cache)
{
  if (g_data.snapshot_child_pid > 0)
    {
      msgf ("Killing active snapshot child %d to perform sync save...",
	    g_data.snapshot_child_pid);
      kill (g_data.snapshot_child_pid, SIGKILL);
      int status;
      waitpid (g_data.snapshot_child_pid, &status, 0);

      g_data.snapshot_child_pid = 0;
    }
  msg ("Saving DB to disk (Synchronous)...");
  uint64_t now_us = get_monotonic_usec ();
  if (cache_save (cache, g_data.db_dir, now_us))
    {
      msg ("DB saved successfully.");
    }
  else
    {
      msg ("ERROR: Failed to save DB on shutdown!");
    }
}

static void
server_shutdown (Cache *cache, int listen_fd, int epfd)
{
  save_sync (cache);
  dump_stats ();
  cleanup_server_resources (cache, listen_fd, epfd);
}

int
server_run (int port, const char *bind_addr, const char *db_dir,
	    int ready_fd)
{
  g_data.db_dir = db_dir;
  struct epoll_event events[MAX_EVENTS];
  int listen_fd = -1;
  int epfd = -1;

  g_data.terminate_flag = 0;
  g_data.wake_fd = -1;
  setup_signal_handlers ();

  dlist_init (&g_data.idle_list);
  Cache *cache = init_cache (get_monotonic_usec ());

  if (!initialize_server_core (port, bind_addr, &listen_fd, &epfd) != 0)
    return -1;

  // Loaded and listening: tell whoever waits on the daemon
  if (ready_fd >= 0)
    {
      ssize_t ret = write (ready_fd, "1", 1);
      (void) ret;
      close (ready_fd);
    }

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

      process_snapshots (cache, now_us);
      process_timers (cache, now_us);

      if (blocking_next_deadline () <= now_us)
	{
	  serve_blocked (cache, now_us, true);
	  flush_close_later ();
	}
    }
  server_shutdown (cache, listen_fd, epfd);
  return 0;
}
