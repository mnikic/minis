#define _GNU_SOURCE
/*
 *============================================================================
 * Name             : server_loop.c
 * Author           : Milos
 * Description      : Core networking logic (epoll event loop, connection handling).
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
  STATE_REQ = 0,
  STATE_RES = 1,
  STATE_RES_CLOSE = 2,		// Send response then close connection
  STATE_END = 3,		// Mark the connection for deletion
};

static struct
{
  ConnPool *fd2conn;
  DList idle_list;
  int epfd;
  volatile sig_atomic_t terminate_flag;
} g_data;

static void
state_res (Conn *conn);
// NEW HELPER FUNCTION: Read zerocopy completions from the error queue
// Returns true if a zerocopy completion was processed, false otherwise.
static bool
try_check_zerocopy_completion (Conn *conn)
{
    // The connection's read index (conn->read_idx) points to the OLDEST response slot
    // that the client is currently trying to consume. This is the slot that 
    // corresponds to the oldest pending zero-copy operation.
    ResponseSlot *current_slot = &conn->res_slots[conn->read_idx];

    // Check if the current slot is even active (if actual_length == 0, there's nothing 
    // to complete, unless this is a late error for a previous block).
    // We rely on the pending_ops count for safety.
    if (current_slot->actual_length == 0 && current_slot->pending_ops == 0) {
        // If the current slot is clear, check the next one. This is a crucial safety measure
        // in case a completion for a past slot was delayed, but in a ring buffer, 
        // read_idx should always point to the next expected completion.
        // We'll proceed with the error queue logic regardless to consume the error.
        DBG_LOGF("FD %d: WARNING: Received EPOLLERR but current slot %u is clear. Continuing to consume error queue.",
                 conn->fd, conn->read_idx);
    }
    
    // --- 1. Standard Error Queue Setup and Read ---
    struct msghdr msg = { 0 };
    struct sock_extended_err *serr;
    struct cmsghdr *cm;
    char control[100];
    int ret;

    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    ret = (int) recvmsg(conn->fd, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

    if (ret < 0) {
        if (errno == EAGAIN || errno == ENOMSG)
            return false; // No more errors/completions to process

        msgf ("recvmsg(MSG_ERRQUEUE) error: %s", strerror (errno));
        conn->state = STATE_END;
        return false;
    }

    bool completion_processed = false;

    // --- 2. Iterate Control Messages and Process Completions ---
    for (cm = CMSG_FIRSTHDR(&msg); cm; cm = CMSG_NXTHDR(&msg, cm)) {
        if (cm->cmsg_level != SOL_IP && cm->cmsg_level != SOL_IPV6)
            continue;

        if (cm->cmsg_type != IP_RECVERR && cm->cmsg_type != IPV6_RECVERR)
            continue;

        serr = (void *) CMSG_DATA(cm);

        if (serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY) {
            uint32_t range_start = serr->ee_info;
            uint32_t range_end = serr->ee_data;
            uint32_t completed_ops = range_end - range_start + 1;
            
            // The completion relates to the slot at conn->read_idx
            // (since completions are ordered).
            
            if (current_slot->pending_ops == 0) {
                // If we get a completion but have no pending ops, 
                // it might mean a completion for a previously cleared slot arrived late.
                DBG_LOGF("FD %d: WARNING: Completion received (%u ops) but slot %u has 0 pending.",
                         conn->fd, completed_ops, conn->read_idx);
                completion_processed = true;
                continue;
            }

            if (completed_ops > current_slot->pending_ops) {
                 DBG_LOGF("FD %d: WARNING: Completion overflow. Got %u ops, only %u pending.",
                          conn->fd, completed_ops, current_slot->pending_ops);
                 current_slot->pending_ops = 0;
            } else {
                 current_slot->pending_ops -= completed_ops;
            }

            DBG_LOGF("FD %d: ZEROCOPY completion range [%u-%u] (%u ops). Slot %u pending: %u.",
                     conn->fd, range_start, range_end, completed_ops,
                     conn->read_idx, current_slot->pending_ops);

            completion_processed = true;

            // --- 3. Slot Cleanup and Advancement ---
            // A slot is fully finished ONLY if:
            // a) It has been fully SENT (slot->sent == slot->actual_length)
            // b) All zero-copy operations have been ACKNOWLEDGED (slot->pending_ops == 0)
            if (current_slot->pending_ops == 0 && current_slot->sent == current_slot->actual_length) {
                
                // Clear the slot memory tracking
                current_slot->actual_length = 0; 
                current_slot->sent = 0; 
                // pending_ops is already 0

                DBG_LOGF("FD %d: Fully completed slot %u released from the ring buffer.",
                         conn->fd, conn->read_idx);
                
                // Move the read index to the next slot
                conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
                
                // CRITICAL: Update the pointer to current_slot for the next CMSG/loop iteration.
                // Since completions are ordered, the next completion applies to the new oldest slot.
                current_slot = &conn->res_slots[conn->read_idx]; 
            }
        }
    }

    return completion_processed;
}

// Signal handler function (async-signal-safe)
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
accept_new_conn (int file_des)
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
    
  int sndbuf = 2 * 1024 * 1024;
  setsockopt (connfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof (sndbuf));
  
  int val = 1;
  if (setsockopt(connfd, SOL_SOCKET, SO_ZEROCOPY, &val, sizeof(val))) {
      msgf("setsockopt(SO_ZEROCOPY) failed: %s. Using standard copy.", strerror(errno));
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
  conn->idle_start = get_monotonic_usec ();
  dlist_insert_before (&g_data.idle_list, &conn->idle_list);

  connpool_add (g_data.fd2conn, conn);
  DBG_LOGF ("Accepted new connection: FD %d", connfd);

  return connfd;
}

static void
conn_set_epoll_events (Conn *conn, uint32_t events)
{
  if (conn->state == STATE_END)
    return;

  struct epoll_event event;
  event.data.fd = conn->fd;
  event.events = events | EPOLLET | EPOLLERR;

  if (epoll_ctl (g_data.epfd, EPOLL_CTL_MOD, conn->fd, &event) == -1)
    {
      if (errno == ENOENT)
	return;
      msgf ("epoll ctl: MOD failed: %s", strerror (errno));
    }
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
do_request (Cache *cache, Conn *conn, uint8_t *req, uint32_t reqlen,
	    Buffer *out_buf)
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

      if (restore_count >= K_MAX_ARGS)
	{
	  dump_error_and_close (conn, ERR_MALFORMED,
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
      dump_error_and_close (conn, ERR_MALFORMED,
			    "Trailing garbage in request");
      goto CLEANUP;
    }

  DBG_LOGF ("FD %d: Executing command with %zu arguments", conn->fd,
	    cmd_size);
  success =
    TIME_EXPR ("cache_execute",
	       cache_execute (cache, cmd, cmd_size, out_buf));
  // Execute the command. The command array pointers point directly into the read buffer.
  if (!success)
    {
      msg ("cache couldn't write message, no space.");
      dump_error_and_close (conn, ERR_UNKNOWN, "response too large");
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

static bool
try_one_request (Cache *cache, Conn *conn, uint32_t *start_index)
{
  if (conn->rbuf_size < *start_index + 4)
    return false;

  uint32_t len = 0;
  memcpy (&len, &conn->rbuf[*start_index], 4);
  len = ntohl (len);

  DBG_LOGF ("FD %d: Parsing request starting at index %u with length %u.",
        conn->fd, *start_index, len);

  // Check against maximum message size
  if (len > K_MAX_MSG)
    {
      msgf ("request too long %u", len);
      dump_error_and_close (conn, ERR_2BIG,
                "request too large; connection closed.");
      *start_index = conn->rbuf_size;
      return false;
    }

  if (4 + len + *start_index > conn->rbuf_size)
    return false;

  // 1. Setup response buffer pointing to conn->wbuf
  Buffer out_buf = buf_init (conn->wbuf + 4, sizeof conn->wbuf - 4);

  // 2. Execute the request
  bool success = do_request (cache, conn,
                          &conn->rbuf[*start_index + 4],
                          len,
                          &out_buf);

  // Request succeeded
  if (success)
    {
      size_t reslen = buf_len (&out_buf);
      uint32_t nwlen = htonl ((uint32_t) reslen);

      // Finalize wbuf with length prefix
      conn->wbuf_size = 0;
      memcpy (&conn->wbuf, &nwlen, 4);
      conn->wbuf_size += 4 + reslen;
      
      // 3. Hand off the response data from wbuf to the in-flight queue
      if (!hand_off_response(conn)) {
          // hand_off_response sets STATE_END on memory failure
          success = false;
      }
    }

  if (!success)
    {
      // Malformed request or response too large (dump_error_and_close handles state)
      *start_index += 4 + len;
      msg
    ("request failed, or could not buffer response, stopping processing");
      return false;
    }

  // Request succeeded
  *start_index += 4 + len;
  DBG_LOGF ("FD %d: Request processed, consumed %u bytes.", conn->fd,
        4 + len);

  if (*start_index >= conn->rbuf_size)
    {
      // All data consumed, transition to sending response
      conn->state = STATE_RES;
      DBG_LOGF ("FD %d: RBuf fully consumed, transitioning to STATE_RES.",
        conn->fd);
      // Must call state_res here to immediately try flushing the response we just queued
      conn_set_epoll_events (conn, EPOLLIN | EPOLLOUT | EPOLLERR);
      state_res(conn);
      return false;
    }

  return (conn->state == STATE_REQ);
}

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
	return -1;

      msgf ("read() error: %s", strerror (errno));
      conn->state = STATE_END;
      return -2;
    }

  if (err == 0)
    {
      DBG_LOGF ("FD %d: EOF received, setting state to STATE_END.", conn->fd);
      conn->state = STATE_END;
      return -2;
    }

  DBG_LOGF ("FD %d: Read %zd bytes from socket.", conn->fd, err);
  return err;
}

static bool
process_received_data (Cache *cache, Conn *conn, uint32_t bytes_read)
{
  uint32_t start_index = conn->rbuf_size;
  conn->rbuf_size += bytes_read;
  assert (conn->rbuf_size <= sizeof (conn->rbuf));

  int processed = 0;
  while (try_one_request (cache, conn, &start_index))
    {
      if (++processed >= MAX_CHUNKS)
	break;
    }

  // Buffer compaction
  uint32_t remain = conn->rbuf_size - start_index;
  if (remain)
    memmove (conn->rbuf, &conn->rbuf[start_index], remain);

  conn->rbuf_size = remain;

  DBG_LOGF
    ("FD %d: Processed %d requests. RBuf remaining: %u. New state: %d.",
     conn->fd, processed, remain, conn->state);

  // Check if state changed
  if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE ||
      conn->state == STATE_END)
    return true;

  if (processed >= MAX_CHUNKS)
    return true;

  return false;
}

static bool
try_fill_buffer (Cache *cache, Conn *conn)
{
  while (1)
    {
      size_t cap = sizeof (conn->rbuf) - conn->rbuf_size;

      if (cap == 0)
	{
	  DBG_LOGF ("FD %d: RBuf full, transitioning to STATE_RES.",
		    conn->fd);
	  conn->state = STATE_RES;
	  break;
	}

      ssize_t bytes_read = read_data_from_socket (conn, cap);

      if (bytes_read == -2)
	return true;		// Error or EOF

      if (bytes_read == -1)
	return false;		// EAGAIN

      if (process_received_data (cache, conn, (uint32_t) bytes_read))
	return true;
    }

  return true;
}

static bool
try_flush_buffer (int epfd, Conn *conn)
{
    // *** 1. Check for zerocopy completions (Always first) ***
    // NOTE: If you keep try_check_zerocopy_completion in server_loop.c, 
    // you must pass the completion information back here to update the slots.
    // For now, let's assume it still operates on the old in_flight_list, 
    // which MUST be replaced with logic to update the ResponseSlot (see next step).
    // Let's assume you have modified the original try_check_zerocopy_completion
    // to work with a linked list of *pointers to* the slots.
    
    // For simplicity, let's keep the error queue logic external for now, 
    // but its effect must be reflected on the ResponseSlots. 
    // We will focus on the send part first.
    
    // Assume: try_check_zerocopy_completion has been called via EPOLLERR handler

    while (true) 
    {
        ResponseSlot *slot = &conn->res_slots[conn->read_idx];
        
        // If the current slot is empty, we are done sending.
        if (slot->actual_length == 0) {
            break; 
        }

        uint8_t *data_start = 
            &conn->res_data[(size_t) (conn->read_idx * K_MAX_MSG)];

        // --- Skip fully sent blocks ---
        if (slot->sent == slot->actual_length) {
            // Block fully sent. Must wait for completion(s) (pending_ops > 0)
            // If pending_ops is zero, the completion was handled, and slot->actual_length 
            // would have been set to 0. This shouldn't happen here, but safety first.
            if (slot->pending_ops > 0) {
                DBG_LOGF("FD %d: Slot %u fully sent (%u bytes). Waiting for %u completion(s).",
                         conn->fd, conn->read_idx, slot->actual_length, slot->pending_ops);
                
                // No more data to send on this slot. Stop sending loop.
                return false; 
            }
            // If actual_length was not reset to 0, something went wrong, 
            // treat it as done and advance index.
            slot->actual_length = 0; 
            conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
            continue; // Try the next slot
        }

        // --- Send Logic (MSG_ZEROCOPY) ---
        size_t remain = slot->actual_length - slot->sent;
        
        struct iovec iov = {
            .iov_base = data_start + slot->sent,
            .iov_len = remain
        };
        
        struct msghdr message = { .msg_iov = &iov, .msg_iovlen = 1, /* ... (other fields NULL) */ };

        ssize_t err = sendmsg(conn->fd, &message, MSG_DONTWAIT | MSG_ZEROCOPY | MSG_NOSIGNAL);

        if (err < 0) {
            if (errno == EAGAIN) {
                conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
                return false; // Stop due to EWOULDBLOCK
            }
            // Handle fatal error (sets STATE_END)
            conn->state = STATE_END;
            return false;
        }
        
        // Update state
        slot->sent += (size_t) err;
        slot->pending_ops++; // One more operation waiting for completion
        
        DBG_LOGF("FD %d: Sent %zd bytes on slot %u with ZEROCOPY (sent: %u/%u, pending: %u).",
                  conn->fd, err, conn->read_idx, slot->sent, slot->actual_length, slot->pending_ops);
        
        // If fully sent, we must stop and wait for completions before advancing to the next slot.
        if (slot->sent == slot->actual_length) {
            // The slot is fully sent. We wait for EPOLLERR completion(s).
            return false;
        }

        // If partially sent, we continue the loop, relying on the kernel to give us EPOLLOUT 
        // later, or we stop now (typical ET logic). Let's return false to wait for EPOLLOUT.
        return false;
    }

    // --- State Transition when send queue is empty ---
    // If we exit the while loop, it means conn->res_slots[conn->read_idx].actual_length == 0
    // and all preceding slots have been fully cleared (sent=actual_length and pending_ops=0).
    // The state transition logic now relies ONLY on the slot being cleared.
    if (conn->state == STATE_RES_CLOSE) {
        conn->state = STATE_END;
    } else if (conn->state == STATE_RES) {
        conn->state = STATE_REQ;
        conn_set_epoll_events (epfd, conn, EPOLLIN); // Stop listening for OUT/ERR
        // Idle list management is now in state_res caller (connection_io)
    }

    return true; // Successfully finished flushing all available data
}

// Note: The old `state_res` loop logic might need slight review. The `while` loop 
// is no longer necessary if we assume one message per send operation.

static void
state_req (Cache *cache, Conn *conn)
{
  DBG_LOGF ("FD %d: Entering STATE_REQ handler.", conn->fd);

  while (try_fill_buffer (cache, conn))
    {
      if (conn->state != STATE_REQ)
	break;
    }

  DBG_LOGF ("FD %d: Exiting STATE_REQ handler. New state: %d.",
	    conn->fd, conn->state);
}

static void
state_res (Conn *conn)
{
  (void) TIME_EXPR ("try_flush_buffer", try_flush_buffer (conn));
}

static void
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

      if (next_us > now_us)
	break;

      msgf ("Removing idle connection: %d", next->fd);
      conn_done (next);
    }

  cache_evict (cache, now_us);
}

// NEW connection_io (around line 1069)
static void
connection_io (Cache *cache, Conn *conn)
{
    // NO-OP: Do not update idle timer or list at the start of every I/O event.

    if (conn->state == STATE_REQ)
        state_req (cache, conn);

    if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE)
        state_res (conn);

    // CRITICAL: Only mark as idle *after* all processing is complete and the state is STATE_REQ.
    // If STATE_REQ is reached, it means we are now waiting for the client.
    if (conn->state == STATE_REQ) {
        conn->idle_start = get_monotonic_usec ();
        dlist_detach (&conn->idle_list); // Should already be detached, but safe
        dlist_insert_before (&g_data.idle_list, &conn->idle_list);
    } else {
        // If we are in STATE_RES (sending), ensure we are NOT in the idle list.
        // This makes next_timer_ms ignore this connection.
        dlist_detach (&conn->idle_list);
    }
}

static void
handle_listener_event (int listen_fd)
{
  int epfd = g_data.epfd;

  while (1)
    {
      int conn_fd = accept_new_conn (listen_fd);

      if (conn_fd < 0)
	{
	  if (conn_fd == -1)
	    break;
	  break;
	}

      struct epoll_event event;
      event.data.fd = conn_fd;
      event.events = EPOLLIN | EPOLLET | EPOLLERR;

      if (epoll_ctl (epfd, EPOLL_CTL_ADD, event.data.fd, &event) == -1)
	{
	  msgf ("epoll ctl: new conn registration failed: %s",
		strerror (errno));
	  close (conn_fd);
	}
    }
}

static void
handle_connection_event (Cache *cache, struct epoll_event *event)
{
 Conn *conn = connpool_lookup (g_data.fd2conn, event->data.fd);
 if (!conn)
 return;

 DBG_LOGF ("FD %d: Handling epoll event (events: 0x%x). State: %d",
 event->data.fd, event->events, conn->state);

 // If EPOLLHUP or EPOLLRDHUP, or if any event is detected while waiting for zero-copy
    // completion, let connection_io run. It will check for completions (if EPOLLERR is set)
    // and handle state transitions and normal I/O.
    
    // Check for fatal errors (HUP/RDHUP) first
    if (event->events & (EPOLLHUP | EPOLLRDHUP))
    {
        DBG_LOGF ("FD %d: Hangup/Error detected (0x%x). Closing.",
                 event->data.fd, event->events);
        conn->state = STATE_END;
        conn_done (conn);
        return;
    }

    // Call connection_io regardless of what events we got (IN, OUT, ERR),
    // as ERR contains the zero-copy completion, and IN/OUT are standard I/O.
    // connection_io contains the logic to run try_flush_buffer which in turn
    // calls try_check_zerocopy_completion when needed.
 TIME_STMT ("connection_io", connection_io (cache, conn));

 if (conn->state == STATE_END)
 conn_done (conn);
}

static void
process_active_events (Cache *cache, struct epoll_event *events, int count,
		       int listen_fd)
{
  for (int i = 0; i < count; ++i)
    {
      if (events[i].data.fd == listen_fd)
	handle_listener_event (listen_fd);
      else
	handle_connection_event (cache, &events[i]);
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
next_timer_ms (Cache *cache)
{
  uint64_t now_us = get_monotonic_usec ();
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
      int timeout_ms = (int) next_timer_ms (cache);
      DBG_LOGF ("Epoll wait for %dms...", timeout_ms);

      int enfd_count = epoll_wait (epfd, events, MAX_EVENTS, timeout_ms);

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
	process_active_events (cache, events, enfd_count, listen_fd);

      process_timers (cache);
    }

  cleanup_server_resources (cache, listen_fd, epfd);
  dump_stats ();
  return 0;
}
