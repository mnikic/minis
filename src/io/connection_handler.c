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

  conn->res_slots[w_idx].actual_length =  4 + (uint32_t) err_payload_len;
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

static bool
try_one_request (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
    // Rework Note: We are now passing conn->read_offset directly 
    // instead of using a pointer 'start_index'. The caller (process_received_data) 
    // will now use conn->read_offset as its loop variable.

    // 1. Pipelining Check: Stop parsing if the response queue is full.
    if (is_res_queue_full (conn))
    {
        DBG_LOGF ("FD %d: Response queue full. Pausing request parsing.", conn->fd);
        // Ensure EPOLLOUT and EPOLLERR are set to drain the response queue
        conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
        return false;
    }

    size_t bytes_remain = conn->rbuf_size - conn->read_offset;

    // Check for length prefix (4 bytes)
    if (bytes_remain < 4)
        return false;

    uint32_t len = 0;
    // Read length prefix from current offset
    memcpy (&len, &conn->rbuf[conn->read_offset], 4); 
    len = ntohl (len);

    DBG_LOGF ("FD %d: Parsing request starting at offset %u with length %u.",
              conn->fd, conn->read_offset, len);

    // Check against maximum message size
    if (len > K_MAX_MSG)
    {
        msgf ("request too long %u", len);
        dump_error_and_close (epfd, conn, ERR_2BIG, "request too large; connection closed.");
        // Consume the whole buffer on error to prevent re-parsing the bad data
        conn->read_offset = conn->rbuf_size; 
        return false;
    }

    size_t total_req_len = 4 + len;
    
    // Check if the entire request is in the buffer
    if (total_req_len > bytes_remain)
        return false;

    // --- NEW RESPONSE BUFFERING SETUP (Using Ring Buffer) ---
    uint32_t w_idx = conn->write_idx;
    uint8_t *slot_mem_data = 
        &conn->res_data[(size_t) (w_idx * K_MAX_MSG)];
    
    // The Buffer structure will only operate on the payload area of the slot
    Buffer out_buf = buf_init (slot_mem_data + 4, K_MAX_MSG - 4);
    
    // 2. Execute the request (do_request writes payload into out_buf)
    bool success = do_request (epfd, cache, conn,
                             &conn->rbuf[conn->read_offset + 4], // Request payload
                             len,
                             &out_buf, now_us);

    // --- NEW RESPONSE FINALIZATION (No Hand-Off/Copy Required) ---
    if (success)
    {
        size_t reslen = buf_len (&out_buf); // Actual size of payload written
        uint32_t nwlen = htonl ((uint32_t) reslen);

        // Finalize the response slot with the length prefix
        memcpy (slot_mem_data, &nwlen, 4); 
        
        // Mark the ResponseSlot as ready for sending
        ResponseSlot *slot = &conn->res_slots[w_idx];
        slot->actual_length = 4 + (uint32_t)reslen;
        slot->sent = 0;
        slot->pending_ops = 0; // Ready for MSG_ZEROCOPY send
        
        // Advance the write index
        conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;
        
        // Update state to RES if we are not already sending
        if (conn->state == STATE_REQ) {
            conn->state = STATE_RES;
            // Ensure EPOLLOUT and EPOLLERR are set for response draining
            conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
        }
    }
    
    // 3. Consume the Request Data
    conn->read_offset += total_req_len; 

    if (!success)
    {
        // Malformed request or response too large (dump_error_and_close handles state)
        // Since the state machine is now responsible for compaction/reset, we just
        // let the offset stick, and the next call to process_received_data will
        // either consume the rest of the buffer (if error forced offset advance) 
        // or break the loop.
        msg("request failed, stopping processing.");
        return false; 
    }

    DBG_LOGF ("FD %d: Request processed, consumed %u bytes.", conn->fd, 4 + len);

    // Continue processing if the connection is still in a request state
    // (i.e., not forced to close due to error).
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

  DBG_LOGF ("FD %d: Read %zd bytes from socket.", conn->fd, err);
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
    // --- 1. DRAIN ALL PENDING ZEROCOPY COMPLETIONS ---
    // We must drain the error queue until EAGAIN/ENOMSG is hit.
    while (try_check_zerocopy_completion (conn)) {}

    // --- 2. START AGGRESSIVE SENDING LOOP ---
    while (true)
    {
        ResponseSlot *slot = &conn->res_slots[conn->read_idx];
        
        // A. Queue Drained Check
        if (slot->actual_length == 0) {
            break; // All slots processed, exit send loop. 
        }

        uint8_t *data_start = &conn->res_data[(size_t) (conn->read_idx * K_MAX_MSG)];

        // B. Fully Sent Check (Waiting for ACK)
        if (slot->sent == slot->actual_length) {
            if (slot->pending_ops > 0) {
                // Fully sent, but still waiting for kernel completion.
                DBG_LOGF("FD %d: Slot %u fully sent. Waiting for %u completion(s). Stopping send.",
                         conn->fd, conn->read_idx, slot->pending_ops);
                // Ensure EPOLLOUT is removed while we wait for EPOLLERR
                conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
                return false;
            }
            // Should not happen, but defensively advance if the slot isn't cleared
            slot->actual_length = 0;
            conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
            continue; // Move to the next slot
        }

        // C. Send Logic (MSG_ZEROCOPY)
        size_t remain = slot->actual_length - slot->sent;
        
        struct iovec iov = {
            .iov_base = data_start + slot->sent,
            .iov_len = remain
        };
        
        struct msghdr message = { .msg_iov = &iov, .msg_iovlen = 1, /* ... */ };

        ssize_t err = sendmsg(conn->fd, &message, MSG_DONTWAIT | MSG_ZEROCOPY | MSG_NOSIGNAL);

        if (err < 0) {
            if (errno == EAGAIN) {
                // EWOULDBLOCK: Stop sending for now, but keep EPOLLOUT enabled
                conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
                return false; // Stop due to EWOULDBLOCK
            }
            // Handle fatal error
            msgf ("sendmsg() error: %s", strerror (errno));
            conn->state = STATE_END;
            return false;
        }
        
        // D. Update State
        slot->sent += (uint32_t) err;
        slot->pending_ops++; // One more operation waiting for completion
        
        DBG_LOGF("FD %d: Sent %zd bytes on slot %u with ZEROCOPY (sent: %u/%u, pending: %u).",
                  conn->fd, err, conn->read_idx, slot->sent, slot->actual_length, slot->pending_ops);
        
        // CRITICAL FIX: If we successfully sent, DO NOT return false.
        // Let the loop continue immediately to try sending the next chunk 
        // (of the current slot) or move to the next slot if the current one is complete.
        // The only thing we do is adjust the events if the slot is fully sent.
        if (slot->sent == slot->actual_length) {
            // Slot is now fully sent, we stop asking for EPOLLOUT and wait for EPOLLERR.
            conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
            // Now, the loop continues to the next iteration to check the next slot.
        }
        // If partially sent, we continue the loop, and rely on the next iteration
        // to either finish the slot or hit EAGAIN.
    }

    // --- 3. State Transition when send queue is empty ---
    // Only happens if the 'while(true)' loop breaks cleanly.
    if (conn->state == STATE_RES_CLOSE) {
        conn->state = STATE_END;
    } else if (conn->state == STATE_RES) {
        conn->state = STATE_REQ;
        conn_set_epoll_events (epfd, conn, EPOLLIN); // Stop listening for OUT/ERR
    }

    return true; // Successfully finished flushing all available data
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
