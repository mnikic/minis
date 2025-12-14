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

static void state_req (int epfd, Cache * cache, uint64_t now_us, Conn * conn);

// Returns true if a zerocopy completion was processed, false otherwise.

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

static bool
try_check_zerocopy_completion (int epfd, Conn *conn, Cache *cache,
			       uint64_t now_us)
{
  // The connection's read index (conn->read_idx) points to the OLDEST response slot
  // that the client is currently trying to consume. This is the slot that 
  // corresponds to the oldest pending zero-copy operation.
  ResponseSlot *current_slot = &conn->res_slots[conn->read_idx];
  if (!current_slot->is_zerocopy)
    return false;
  // Check if the current slot is even active (if actual_length == 0, there's nothing 
  // to complete, unless this is a late error for a previous block).
  // We rely on the pending_ops count for safety.
  if (current_slot->actual_length == 0 && current_slot->pending_ops == 0)
    {
      // If the current slot is clear, check the next one. This is a crucial safety measure
      // in case a completion for a past slot was delayed, but in a ring buffer, 
      // read_idx should always point to the next expected completion.
      // We'll proceed with the error queue logic regardless to consume the error.
      DBG_LOGF
	("FD %d: WARNING: Received EPOLLERR but current slot %u is clear. Continuing to consume error queue.",
	 conn->fd, conn->read_idx);
    }

  // --- 1. Standard Error Queue Setup and Read ---
  struct msghdr msg = { 0 };
  struct sock_extended_err *serr;
  struct cmsghdr *cm;
  char control[100];
  int ret;

  msg.msg_control = control;
  msg.msg_controllen = sizeof (control);

  ret = (int) recvmsg (conn->fd, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

  if (ret < 0)
    {
      if (errno == EAGAIN || errno == ENOMSG)
	return false;		// No more errors/completions to process

      msgf ("recvmsg(MSG_ERRQUEUE) error: %s", strerror (errno));
      conn->state = STATE_END;
      return false;
    }

  bool completion_processed = false;

  // --- 2. Iterate Control Messages and Process Completions ---
  for (cm = CMSG_FIRSTHDR (&msg); cm; cm = CMSG_NXTHDR (&msg, cm))
    {
      if (cm->cmsg_level != SOL_IP && cm->cmsg_level != SOL_IPV6)
	continue;

      if (cm->cmsg_type != IP_RECVERR && cm->cmsg_type != IPV6_RECVERR)
	continue;

      serr = (void *) CMSG_DATA (cm);

      if (serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY)
	{
	  uint32_t range_start = serr->ee_info;
	  uint32_t range_end = serr->ee_data;
	  uint32_t completed_ops = range_end - range_start + 1;

	  // The completion relates to the slot at conn->read_idx
	  // (since completions are ordered).

	  if (current_slot->pending_ops == 0)
	    {
	      // If we get a completion but have no pending ops, 
	      // it might mean a completion for a previously cleared slot arrived late.
	      DBG_LOGF
		("FD %d: WARNING: Completion received (%u ops) but slot %u has 0 pending.",
		 conn->fd, completed_ops, conn->read_idx);
	      completion_processed = true;
	      continue;
	    }

	  if (completed_ops > current_slot->pending_ops)
	    {
	      DBG_LOGF
		("FD %d: WARNING: Completion overflow. Got %u ops, only %u pending.",
		 conn->fd, completed_ops, current_slot->pending_ops);
	      current_slot->pending_ops = 0;
	    }
	  else
	    {
	      current_slot->pending_ops -= completed_ops;
	    }

	  DBG_LOGF
	    ("FD %d: ZEROCOPY completion range [%u-%u] (%u ops). Slot %u pending: %u.",
	     conn->fd, range_start, range_end, completed_ops, conn->read_idx,
	     current_slot->pending_ops);

	  completion_processed = true;
	  // --- 3. Continuous Slot Cleanup and Advancement ---
	  // Process all fully completed slots starting from conn->read_idx
	  while (current_slot->pending_ops == 0 &&
		 current_slot->sent == current_slot->actual_length &&
		 current_slot->actual_length > 0)
	    {
	      // Clear the slot memory tracking
	      current_slot->actual_length = 0;
	      current_slot->sent = 0;
	      // pending_ops is already 0

	      DBG_LOGF
		("FD %d: Fully completed slot %u released from the ring buffer.",
		 conn->fd, conn->read_idx);

	      // Move the read index to the next slot
	      conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;

	      // CRITICAL: Update the pointer to current_slot for the next loop iteration.
	      current_slot = &conn->res_slots[conn->read_idx];
	    }
	}
      if (completion_processed && conn->state != STATE_END)
	{
	  // We might have been waiting without EPOLLIN if the ZC slot was fully sent and
	  // we explicitly removed EPOLLOUT/EPOLLIN flags in try_flush_buffer (though current code keeps EPOLLIN).
	  // For robustness after a ZC event, ensure we are properly ready for the next request.
	  //conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);     // <--- We need epfd here!
	}
    }

  return completion_processed;
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

  conn->res_slots[w_idx].actual_length = 4 + (uint32_t) err_payload_len;
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
  if (is_res_queue_full (conn))
    {
      DBG_LOGF ("FD %d: Response queue full. Pausing request parsing.",
		conn->fd);
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
      dump_error_and_close (epfd, conn, ERR_2BIG,
			    "request too large; connection closed.");
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
  uint8_t *slot_mem_data = &conn->res_data[(size_t) (w_idx * K_MAX_MSG)];

  // The Buffer structure will only operate on the payload area of the slot
  Buffer out_buf = buf_init (slot_mem_data + 4, K_MAX_MSG - 4);

  // 2. Execute the request (do_request writes payload into out_buf)
  bool success = do_request (epfd, cache, conn,
			     &conn->rbuf[conn->read_offset + 4],	// Request payload
			     len,
			     &out_buf, now_us);

  if (success)
    {
      size_t reslen = buf_len (&out_buf);
      uint32_t nwlen = htonl ((uint32_t) reslen);

      // Finalize the response slot with the length prefix
      memcpy (slot_mem_data, &nwlen, 4);

      // Mark the ResponseSlot as ready for sending
      ResponseSlot *slot = &conn->res_slots[w_idx];
      slot->actual_length = 4 + (uint32_t) reslen;
      slot->sent = 0;
      slot->pending_ops = 0;
      slot->is_zerocopy = (reslen > (size_t) K_ZEROCPY_THEASHOLD);
      // Advance the write index
      conn->write_idx = (w_idx + 1) % K_SLOT_COUNT;

      // Update state to RES if we are not already sending
      if (conn->state == STATE_REQ)
	{
	  conn->state = STATE_RES;
	  // Ensure EPOLLOUT and EPOLLERR are set for response draining
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
	}
      DBG_LOGF
	("FD %d: Request processed, consumed %u bytes. Response mode: %s (len: %u)",
	 conn->fd, 4 + len, slot->is_zerocopy ? "ZEROCOPY" : "VANILLA",
	 slot->actual_length);

      // 3. Consume the Request Data (ONLY on success)
      conn->read_offset += total_req_len;
    }

  if (!success)
    {
      // Malformed request or response too large (dump_error_and_close handles state)
      // The read_offset must NOT be advanced here (unless ERR_2BIG did it),
      // otherwise we skip the start of the next valid request.
      msg ("request failed, stopping processing.");
      return false;
    }

  // The original line that was removed (because it was inside the block above)
  // conn->read_offset += total_req_len; <--- THIS IS NOW GONE

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
	  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLOUT | EPOLLERR);
	}
    }
}

// event_state_machine.c

static void
try_fill_buffer (Conn *conn, size_t *out_bytes_read)	// No longer needs epfd, cache, now_us
{
  *out_bytes_read = 0;		// Reset read counter

  while (true)
    {
      size_t cap = sizeof (conn->rbuf) - conn->rbuf_size;

      if (cap == 0)
	{
	  return;
	}

      ssize_t bytes_read = read_data_from_socket (conn, cap);
      if (bytes_read == -2 || bytes_read == -1)
	return;

      conn->rbuf_size += (uint32_t) bytes_read;
      *out_bytes_read += (size_t) bytes_read;
    }
}

static bool
try_flush_buffer (int epfd, Conn *conn, Cache *cache, uint64_t now_us)
{
  // --- 1. DRAIN ALL PENDING ZEROCOPY COMPLETIONS ---
  // This relies on the short-circuit in try_check_zerocopy_completion
  // to stop when the current slot (conn->read_idx) is not a ZC slot.
  while (try_check_zerocopy_completion (epfd, conn, cache, now_us))
    {
    }

  if (conn->state == STATE_RES && conn->read_offset < conn->rbuf_size)
    {
      // ZC ACKs cleared, pending requests remain. Resetting state for pipeline.
      DBG_LOGF
	("FD %d: ACKs cleared, %u bytes remain in RBuf. Resetting state to STATE_REQ for pipeline processing.",
	 conn->fd, conn->rbuf_size - conn->read_offset);
      conn->state = STATE_REQ;
    }

  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];

      // A. Queue Drained Check
      if (slot->actual_length == 0)
	{
	  break;		// All slots processed, exit send loop.
	}

      uint8_t *data_start =
	&conn->res_data[(size_t) (conn->read_idx * K_MAX_MSG)];
      size_t remain = slot->actual_length - slot->sent;

      // B. Fully Sent Check (Waiting for ZC ACK)
      if (slot->sent == slot->actual_length)
	{

	  // This entire block is ONLY relevant for Zero-Copy operations.
	  if (slot->is_zerocopy)
	    {
	      if (slot->pending_ops > 0)
		{
		  // Fully sent, but still waiting for kernel completion.
		  DBG_LOGF
		    ("FD %d: Slot %u fully sent. Waiting for %u ZC completion(s). Stopping send.",
		     conn->fd, conn->read_idx, slot->pending_ops);
		  // Ensure EPOLLOUT is removed while we wait for EPOLLERR
		  conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
		  return false;
		}
	      DBG_LOGF
		("FD %d: Slot %u (ZEROCOPY) completed and ACK'd. Releasing.",
		 conn->fd, conn->read_idx);
	    }
	  else
	    DBG_LOGF
	      ("FD %d: Slot %u completed in VANILLA mode. Releasing from the read index.",
	       conn->fd, conn->read_idx);
	  // Since slot->sent == actual_length and it's either Vanilla or ZC-ACK'd:
	  // This slot is implicitly released and advanced here to keep the loop tight.
	  slot->actual_length = 0;	// Release the slot
	  slot->sent = 0;
	  conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
	  continue;		// Start next loop iteration to check the new conn->read_idx slot
	}

      // C. Conditional Send Logic
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

	  // Zero-Copy call
	  err =
	    sendmsg (conn->fd, &message,
		     MSG_DONTWAIT | MSG_ZEROCOPY | MSG_NOSIGNAL);

	}
      else
	{
	  // --- VANILLA SEND (using write/send) ---
	  // Using write() for simplicity and to differentiate from sendmsg() ZC path
	  err = write (conn->fd, data_start + slot->sent, remain);
	}

      // D. Error and EAGAIN Handling (Unified)
      if (err < 0)
	{
	  if (errno == EAGAIN)
	    {
	      // EWOULDBLOCK: Stop sending for now, keep EPOLLOUT enabled
	      conn_set_epoll_events (epfd, conn,
				     EPOLLIN | EPOLLOUT | EPOLLERR);
	      return false;	// Stop due to EWOULDBLOCK
	    }
	  // Handle fatal error
	  msgf ("send/sendmsg() error: %s", strerror (errno));
	  conn->state = STATE_END;
	  return false;
	}

      // E. Update State (Conditional on Mode)
      slot->sent += (uint32_t) err;

      if (slot->is_zerocopy)
	{
	  slot->pending_ops++;	// Increment ONLY for ZC
	  DBG_LOGF
	    ("FD %d: Sent %zd bytes on slot %u with ZEROCOPY (sent: %u/%u, pending: %u).",
	     conn->fd, err, conn->read_idx, slot->sent, slot->actual_length,
	     slot->pending_ops);

	  // ZC: If fully sent, stop asking for EPOLLOUT and wait for EPOLLERR
	  if (slot->sent == slot->actual_length)
	    {
	      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
	    }
	}
      else
	{
	  DBG_LOGF
	    ("FD %d: Sent %zd bytes on slot %u with VANILLA (sent: %u/%u).",
	     conn->fd, err, conn->read_idx, slot->sent, slot->actual_length);
	}
    }

  // Only happens if the 'while(true)' loop breaks cleanly (A).
  if (conn->state == STATE_RES_CLOSE)
    {
      conn->state = STATE_END;
    }
  else if (conn->state == STATE_RES)
    {
      conn->state = STATE_REQ;
      // FIX: Must listen for EPOLLERR here to catch ZC ACKs after sending all responses.
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
    }
  else if (conn->state == STATE_REQ)
    {
      conn_set_epoll_events (epfd, conn, EPOLLIN | EPOLLERR);
      // Call state_req to process the pending R3 data. 
      // We do this to immediately respond to the fact that resources (the ZC slot) have been freed.
      // This is necessary in case the R3 data stalled processing before the ZC ACK arrived.
      state_req (epfd, cache, now_us, conn);

      // If state_req generated a new response (e.g., R3 was fully sent by the client 
      // between the EPOLLIN and EPOLLERR events), we must return false to signal
      // that the response loop should continue.
      if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE)
	{
	  return false;		// Signal to state_res to continue flushing the newly generated R3 response.
	}
    }
  return true;			// Successfully finished flushing all available data
}

// event_state_machine.c

static void
state_req (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  DBG_LOGF ("FD %d: Entering STATE_REQ handler.", conn->fd);

  // --- 1. Read All Available Data (EPOLLET Pattern) ---
  size_t bytes_read_total = 0;
  // try_fill_buffer now returns true on success/EAGAIN/EOF, false on fatal errors
  try_fill_buffer (conn, &bytes_read_total);

  if (conn->state == STATE_END)
    return;

  // --- 2. Process All Available Data (Pipeline Pattern) ---
  // This handles data read just now AND any data that was lingering
  // from previous cycles (the 72 bytes).
  if (conn->rbuf_size > conn->read_offset || bytes_read_total > 0)
    {
      process_received_data (epfd, cache, conn, now_us);
    }
  else if (bytes_read_total == 0)
    {
      // Edge case: We got an EPOLLIN but read 0 bytes, and had no pending data.
      // This can happen with a spurious wake-up or if the client closed cleanly.
      // Since EOF is handled in read_data_from_socket, we don't need to panic here.
    }

  DBG_LOGF ("FD %d: Exiting STATE_REQ handler. New state: %d.",
	    conn->fd, conn->state);
}

// event_state_machine.c

static void
state_res (int epfd, Cache *cache, uint64_t now_us, Conn *conn)
{
  while (true)
    {
      bool finished = try_flush_buffer (epfd, conn, cache, now_us);

      if (conn->state == STATE_END || !finished)
	{
	  return;
	}

      if (conn->state == STATE_RES_CLOSE)
	{
	  conn->state = STATE_END;
	  return;
	}

      // At this point, conn->state is STATE_REQ (set by try_flush_buffer)

      // --- PIPELINE CONTINUATION LOGIC ---
      // If we transitioned back to STATE_REQ AND there is data remaining in the read buffer,
      // we must immediately process it to avoid blocking on EPOLLET.
      if (conn->read_offset < conn->rbuf_size)
	{
	  DBG_LOGF
	    ("FD %d: Pipeline data detected (%u bytes). Re-entering STATE_REQ.",
	     conn->fd, conn->rbuf_size - conn->read_offset);

	  // Process the remaining data (the 72 bytes).
	  state_req (epfd, cache, now_us, conn);

	  // If state_req generated new responses, the state will be STATE_RES.
	  if (conn->state == STATE_RES || conn->state == STATE_RES_CLOSE)
	    {
	      // Go back to the top of the while(true) loop to flush the new responses.
	      continue;
	    }
	}
      if (conn->state == STATE_REQ && conn->read_offset == conn->rbuf_size)
	{
	  // RBuf is empty. Try a final non-blocking read before going back to epoll_wait.
	  // This is the last chance to clear any "phantom" data that EPOLLET might have missed.
	  size_t ignored_read_bytes;
	  try_fill_buffer (conn, &ignored_read_bytes);

	  // If we read something, process it. This mimics the core logic of state_req.
	  if (conn->rbuf_size > conn->read_offset)
	    {
	      process_received_data (epfd, cache, conn, now_us);
	    }
	}

      // If we fully processed or stalled on the pipeline, we exit.
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

      // On success, we consumed one or more Zero-Copy ACKs (ret > 0). Continue draining.
    }
}
