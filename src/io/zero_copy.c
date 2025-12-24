/*
 *============================================================================
 * Name             : zero_copy.c
 * Author           : Milos
 * Description      : Linux MSG_ZEROCOPY implementation.
 *============================================================================
 */
#include "zero_copy.h"
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <linux/errqueue.h>
#include <linux/if_packet.h>
#include <linux/ipv6.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>

#include "connection.h"
#include "common/common.h"
#include "common/macros.h"

// Helper: Fetch one notification count from the kernel.
// Returns: number of completed operations (ranges), or 0 if none.
static uint32_t
read_one_notification (int file_desc)
{
  struct msghdr msg = { 0 };
  struct sock_extended_err *serr;
  struct cmsghdr *cms;

  // Buffer for ancillary data (must hold sock_extended_err)
  char control[CMSG_SPACE (sizeof (struct sock_extended_err)) + 32];

  msg.msg_control = control;
  msg.msg_controllen = sizeof (control);

  int ret = (int) recvmsg (file_desc, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

  if (unlikely (ret < 0))
    {
      if (errno == EAGAIN || errno == ENOMSG)
	return 0;		// Queue empty

      msgf ("zc: recvmsg(MSG_ERRQUEUE) failed: %s", strerror (errno));
      return 0;
    }

  for (cms = CMSG_FIRSTHDR (&msg); cms; cms = CMSG_NXTHDR (&msg, cms))
    {
      if ((cms->cmsg_level == SOL_IP && cms->cmsg_type == IP_RECVERR) ||
	  (cms->cmsg_level == SOL_IPV6 && cms->cmsg_type == IPV6_RECVERR))
	{
	  serr = (void *) CMSG_DATA (cms);

	  if (serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY)
	    {
	      // Kernel returns a range [ee_info, ee_data]
	      uint32_t range_start = serr->ee_info;
	      uint32_t range_end = serr->ee_data;
	      return range_end - range_start + 1;
	    }
	}
    }

  return 0;
}

bool
zc_process_completions (Conn *conn)
{
  bool made_progress = false;

  // Loop until the kernel Error Queue is completely empty
  while (true)
    {
      uint32_t ops_to_ack = read_one_notification (conn->fd);

      if (ops_to_ack == 0)
	break;

      made_progress = true;

      // Distribute these ACKs across the pipeline
      // If ops_to_ack > head->pending, we finish head and move to next.
      while (ops_to_ack > 0)
	{
	  if (conn->pipeline_depth == 0)
	    {
	      msgf
		("FD %d: ZC Error: Received ACK for %u ops but pipeline is empty!",
		 conn->fd, ops_to_ack);
	      break;
	    }

	  ResponseSlot *head = conn_get_head_slot (conn);

	  // If a slot isn't zero-copy, it shouldn't be waiting for ZC acks.
	  // (Unless we have mixed ZC/Non-ZC traffic, but strict ordering applies)
	  if (!head->is_zero_copy)
	    {
	      // Just skip/release it if it was already sent? 
	      // In strict TCP serialization, this shouldn't happen if logic is correct.
	      // But for safety, we assume this ACK belongs to the first ZC slot found?
	      // No, TCP is ordered. This is likely a bug if hit.
	      break;
	    }

	  if (ops_to_ack >= head->pending_ops)
	    {
	      // This slot is fully ACKed
	      ops_to_ack -= head->pending_ops;
	      head->pending_ops = 0;

	      DBG_LOGF ("FD %d: Slot %u fully ACKed via spillover", conn->fd,
			conn->read_idx);

	      // Try to release it immediately to advance read_idx
	      // This brings the next slot to the Head for the next loop iteration
	      conn_release_comp_slots (conn);
	    }
	  else
	    {
	      // Partial ACK for this slot (ops_to_ack < pending)
	      head->pending_ops -= ops_to_ack;
	      ops_to_ack = 0;
	      DBG_LOGF ("FD %d: Slot %u partial ACK (remaining: %u)",
			conn->fd, conn->read_idx, head->pending_ops);
	    }
	}
    }

  return made_progress;
}

COLD void
zc_drain_errors (int file_desc)
{
  struct msghdr msg = { 0 };
  char control[100];
  msg.msg_control = control;
  msg.msg_controllen = sizeof (control);

  int safety_loop = 0;

  while (true)
    {
      if (unlikely (++safety_loop > 1000))
	break;
      int ret = (int) recvmsg (file_desc, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);
      if (ret < 0)
	return;
    }
}
