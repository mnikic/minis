/*
 *============================================================================
 * Name             : zerocopy.c
 * Author           : Milos
 * Description      : Linux MSG_ZEROCOPY implementation.
 *============================================================================
 */

#include "zerocopy.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <linux/errqueue.h>
#include <linux/if_packet.h>
#include <linux/ipv6.h>
#include <netinet/ip.h>
#include <netinet/ip6.h>

#include "common/common.h"
#include "connection.h"

// Returns true if the slot is now fully complete (sent + acked)
static bool
apply_zerocopy_completion (int file_desc, uint32_t slot_idx,
			   ResponseSlot *slot, uint32_t completed_ops)
{
  if (unlikely (completed_ops > slot->pending_ops))
    {
      msgf
	("FD %d: ZC CRITICAL ERROR: Kernel reported %u completions, but slot %u only had %u pending.",
	 file_desc, completed_ops, slot_idx, slot->pending_ops);
      // Force fix to avoid stuck connection
      slot->pending_ops = 0;
    }
  else
    {
      slot->pending_ops -= completed_ops;
    }

  DBG_LOGF ("FD %d: ZC ACK received. Slot %u pending ops dropped to %u.",
	    file_desc, slot_idx, slot->pending_ops);

  // The slot is ready to be released IF:
  // 1. We have finished passing data to the kernel (sent == actual_length)
  // 2. The kernel has finished sending data to NIC (pending_ops == 0)
  return (slot->sent == slot->actual_length && slot->pending_ops == 0);
}

// Helper: Fetch one notification from the kernel.
// Returns the number of completed operations (ranges), or 0 if none.
static uint32_t
read_one_notification (int file_desc)
{
  struct msghdr msg = { 0 };
  struct sock_extended_err *serr;
  struct cmsghdr *cms;

  // Buffer for control messages (ancillary data)
  // Must be large enough to hold sock_extended_err
  char control[CMSG_SPACE (sizeof (struct sock_extended_err)) + 32];

  msg.msg_control = control;
  msg.msg_controllen = sizeof (control);

  // Try to read from the Error Queue
  int ret = (int) recvmsg (file_desc, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

  if (unlikely (ret < 0))
    {
      // EAGAIN/ENOMSG are normal "Empty Queue" signals
      if (errno == EAGAIN || errno == ENOMSG)
	return 0;

      msgf ("zc: recvmsg(MSG_ERRQUEUE) failed: %s", strerror (errno));
      return 0;
    }

  // Parse the Control Messages to find the ZeroCopy notification
  for (cms = CMSG_FIRSTHDR (&msg); cms; cms = CMSG_NXTHDR (&msg, cms))
    {
      if ((cms->cmsg_level == SOL_IP && cms->cmsg_type == IP_RECVERR) ||
	  (cms->cmsg_level == SOL_IPV6 && cms->cmsg_type == IPV6_RECVERR))
	{
	  serr = (void *) CMSG_DATA (cms);

	  if (serr->ee_origin == SO_EE_ORIGIN_ZEROCOPY)
	    {
	      // The kernel returns a range [ee_info, ee_data]
	      uint32_t range_start = serr->ee_info;
	      uint32_t range_end = serr->ee_data;
	      uint32_t count = range_end - range_start + 1;

	      DBG_LOGF ("FD %d: ZC completion [%u-%u] (%u ops).",
			file_desc, range_start, range_end, count);

	      return count;
	    }
	}
    }

  return 0;
}

bool
zc_process_completions (Conn *conn)
{
  // Optimization: Don't check queue if we aren't expecting ZC logic
  ResponseSlot *current_slot = &conn->res_slots[conn->read_idx];
  if (!current_slot->is_zerocopy)
    return false;

  uint32_t completed_ops = read_one_notification (conn->fd);

  if (completed_ops == 0)
    return false;

  bool slot_complete =
    apply_zerocopy_completion (conn->fd, conn->read_idx, current_slot,
			       completed_ops);

  if (slot_complete)
    {
      release_completed_slots (conn);
    }

  return true;
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
	{
	  msgf ("FD %d: Stuck in zc_drain loop, breaking.", file_desc);
	  break;
	}

      int ret = (int) recvmsg (file_desc, &msg, MSG_ERRQUEUE | MSG_DONTWAIT);

      if (ret < 0)
	{
	  if (errno == EAGAIN || errno == ENOMSG)
	    return;		// Queue is empty, success.

	  DBG_LOGF ("FD %d: drain error: %s", file_desc, strerror (errno));
	  return;
	}
    }
}
