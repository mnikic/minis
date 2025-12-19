#include <stdbool.h>
#include <stdint.h>
#include "common/common.h"
#include "connection.h"

// Release completed slots from the ring buffer
// Returns: number of slots released
uint32_t
release_completed_slots (Conn *conn)
{
  uint32_t released_count = 0;

  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];

      // Check if this slot is fully complete
      if (!is_slot_complete (slot))	// Using helper!
	break;

      // Release this slot
      slot->actual_length = 0;
      slot->sent = 0;

      DBG_LOGF ("FD %d: Released slot %u from ring buffer.",
		conn->fd, conn->read_idx);

      conn->read_idx = (conn->read_idx + 1) % K_SLOT_COUNT;
      released_count++;
    }

  return released_count;
}

bool
is_connection_idle (Conn *conn)
{
  if (conn->state != STATE_ACTIVE)
    {
      return false;
    }

  bool no_pending_responses =
    is_slot_empty (&conn->res_slots[conn->read_idx]);
  bool no_unprocessed_reqs = !has_unprocessed_data (conn);

  return no_pending_responses && no_unprocessed_reqs;
}
