#include <stdbool.h>
#include <stdint.h>
#include "common/common.h"
#include "connection.h"

// Update slot's pending_ops count based on completions
// Returns: true if the slot became fully complete (sent AND acked)
// Corrected signature and body
bool
apply_zerocopy_completion (int file_desc, uint32_t slot_idx,
			   ResponseSlot *slot, uint32_t completed_ops)
{
  (void) file_desc, (void) slot_idx;	// Need the for logging, but GCC complains!
  if (completed_ops == 0)
    return false;

  if (slot->pending_ops == 0)
    {
      DBG_LOGF
	("FD %d: WARNING: Completion received for %u ops but slot %u has 0 pending.",
	 file_desc, completed_ops, slot_idx);
      return false;
    }

  uint32_t ops_before = slot->pending_ops;
  (void) ops_before;		// make the compiler happy!
  if (completed_ops > slot->pending_ops)
    {
      DBG_LOGF
	("FD %d: WARNING: Completion overflow. Got %u ops, only %u pending. Setting pending to 0.",
	 file_desc, completed_ops, slot->pending_ops);
      slot->pending_ops = 0;
    }
  else
    {
      slot->pending_ops -= completed_ops;
    }

  // 4. Log the state change
  DBG_LOGF
    ("FD %d: Slot %u completion applied. Pending ops: %u -> %u. (%u ops acknowledged).",
     file_desc, slot_idx, ops_before, slot->pending_ops, completed_ops);

  // 5. Check for full completion
  // This uses the logic from the `is_slot_complete` helper you defined earlier.
  return (slot->pending_ops == 0 &&
	  slot->sent == slot->actual_length && slot->actual_length > 0);
}

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
