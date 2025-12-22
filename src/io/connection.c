#include <stdbool.h>
#include <stdint.h>
#include "common/common.h"
#include "connection.h"

// Release completed slots from the ring buffer
// Returns: number of slots released
uint32_t
conn_release_comp_slots (Conn *conn)
{
  uint32_t released_count = 0;

  while (true)
    {
      ResponseSlot *slot = &conn->res_slots[conn->read_idx];

      // Check if this slot is fully complete
      if (!conn_is_slot_complete (slot))	// Using helper!
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
conn_is_idle (Conn *conn)
{
  if (conn->state != STATE_ACTIVE)
    {
      return false;
    }

  bool no_pending_responses =
    conn_is_slot_empty (&conn->res_slots[conn->read_idx]);
  bool no_unprocessed_reqs = !conn_has_unprocessed_data (conn);

  return no_pending_responses && no_unprocessed_reqs;
}

void
conn_reset (Conn *conn, int file_desc)
{
  conn->fd = file_desc;
  conn->state = STATE_ACTIVE;
  conn->last_events = 0;
  conn->pending_events = 0;

  // Reset Ring Buffer State
  conn->read_idx = 0;
  conn->write_idx = 0;
  conn->rbuf_size = 0;
  conn->read_offset = 0;

  memset (conn->res_slots, 0, sizeof (conn->res_slots));

  conn->idle_start = 0;		// Will be set externally
  dlist_init (&conn->idle_list);
}
