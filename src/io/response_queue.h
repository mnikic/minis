#ifndef RESPONSE_QUEUE_H_
#define RESPONSE_QUEUE_H_

#include <stdint.h>
#include <stdbool.h>

#include "io/connection.h"
#include "io/transport.h"
#include "cache/cache.h"
#include "common/macros.h"

typedef enum
{
  QUEUE_ERROR,			// Fatal error (close connection)
  QUEUE_STALLED,		// Blocked on Write (Enable EPOLLOUT immediately)
  QUEUE_PROGRESSED,		// Success: Parsed at least one command (Loop again!)
  QUEUE_DONE			// Healthy, but no full command ready (Sleep/Wait)
} QueueStatus;

HOT QueueStatus
response_queue_process_buffered_data (Cache * cache, Conn * conn,
				      uint64_t now_us);

// Flush pending responses to the network
// Returns: IO_OK if all sent, IO_WAIT if blocked, IO_ERROR on error
HOT IOStatus response_queue_flush (Conn * conn);

#endif /* RESPONSE_QUEUE_H_ */
