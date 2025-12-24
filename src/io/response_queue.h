#ifndef RESPONSE_QUEUE_H_
#define RESPONSE_QUEUE_H_

#include <stdint.h>
#include <stdbool.h>

#include "io/connection.h"
#include "io/transport.h"
#include "cache/cache.h"
#include "common/macros.h"

// Process incoming requests and fill the response queue
// Returns: true if all input was consumed, false if more work remains
HOT bool response_queue_process_input (Cache * cache, Conn * conn,
				       uint64_t now_us);

// Flush pending responses to the network
// Returns: IO_OK if all sent, IO_WAIT if blocked, IO_ERROR on error
HOT IOStatus response_queue_flush (Conn * conn);

#endif /* RESPONSE_QUEUE_H_ */
