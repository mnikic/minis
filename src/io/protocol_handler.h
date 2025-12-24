#ifndef PROTOCOL_HANDLER_H_
#define PROTOCOL_HANDLER_H_

#include <stdint.h>
#include <stdbool.h>
#include "io/connection.h"
#include "cache/cache.h"

// Context for request processing
typedef struct
{
  Cache *cache;
  Conn *conn;
  uint64_t now_us;
} RequestContext;

// Try to parse and execute one request from the connection's read buffer
// Returns: true if a request was processed, false if incomplete/error
HOT bool protocol_try_one_request (RequestContext * ctx);

#endif /* PROTOCOL_HANDLER_H_ */
