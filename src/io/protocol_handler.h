#ifndef PROTOCOL_HANDLER_H_
#define PROTOCOL_HANDLER_H_

#include <stdint.h>
#include <stdbool.h>
#include "io/connection.h"
#include "cache/cache.h"
#include "io/proto_defs.h"

// Context for request processing
typedef struct
{
  Cache *cache;
  Conn *conn;
  uint64_t now_us;
  uint64_t block_timeout_us;	// Set when a request blocks
  // The next request framed already, or NULL, and its key's hash
  const ProtoMessageInfo *frame;
  uint64_t key_hcode;
} RequestContext;

// Try to parse and execute one request from the connection's read buffer
// Returns: true if a request was processed, false if incomplete/error
HOT bool protocol_try_one_request (RequestContext * ctx);

#endif /* PROTOCOL_HANDLER_H_ */
