#include <string.h>
#include <strings.h>
#include <stdint.h>

#include "io/buffer.h"
#include "io/protocol_handler.h"
#include "io/proto_defs.h"
#include "io/out.h"
#include "io/proto_parser.h"
#include "io/connection.h"
#include "io/blocking.h"
#include "cache/cache.h"
#include "common/common.h"
#include "common/macros.h"

// We are sync and single threaded, one cmd scratch will do!
static const char *global_cmd_scratch[K_MAX_ARGS];
static size_t global_len_scratch[K_MAX_ARGS];

// Send error and close
COLD static void
protocol_send_error (Conn *conn, int code, const char *msg)
{
  DBG_LOGF ("FD %d: Sending error %d and closing: %s", conn->fd, code, msg);

  if (conn_is_res_queue_full (conn))
    {
      DBG_LOGF ("FD %d: Critical: Queue full. Hard closing.", conn->fd);
      conn->state = STATE_CLOSE;
      return;
    }

  uint32_t gap = 0;
  // Even in error path, we use the standard slot preparation
  uint8_t *write_ptr = conn_prepare_write_slot (conn, K_MAX_MSG, &gap);
  if (!write_ptr)
    {
      conn->state = STATE_CLOSE;
      return;
    }

  uint32_t offset = (conn->proto == PROTO_BIN) ? 4 : 0;
  Buffer err_buf = buf_init (write_ptr + offset, K_MAX_MSG - offset);
  buf_set_proto (&err_buf, conn->proto);

  if (!out_err (&err_buf, code, msg))
    {
      conn->state = STATE_CLOSE;
      return;
    }

  conn_commit_write (conn, write_ptr, buf_len (&err_buf), gap, false);
  conn->state = STATE_FLUSH_CLOSE;
  conn_set_events (conn, IO_EVENT_READ | IO_EVENT_WRITE);
}

// Handle validation errors
COLD static void
handle_validation_error (Conn *conn, ValidationResult result)
{
  const char *err_msg = NULL;
  switch (result)
    {
    case VALIDATE_TOO_SHORT:
      err_msg = "Request too short for argument count";
      break;
    case VALIDATE_TOO_MANY_ARGS:
      err_msg = "Too many arguments.";
      break;
    case VALIDATE_TOO_FEW_ARGS:
      err_msg = "Must have at least one argument (the command)";
      break;
    case VALIDATE_OK:
    default:
      return;
    }
  protocol_send_error (conn, ERR_MALFORMED, err_msg);
}

// Handle parse errors
COLD static void
handle_parse_error (Conn *conn, ParseResult result)
{
  const char *err_msg = NULL;
  switch (result)
    {
    case PARSE_MISSING_LENGTH:
      err_msg = "Argument count mismatch: missing length header";
      break;
    case PARSE_LENGTH_OVERFLOW:
      err_msg = "Argument count mismatch: data length exceeds packet size";
      break;
    case PARSE_TRAILING_DATA:
      err_msg = "Trailing garbage in request";
      break;
    case PARSE_BAD_PROTOCOL:
      err_msg = "Bad protocol";
      break;
    case PARSE_OUT_OF_MEMORY:
      err_msg = "OOM";
      break;
    case PARSE_OK:
    default:
      return;
    }
  protocol_send_error (conn, ERR_MALFORMED, err_msg);
}

typedef enum
{
  EXEC_OK,
  EXEC_FAIL,			// An error was sent, the connection closes
  EXEC_BLOCK,			// BLPOP/BRPOP has to wait, nothing was written
  EXEC_SKIP			// An empty request, which gets no reply
} ExecResult;

static ALWAYS_INLINE bool
is_blocking_pop (const char *cmd)
{
  return strcasecmp (cmd, "blpop") == 0 || strcasecmp (cmd, "brpop") == 0;
}

// BLPOP/BRPOP. A client that already waits retries: once its timeout is
// over it gets the null array, until then it keeps waiting.
static ExecResult
execute_blocking_pop (RequestContext *ctx, size_t size, Buffer *out_buf)
{
  Conn *conn = ctx->conn;
  uint64_t timeout_us = 0;
  BPopResult res = cache_bpop (ctx->cache, global_cmd_scratch,
			       global_len_scratch, size, conn->blocked, out_buf, ctx->now_us,
			       &timeout_us);
  if (res == BPOP_BLOCK)
    {
      bool timed_out = conn->blocked && conn->block_deadline_us != 0
	&& ctx->now_us >= conn->block_deadline_us;
      if (!timed_out)
	{
	  ctx->block_timeout_us = timeout_us;
	  return EXEC_BLOCK;
	}
      res = out_nil_arr (out_buf) ? BPOP_DONE : BPOP_FAIL;
    }
  return res == BPOP_DONE ? EXEC_OK : EXEC_FAIL;
}

// Execute a parsed request
HOT static ExecResult
execute_request (RequestContext *ctx, uint8_t *req, uint32_t reqlen,
		 Buffer *out_buf)
{
  RestoreState restore;
  ProtoRequest proto_req = {
    .req = req,
    .reqlen = reqlen,
    .cmd = global_cmd_scratch,
    .lens = global_len_scratch,
    .restore = &restore
  };

  // Parse based on protocol
  ParseResult parse_result;
  if (ctx->conn->proto == PROTO_RESP)
    {
      parse_result = parse_resp_arguments (&proto_req);
    }
  else
    {
      uint32_t arg_count = 0;
      ValidationResult val_result =
	validate_request_header (req, reqlen, &arg_count);

      if (unlikely (val_result != VALIDATE_OK))
	{
	  handle_validation_error (ctx->conn, val_result);
	  return EXEC_FAIL;
	}
      proto_req.arg_count = arg_count;
      parse_result = proto_parse_arguments (&proto_req);
    }

  // Parse failure is rare -> UNLIKELY
  if (unlikely (parse_result != PARSE_OK))
    {
      restore_all_bytes (&restore);
      if (parse_result == PARSE_BAD_PROTOCOL)
	protocol_send_error (ctx->conn, ERR_MALFORMED, "Protocol error");
      else if (parse_result == PARSE_OUT_OF_MEMORY)
	protocol_send_error (ctx->conn, ERR_ARG, "Too many arguments");
      else
	handle_parse_error (ctx->conn, parse_result);
      return EXEC_FAIL;
    }

  DBG_LOGF ("FD %d: Executing command with %zu arguments",
	    ctx->conn->fd, proto_req.cmd_size);

  if (proto_req.cmd_size == 0)
    {
      restore_all_bytes (&restore);
      return EXEC_SKIP;
    }

  ExecResult result;
  if (proto_req.cmd_size > 0 && is_blocking_pop (global_cmd_scratch[0]))
    result = execute_blocking_pop (ctx, proto_req.cmd_size, out_buf);
  else
    result = TIME_EXPR ("cache_execute",
			cache_execute_n (ctx->cache, global_cmd_scratch,
					 global_len_scratch, proto_req.cmd_size,
					 out_buf, ctx->now_us)) ? EXEC_OK :
      EXEC_FAIL;
  restore_all_bytes (&restore);

  if (unlikely (result == EXEC_FAIL))
    {
      msg ("cache couldn't write message, no space.");
      protocol_send_error (ctx->conn, ERR_UNKNOWN, "response too large");
    }

  return result;
}

// Try to parse and execute one request
HOT bool
protocol_try_one_request (RequestContext *ctx)
{
  Conn *conn = ctx->conn;

  // Check if we can accept more responses
  if (conn_is_res_queue_full (conn))
    return false;

  // Check if we have enough data to identify the message
  size_t bytes_remain = conn->rbuf_size - conn->read_offset;
  ProtoMessageInfo msg =
    proto_identify_message (&conn->rbuf[conn->read_offset], bytes_remain);

  if (msg.status == PROTO_DECISION_INCOMPLETE)
    return false;

  // Set protocol type, before any error is sent in it
  conn->proto = msg.type;

  if (unlikely (msg.status == PROTO_DECISION_TOO_BIG))
    {
      protocol_send_error (conn, ERR_2BIG, "request too large");
      conn->read_offset = conn->rbuf_size;
      return false;
    }

  // Like Redis, answer a request that can never be parsed with an error
  // and close: the rest of the stream cannot be trusted
  if (unlikely (msg.status == PROTO_DECISION_INVALID))
    {
      protocol_send_error (conn, ERR_MALFORMED, msg.error);
      conn->read_offset = conn->rbuf_size;
      return false;
    }

  // Prepare output buffer. A wrap moves wbuf_head, which a request that
  // blocks has to put back since it commits nothing.
  uint32_t gap = 0;
  uint32_t head_before = conn->wbuf_head;
  uint8_t *write_ptr = conn_prepare_write_slot (conn, K_MAX_MSG, &gap);

  // Buffer full is technically "valid" backpressure, but in this specific logic flow,
  // it usually means we failed to allocate what we *expect* to be available.
  if (unlikely (!write_ptr))
    return false;

  // Execute request
  uint32_t offset = (conn->proto == PROTO_BIN) ? 4 : 0;
  Buffer out_buf = buf_init (write_ptr + offset, K_MAX_MSG - offset);
  buf_set_proto (&out_buf, conn->proto);

  uint64_t pushes_before = cache_list_pushes (ctx->cache);
  ExecResult result = execute_request (ctx,
				       &conn->rbuf[conn->read_offset +
						   msg.header_len],
				       msg.payload_len,
				       &out_buf);

  if (unlikely (result == EXEC_FAIL))
    return false;

  if (result == EXEC_BLOCK)
    {
      // Leave the request in the buffer, to run again when served
      conn->wbuf_head = head_before;
      blocking_park (conn, ctx->block_timeout_us, ctx->now_us);
      return false;
    }

  if (result == EXEC_SKIP)
    {
      conn->wbuf_head = head_before;
      conn->read_offset += msg.total_len;
      return true;
    }

  // Commit response
  conn_commit_write (conn, write_ptr, buf_len (&out_buf), gap, true);
  conn->read_offset += msg.total_len;
  blocking_unpark (conn);

  // Waiting clients get what this pushed before our next command, like
  // in Redis, so stop here and let the server loop serve them
  if (cache_list_pushes (ctx->cache) != pushes_before
      && blocking_count () > 0 && !blocking_is_serving ())
    {
      conn->yield = true;
      return false;
    }

  DBG_LOGF ("FD %d: Request processed.", conn->fd);
  return true;
}
