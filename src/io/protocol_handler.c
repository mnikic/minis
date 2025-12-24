#include <string.h>
#include <stdint.h>

#include "protocol_handler.h"
#include "io/proto_defs.h"
#include "common/common.h"
#include "io/out.h"
#include "io/proto_parser.h"
#include "io/connection.h"
#include "cache/cache.h"
#include "common/macros.h"
#include "io/buffer.h"

// We are sync and single threaded, one cmd scratch will do!
static char *global_cmd_scratch[K_MAX_ARGS];

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

// Execute a parsed request
HOT static bool
execute_request (RequestContext *ctx, uint8_t *req, uint32_t reqlen,
		 Buffer *out_buf)
{
  RestoreState restore;
  ProtoRequest proto_req = {
    .req = req,
    .reqlen = reqlen,
    .cmd = global_cmd_scratch,
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
	  return false;
	}
      proto_req.arg_count = arg_count;
      parse_result = parse_arguments (&proto_req);
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
      return false;
    }

  DBG_LOGF ("FD %d: Executing command with %zu arguments",
	    ctx->conn->fd, proto_req.cmd_size);

  bool success = TIME_EXPR ("cache_execute",
			    cache_execute (ctx->cache, global_cmd_scratch,
					   proto_req.cmd_size,
					   out_buf, ctx->now_us));
  restore_all_bytes (&restore);

  if (unlikely (!success))
    {
      msg ("cache couldn't write message, no space.");
      protocol_send_error (ctx->conn, ERR_UNKNOWN, "response too large");
      return false;
    }

  return true;
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

  if (unlikely (msg.status == PROTO_DECISION_TOO_BIG))
    {
      protocol_send_error (conn, ERR_2BIG, "request too large");
      conn->read_offset = conn->rbuf_size;
      return false;
    }

  // Set protocol type
  conn->proto = msg.type;

  // Prepare output buffer
  uint32_t gap = 0;
  uint8_t *write_ptr = conn_prepare_write_slot (conn, K_MAX_MSG, &gap);

  // Buffer full is technically "valid" backpressure, but in this specific logic flow,
  // it usually means we failed to allocate what we *expect* to be available.
  if (unlikely (!write_ptr))
    return false;

  // Execute request
  uint32_t offset = (conn->proto == PROTO_BIN) ? 4 : 0;
  Buffer out_buf = buf_init (write_ptr + offset, K_MAX_MSG - offset);
  buf_set_proto (&out_buf, conn->proto);

  bool success = execute_request (ctx,
				  &conn->rbuf[conn->read_offset +
					      msg.header_len],
				  msg.payload_len,
				  &out_buf);

  if (unlikely (!success))
    return false;

  // Commit response
  conn_commit_write (conn, write_ptr, buf_len (&out_buf), gap, true);
  conn->read_offset += msg.total_len;

  DBG_LOGF ("FD %d: Request processed.", conn->fd);
  return true;
}
