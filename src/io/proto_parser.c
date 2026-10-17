/*
 *============================================================================
 * Name             : proto_parser.c
 * Author           : Milos
 * Description      : Zero-copy protocol parser using in-place modification.
 *============================================================================
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <assert.h>
#include <arpa/inet.h>

#include "common/common.h"
#include "io/proto_defs.h"
#include "common/macros.h"
#include "io/proto_parser.h"

/* * Zero-Copy Logic Constraint
 * Packed Binary, Network Byte Order (Big Endian), and the Zero-Copy Hack colliding 
 * here, after this size we start corrupting.
 */
#define STRINGIFY_(x) #x
#define STRINGIFY(x) STRINGIFY_ (x)

_Static_assert (K_MAX_MSG < 16777215,
		"K_MAX_MSG is too large for zero-copy binary protocol hack.");



// Helper: Read a line like "123\r\n" and parse the integer. INCOMPLETE
// only while the bytes so far can still become one, so garbage is caught
// at once instead of waiting for a CR that may never come.
static ALWAYS_INLINE ProtoStatus
read_resp_int (const uint8_t *buf, size_t limit, size_t *pos,
	       int64_t *out_val)
{
  const uint8_t *curr = buf + *pos;
  const uint8_t *end = buf + limit;

  bool neg = false;
  if (curr < end && *curr == '-')
    {
      neg = true;
      curr++;
    }

  int64_t val = 0;
  size_t digits = 0;
  while (curr < end && *curr >= '0' && *curr <= '9')
    {
      int digit = *curr - '0';
      if (unlikely (val > (INT64_MAX - digit) / 10))
	return PROTO_DECISION_INVALID;	// Overflow
      val = (val * 10) + digit;
      digits++;
      curr++;
    }

  if (curr == end)
    return PROTO_DECISION_INCOMPLETE;
  // Strict Protocol Check: digits, then \r\n
  if (unlikely (*curr != '\r' || digits == 0))
    return PROTO_DECISION_INVALID;
  if (curr + 1 == end)
    return PROTO_DECISION_INCOMPLETE;
  if (unlikely (*(curr + 1) != '\n'))
    return PROTO_DECISION_INVALID;

  *out_val = neg ? -val : val;
  *pos = (size_t) (curr - buf) + 2;	// Skip \r\n
  return PROTO_DECISION_OK;
}

/**
 * Scans ahead to find total RESP message length.
 * Format: *<count>\r\n...
 * Marked HOT as it runs on every packet.
 */
HOT ProtoStatus
scan_resp_message (const uint8_t *buf, size_t limit, size_t *out_len,
		   const char **out_error)
{
  size_t pos = 0;
  if (unlikely (limit == 0))
    return PROTO_DECISION_INCOMPLETE;
  if (unlikely (buf[pos] != '*'))
    {
      *out_error = "Protocol error: expected '*'";
      return PROTO_DECISION_INVALID;
    }
  pos++;

  int64_t count = 0;
  ProtoStatus status = read_resp_int (buf, limit, &pos, &count);
  if (status == PROTO_DECISION_INVALID)
    {
      *out_error = "Protocol error: invalid multibulk length";
      return PROTO_DECISION_INVALID;
    }
  if (status != PROTO_DECISION_OK)
    return status;

  // Like Redis, *0 and *-1 are empty requests, which get no reply
  if (count <= 0)
    {
      *out_len = pos;
      return PROTO_DECISION_OK;
    }

  // Waiting for the rest would never end: the request cannot be parsed
  if (unlikely (count > K_MAX_ARGS))
    {
      *out_error = "Protocol error: too many arguments (at most "
	STRINGIFY (K_MAX_ARGS) ")";
      return PROTO_DECISION_INVALID;
    }

  // Loop through arguments
  for (int i = 0; i < count; i++)
    {
      if (pos >= limit)
	return PROTO_DECISION_INCOMPLETE;

      if (unlikely (buf[pos] != '$'))
	{
	  // Single threaded. Like Redis, a CR or LF, which would break
	  // the error line, shows as a space.
	  static char expected[] = "Protocol error: expected '$', got ' '";
	  bool newline = buf[pos] == '\r' || buf[pos] == '\n';
	  expected[sizeof (expected) - 3] = newline ? ' ' : (char) buf[pos];
	  *out_error = expected;
	  return PROTO_DECISION_INVALID;
	}
      pos++;

      int64_t len = 0;
      status = read_resp_int (buf, limit, &pos, &len);
      if (status == PROTO_DECISION_INVALID || (status == PROTO_DECISION_OK
					       && len < 0))
	{
	  *out_error = "Protocol error: invalid bulk length";
	  return PROTO_DECISION_INVALID;
	}
      if (status != PROTO_DECISION_OK)
	return status;

      // Calculate where data ends
      size_t data_end = pos + (size_t) len;

      // The whole request has to fit in the read buffer. The lengths
      // tell before the bytes arrive, so do not wait for them.
      if (unlikely (len > (int64_t) K_MAX_MSG || data_end + 2 > K_RBUF_SIZE))
	return PROTO_DECISION_TOO_BIG;

      // Check: Data + \r\n must fit
      if (data_end + 2 > limit)
	return PROTO_DECISION_INCOMPLETE;

      // STRICT CHECK: Verify tail is actually \r\n
      // Previous code trusted 'len', allowing garbage delimiters.
      if (unlikely (buf[data_end] != '\r' || buf[data_end + 1] != '\n'))
	{
	  *out_error = "Protocol error: invalid CRLF in request";
	  return PROTO_DECISION_INVALID;
	}

      pos = data_end + 2;
    }

  *out_len = pos;
  return PROTO_DECISION_OK;
}

HOT ProtoMessageInfo
proto_identify_message (const uint8_t *buf, size_t len)
{
  ProtoMessageInfo info = { 0 };

  if (unlikely (len == 0))
    {
      info.status = PROTO_DECISION_INCOMPLETE;
      return info;
    }

  // Sniff First Byte: '*' means RESP
  if (likely (buf[0] == '*'))
    {
      info.type = PROTO_RESP;
      size_t msg_len = 0;
      info.status = scan_resp_message (buf, len, &msg_len, &info.error);
      if (info.status == PROTO_DECISION_OK)
	{
	  info.payload_len = (uint32_t) msg_len;
	  info.header_len = 0;
	  info.total_len = info.payload_len;
	}
    }
  else
    {
      info.type = PROTO_BIN;
      if (len < BIN_HEADER_SIZE)
	{
	  info.status = PROTO_DECISION_INCOMPLETE;
	  return info;
	}

      uint32_t msg_len;
      memcpy (&msg_len, buf, 4);
      msg_len = ntohl (msg_len);

      if (unlikely (msg_len > K_MAX_MSG))
	{
	  info.status = PROTO_DECISION_TOO_BIG;
	  return info;
	}

      info.header_len = BIN_HEADER_SIZE;
      info.payload_len = msg_len;
      info.total_len = BIN_HEADER_SIZE + msg_len;

      if (len < info.total_len)
	info.status = PROTO_DECISION_INCOMPLETE;
      else
	info.status = PROTO_DECISION_OK;
    }

  return info;
}

ValidationResult
validate_request_header (const uint8_t *req, uint32_t reqlen,
			 uint32_t *out_arg_count)
{
  if (reqlen < 4)
    return VALIDATE_TOO_SHORT;

  uint32_t num;
  memcpy (&num, req, 4);
  num = ntohl (num);

  if (num > K_MAX_ARGS)
    return VALIDATE_TOO_MANY_ARGS;
  if (num < 1)
    return VALIDATE_TOO_FEW_ARGS;

  *out_arg_count = num;
  return VALIDATE_OK;
}

static ALWAYS_INLINE void
save_and_nullterm (RestoreState *state, uint8_t *location,
		   uint8_t original_char)
{
  // Debug assert to catch logic errors in dev
  assert (state->restore_count < K_MAX_ARGS);

  state->restore_ptrs[state->restore_count] = location;
  state->restore_chars[state->restore_count] = original_char;
  state->restore_count++;
  *location = '\0';
}

HOT ParseResult
proto_parse_arguments (ProtoRequest *proto_request)
{
  restore_state_init (proto_request->restore);
  size_t pos = 4;
  size_t cmd_size = 0;
  uint8_t *req = proto_request->req;

  for (uint32_t i = 0; i < proto_request->arg_count; i++)
    {
      if (unlikely (pos + 4 > proto_request->reqlen))
	return PARSE_MISSING_LENGTH;

      uint32_t arg_len;
      memcpy (&arg_len, &req[pos], 4);
      arg_len = ntohl (arg_len);

      if (unlikely (pos + 4 + arg_len > proto_request->reqlen))
	return PARSE_LENGTH_OVERFLOW;

      uint8_t *null_term_loc = &req[pos + 4 + arg_len];
      uint8_t original_char = *null_term_loc;

      save_and_nullterm (proto_request->restore, null_term_loc,
			 original_char);

      proto_request->lens[cmd_size] = arg_len;
      proto_request->cmd[cmd_size++] = (char *) &req[pos + 4];
      pos += 4 + arg_len;
    }

  if (unlikely (pos != proto_request->reqlen))
    return PARSE_TRAILING_DATA;

  proto_request->cmd_size = cmd_size;
  return PARSE_OK;
}

HOT ParseResult
parse_resp_arguments (ProtoRequest *req)
{
  restore_state_init (req->restore);
  size_t pos = 0;
  uint8_t *buf = req->req;
  size_t limit = req->reqlen;

  // We skip checks done in scan_resp_message
  // But we must re-parse to locate pointers.
  if (unlikely (buf[pos] != '*'))
    return PARSE_BAD_PROTOCOL;
  pos++;

  int64_t count = 0;
  read_resp_int (buf, limit, &pos, &count);	// Result assumed valid from scan

  if (unlikely (count > K_MAX_ARGS))
    return PARSE_OUT_OF_MEMORY;

  req->cmd_size = 0;

  for (int i = 0; i < count; i++)
    {
      pos++;			// Skip '$'

      int64_t len = 0;
      read_resp_int (buf, limit, &pos, &len);

      uint8_t *data_start = &buf[pos];
      uint8_t *data_end = data_start + len;

      // Zero-Copy Trick: Overwrite the \r with \0
      save_and_nullterm (req->restore, data_end, *data_end);

      req->lens[req->cmd_size] = (size_t) len;
      req->cmd[req->cmd_size++] = (char *) data_start;

      pos += (size_t) len + 2;
    }

  return PARSE_OK;
}
