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
#include "proto_parser.h"

/* * Zero-Copy Logic Constraint
 * Packed Binary, Network Byte Order (Big Endian), and the Zero-Copy Hack colliding 
 * here, after this size we start corrupting.
 */
_Static_assert (K_MAX_MSG < 16777215,
		"K_MAX_MSG is too large for zero-copy binary protocol hack.");



// Helper: Read a line like "123\r\n" and parse the integer. 
static ALWAYS_INLINE bool
read_resp_int (const uint8_t *buf, size_t limit, size_t *pos,
	       int64_t *out_val)
{
  const uint8_t *start_ptr = buf + *pos;
  size_t remaining = limit - *pos;

  const uint8_t *cr_ptr = memchr (start_ptr, '\r', remaining);

  // Incomplete (no CR found) or split buffer
  if (unlikely (!cr_ptr))
    return false;

  // Check bounds for \n (Need at least 1 byte after \r)
  // cr_ptr points to \r. We need cr_ptr + 1 to be valid.
  if (unlikely (cr_ptr + 1 >= buf + limit))
    return false;

  // Strict Protocol Check: Must be followed by \n
  if (unlikely (*(cr_ptr + 1) != '\n'))
    return false;

  // Parse Integer
  int64_t val = 0;
  bool neg = false;
  const uint8_t *curr = start_ptr;

  if (*curr == '-')
    {
      neg = true;
      curr++;
    }

  // Loop only over the digits (pointer math is fast)
  while (curr < cr_ptr)
    {
      uint8_t cur = *curr;
      if (unlikely (cur < '0' || cur > '9'))
	return false;		// Not a number

      val = (val * 10) + (cur - '0');
      curr++;
    }

  if (neg)
    val = -val;

  *out_val = val;
  *pos = (size_t) (cr_ptr - buf) + 2;	// Skip \r\n
  return true;
}

/**
 * Scans ahead to find total RESP message length.
 * Format: *<count>\r\n...
 * Marked HOT as it runs on every packet.
 */
HOT ssize_t
scan_resp_message_length (const uint8_t *buf, size_t limit)
{
  if (unlikely (limit < 4))
    return -1;

  size_t pos = 0;
  if (unlikely (buf[pos] != '*'))
    return -1;
  pos++;

  int64_t count = 0;
  if (!read_resp_int (buf, limit, &pos, &count))
    return -1;

  if (unlikely (count < 0 || count > K_MAX_ARGS))
    return -1;

  // Loop through arguments
  for (int i = 0; i < count; i++)
    {
      // Check space for header "$0\r\n" (at least 4 bytes)
      if (unlikely (pos + 4 > limit))
	return -1;

      if (unlikely (buf[pos] != '$'))
	return -1;
      pos++;

      int64_t len = 0;
      if (!read_resp_int (buf, limit, &pos, &len))
	return -1;

      if (unlikely (len < 0 || len > (int64_t) K_MAX_MSG))
	return -1;

      // Calculate where data ends
      size_t data_end = pos + (size_t) len;

      // Check: Data + \r\n must fit
      if (data_end + 2 > limit)
	return -1;

      // STRICT CHECK: Verify tail is actually \r\n
      // Previous code trusted 'len', allowing garbage delimiters.
      if (unlikely (buf[data_end] != '\r' || buf[data_end + 1] != '\n'))
	return -1;

      pos = data_end + 2;
    }

  return (ssize_t) pos;
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
      ssize_t ret = scan_resp_message_length (buf, len);

      if (ret < 0)
	{
	  info.status = PROTO_DECISION_INCOMPLETE;
	}
      else
	{
	  info.status = PROTO_DECISION_OK;
	  info.payload_len = (uint32_t) ret;
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
parse_arguments (ProtoRequest *proto_request)
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

  // We skip checks done in scan_resp_message_length
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

      req->cmd[req->cmd_size++] = (char *) data_start;

      pos += (size_t) len + 2;
    }

  return PARSE_OK;
}
