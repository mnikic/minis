#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include "common/common.h"
#include "common/macros.h"
#include "io/out.h"
#include "io/buffer.h"
#include "io/proto_defs.h"

// RESP CONSTANTS
#define RESP_OK     "+OK\r\n"
#define RESP_NIL    "$-1\r\n"
#define RESP_CRLF   "\r\n"

HOT bool
out_nil (Buffer *out)
{
  if (out->proto == PROTO_RESP)
    {
      return buf_append_cstr (out, RESP_NIL);
    }

  if (unlikely (!buf_has_space (out, 1)))
    return false;

  buf_append_byte (out, SER_NIL);
  return true;
}

/**
 * @brief Appends a string value with SER_STR marker and length prefix.
 * If string is NULL or size is 0, it writes SER_NIL.
 */
HOT bool
out_str_size (Buffer *out, const char *string, size_t size)
{
  if (unlikely (!string || size == 0))
    {
      return out_nil (out);
    }

  if (unlikely (size > UINT32_MAX))
    {
      return out_err (out, ERR_UNKNOWN, "String too large");
    }

  if (unlikely (!buf_has_space (out, 1 + sizeof (uint32_t) + size)))
    return false;

  // Space guaranteed, append without further checks
  uint32_t len = (uint32_t) size;

  buf_append_byte (out, SER_STR);
  buf_append_u32 (out, len);
  buf_append_bytes (out, string, len);

  return true;
}

/**
 * @brief Appends a Double marker (SER_DBL) and a double value. (9 bytes total)
 */
HOT bool
out_dbl (Buffer *out, double val)
{
  // Check space for: 1 byte (SER_DBL) + 8 bytes (double)
  if (unlikely (!buf_has_space (out, 1 + sizeof (double))))
    return false;

  buf_append_byte (out, SER_DBL);
  buf_append_double (out, val);

  return true;
}

HOT bool
out_str (Buffer *out, const char *val)
{
  if (unlikely (!val))
    return out_nil (out);

  size_t len = strlen (val);

  if (out->proto == PROTO_RESP)
    {
      // Format: $<len>\r\n<data>\r\n

      // Header: "$"
      if (unlikely (!buf_append_byte (out, '$')))
	return false;

      // Length: "N"
      if (unlikely (!buf_append_int_as_string (out, (int64_t) len)))
	return false;

      // Separator: "\r\n"
      if (unlikely (!buf_append_cstr (out, RESP_CRLF)))
	return false;

      // Data
      if (unlikely (!buf_append_bytes (out, val, len)))
	return false;

      // Footer: "\r\n"
      return buf_append_cstr (out, RESP_CRLF);
    }

  if (unlikely (len > UINT32_MAX))
    return out_err (out, ERR_UNKNOWN, "String too large");

  if (unlikely (!buf_has_space (out, 1 + 4 + len)))
    return false;

  buf_append_byte (out, SER_STR);
  buf_append_u32 (out, (uint32_t) len);
  buf_append_cstr (out, val);
  return true;
}

HOT bool
out_int (Buffer *out, int64_t val)
{
  if (out->proto == PROTO_RESP)
    {
      // Format: :<number>\r\n

      // Manual construction
      if (unlikely (!buf_append_byte (out, ':')))
	return false;
      if (unlikely (!buf_append_int_as_string (out, val)))
	return false;
      return buf_append_cstr (out, RESP_CRLF);
    }

  if (unlikely (!buf_has_space (out, 1 + 8)))
    return false;

  buf_append_byte (out, SER_INT);
  buf_append_i64 (out, val);
  return true;
}

COLD bool
out_err (Buffer *out, int32_t code, const char *msg)
{
  if (!msg)
    msg = "Unknown Error";

  if (out->proto == PROTO_RESP)
    {
      // Format: -ERR <msg>\r\n
      if (!buf_append_cstr (out, "-ERR "))
	return false;
      if (!buf_append_cstr (out, msg))
	return false;
      return buf_append_cstr (out, RESP_CRLF);
    }

  size_t msg_len = strlen (msg);
  if (unlikely (!buf_has_space (out, 1 + 4 + 4 + msg_len)))
    return false;

  buf_append_byte (out, SER_ERR);
  buf_append_u32 (out, (uint32_t) code);
  buf_append_u32 (out, (uint32_t) msg_len);
  buf_append_cstr (out, msg);
  return true;
}

HOT bool
out_arr (Buffer *out, size_t num)
{
  if (out->proto == PROTO_RESP)
    {
      // Format: *<count>\r\n
      if (unlikely (!buf_append_byte (out, '*')))
	return false;
      if (unlikely (!buf_append_int_as_string (out, (int64_t) num)))
	return false;
      return buf_append_cstr (out, RESP_CRLF);
    }

  if (unlikely (!buf_has_space (out, 1 + 4)))
    return false;

  buf_append_byte (out, SER_ARR);
  buf_append_u32 (out, (uint32_t) num);
  return true;
}

/**
 * @brief Appends an Array marker (SER_ARR) and a 4-byte zero placeholder.
 * NOTE: This is likely Binary Protocol specific. RESP doesn't support easy patching.
 */
HOT size_t
out_arr_begin (Buffer *out)
{
  // Check space for: 1 byte (SER_ARR) + 4 bytes (Placeholder)
  if (unlikely (!buf_has_space (out, 1 + sizeof (uint32_t))))
    return 0;

  buf_append_byte (out, SER_ARR);

  // pos is the current length (start of the 4-byte count placeholder)
  size_t pos = buf_len (out);

  // Append placeholder (0)
  buf_append_u32 (out, 0);

  return pos;
}

/**
 * @brief Patches the array element count into the placeholder.
 */
HOT bool
out_arr_end (Buffer *out, size_t pos, size_t num)
{
  if (unlikely
      (!out || pos == 0 || (pos + sizeof (uint32_t) > buf_len (out))))
    {
      return false;
    }

  uint8_t *data = out->data;

  // Validate that pos-1 is the SER_ARR marker
  if (unlikely (data[pos - 1] != SER_ARR))
    {
      return false;
    }

  // Patch in the actual count (Network Byte Order)
  uint32_t n_net = hton_u32 ((uint32_t) num);
  memcpy (&data[pos], &n_net, sizeof (uint32_t));
  return true;
}

HOT bool
out_ok (Buffer *out)
{
  if (out->proto == PROTO_RESP)
    {
      return buf_append_cstr (out, RESP_OK);
    }

  return out_str (out, "OK");
}
