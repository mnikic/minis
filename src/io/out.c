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

// ============================================================================
// GENERIC HELPERS
// ============================================================================

HOT bool
out_simple_str (Buffer *out, const char *str)
{
  if (unlikely (!str))
    return out_nil (out);

  if (out->proto == PROTO_RESP)
    {
      // Format: +string\r\n
      if (unlikely (!buf_append_byte (out, '+')))
	return false;
      if (unlikely (!buf_append_cstr (out, str)))
	return false;
      return buf_append_cstr (out, RESP_CRLF);
    }

  // Binary Protocol fallback: Just use a normal string
  return out_str (out, str);
}

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
out_ok (Buffer *out)
{
  return out_simple_str (out, "OK");
}

// ============================================================================
// STRING OUTPUT
// ============================================================================

HOT bool
out_str (Buffer *out, const char *val)
{
  if (unlikely (!val))
    return out_nil (out);

  size_t len = strlen (val);

  if (out->proto == PROTO_RESP)
    {
      // Format: $<len>\r\n<data>\r\n
      if (unlikely (!buf_append_byte (out, '$')))
	return false;
      if (unlikely (!buf_append_int_as_string (out, (int64_t) len)))
	return false;
      if (unlikely (!buf_append_cstr (out, RESP_CRLF)))
	return false;
      if (unlikely (!buf_append_bytes (out, val, len)))
	return false;
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
out_str_size (Buffer *out, const char *string, size_t size)
{
  if (unlikely (!string || size == 0))
    {
      return out_nil (out);
    }

  // Handle RESP manually here to avoid double-strlen calculation
  if (out->proto == PROTO_RESP)
    {
      if (unlikely (!buf_append_byte (out, '$')))
	return false;
      if (unlikely (!buf_append_int_as_string (out, (int64_t) size)))
	return false;
      if (unlikely (!buf_append_cstr (out, RESP_CRLF)))
	return false;
      if (unlikely (!buf_append_bytes (out, string, size)))
	return false;
      return buf_append_cstr (out, RESP_CRLF);
    }

  if (unlikely (size > UINT32_MAX))
    return out_err (out, ERR_UNKNOWN, "String too large");

  if (unlikely (!buf_has_space (out, 1 + sizeof (uint32_t) + size)))
    return false;

  buf_append_byte (out, SER_STR);
  buf_append_u32 (out, (uint32_t) size);
  buf_append_bytes (out, string, size);
  return true;
}

// ============================================================================
// NUMERIC OUTPUT
// ============================================================================

HOT bool
out_int (Buffer *out, int64_t val)
{
  if (out->proto == PROTO_RESP)
    {
      // Format: :<number>\r\n
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

HOT bool
out_dbl (Buffer *out, double val)
{
  // RESP also usually sends doubles as Bulk Strings (e.g. "3.14")
  // because precision can drift in floating point transmission.
  if (out->proto == PROTO_RESP)
    {
      char buf[64];
      // %.17g preserves double precision
      snprintf (buf, sizeof (buf), "%.17g", val);
      return out_str (out, buf);
    }

  if (unlikely (!buf_has_space (out, 1 + sizeof (double))))
    return false;

  buf_append_byte (out, SER_DBL);
  buf_append_double (out, val);
  return true;
}

// ============================================================================
// ARRAY OUTPUT
// ============================================================================

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
 * @brief Begins an array for Binary Protocol Patching.
 * @return 0 if failed OR if PROTO_RESP (Patching not supported in RESP).
 */
HOT size_t
out_arr_begin (Buffer *out)
{
  // SAFETY CHECK: RESP cannot support patching because integers are variable length strings.
  // We force the caller to use the count-first strategy by failing here.
  if (out->proto == PROTO_RESP)
    return 0;

  if (unlikely (!buf_has_space (out, 1 + sizeof (uint32_t))))
    return 0;

  buf_append_byte (out, SER_ARR);
  size_t pos = buf_len (out);
  buf_append_u32 (out, 0);	// Placeholder

  return pos;
}

HOT bool
out_arr_end (Buffer *out, size_t pos, size_t num)
{
  // Double check we aren't in RESP mode (though out_arr_begin should have caught it)
  if (out->proto == PROTO_RESP)
    return false;

  if (unlikely
      (!out || pos == 0 || (pos + sizeof (uint32_t) > buf_len (out))))
    {
      return false;
    }

  uint8_t *data = out->data;

  if (unlikely (data[pos - 1] != SER_ARR))
    {
      return false;
    }

  uint32_t n_net = hton_u32 ((uint32_t) num);
  memcpy (&data[pos], &n_net, sizeof (uint32_t));
  return true;
}
