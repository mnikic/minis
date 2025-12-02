/*
 * out.c
 *
 * Created on: Jun 15, 2023
 * Author: loshmi
 */
// out.c
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "out.h"
#include "buffer.h"

void
out_nil (Buffer *out)
{
  buf_append_byte (out, SER_NIL);
}

void
out_str (Buffer *out, const char *val)
{
  if (!val)
    {
      out_nil (out);
      return;
    }

  buf_append_byte (out, SER_STR);
  uint32_t len = (uint32_t) strlen (val);
  buf_append_u32 (out, len);
  buf_append_cstr (out, val);
}

void
out_str_size (Buffer *out, const char *s, size_t size)
{
  if (!s || size == 0)
    {
      out_nil (out);
      return;
    }

  if (size > UINT32_MAX)
    {
      // String too large for protocol
      out_err (out, ERR_UNKNOWN, "String too large");
      return;
    }

  buf_append_byte (out, SER_STR);
  uint32_t len = (uint32_t) size;
  buf_append_u32 (out, len);
  buf_append_bytes (out, s, len);
}

void
out_int (Buffer *out, int64_t val)
{
  buf_append_byte (out, SER_INT);
  buf_append_i64 (out, val);
}

void
out_dbl (Buffer *out, double val)
{
  buf_append_byte (out, SER_DBL);
  buf_append_double (out, val);
}

void
out_err (Buffer *out, int32_t code, const char *msg)
{
  if (!msg)
    {
      msg = "";
    }

  buf_append_byte (out, SER_ERR);
  uint32_t len = (uint32_t) strlen (msg);
  buf_append_u32 (out, (uint32_t) code);
  buf_append_u32 (out, len);
  buf_append_cstr (out, msg);
}

void
out_arr (Buffer *out, uint32_t n)
{
  buf_append_byte (out, SER_ARR);
  buf_append_u32 (out, n);
}

size_t
out_arr_begin (Buffer *out)
{
  buf_append_byte (out, SER_ARR);
  size_t pos = buf_len (out);
  buf_append_u32 (out, 0);
  return pos;
}

bool
out_arr_end (Buffer *out, size_t pos, uint32_t n)
{
  // Validate that pos points to an array type tag
  if (pos == 0 || pos > buf_len (out))
    {
      return false;		// Invalid position
    }

  const uint8_t *data = buf_data (out);
  if (data[pos - 1] != SER_ARR)
    {
      return false;		// Not an array at this position
    }

  // Patch in the actual count
  // We aren't using buf_append_u32 here, so we must explicitly convert to Network Byte Order (Big-Endian).
  uint32_t n_net = hton_u32 (n);

  uint8_t *writable = (uint8_t *) data;
  memcpy (&writable[pos], &n_net, sizeof (uint32_t));
  return true;
}
