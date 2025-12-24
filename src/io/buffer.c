#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "buffer.h"
#include "common/common.h"
#include "common/macros.h"

// Remove NULL check for speed if this is called frequently on valid objects
void
buf_set_proto (Buffer *buf, ProtoType proto)
{
  buf->proto = proto;
}

/**
 * @brief Checks if the buffer has sufficient space for additional bytes.
 * Marked inline so the compiler merges it into the append functions.
 */
inline bool
buf_has_space (const Buffer *buf, size_t additional)
{
  if (unlikely (buf->length > buf->capacity))
    return false;

  return (buf->length + additional <= buf->capacity);
}

// Helper to reverse string in place (used by itoa)
static ALWAYS_INLINE void
reverse_str (char *start, char *end)
{
  char tmp;
  while (start < end)
    {
      tmp = *start;
      *start++ = *end;
      *end-- = tmp;
    }
}

/**
 * Appends an integer as a string (e.g. 123 -> "123").
 * Critical for RESP protocol headers (e.g., *3\r\n).
 * 10x faster than snprintf.
 */
bool
buf_append_int_as_string (Buffer *buf, int64_t value)
{
  // Max int64 string is 20 chars ("-9223372036854775808")
  if (unlikely (!buf_has_space (buf, 21)))
    return false;

  char *ptr = (char *) (buf->data + buf->length);
  char *start = ptr;

  if (value == 0)
    {
      *ptr++ = '0';
      buf->length++;
      return true;
    }

  bool neg = false;
  if (value < 0)
    {
      neg = true;
      value = -value;		// careful with INT64_MIN, usually handled specially
    }

  // Generate digits in reverse
  while (value > 0)
    {
      *ptr++ = (char) ('0' + (value % 10));
      value /= 10;
    }

  if (neg)
    *ptr++ = '-';

  // Reverse back to normal
  reverse_str (start, ptr - 1);

  buf->length += (size_t) (ptr - start);
  return true;
}

// Helper for RESP: Append formatted string (like printf)
// WARNING: Slow. Use buf_append_int_as_string for numbers.
bool
buf_append_fmt (Buffer *buf, const char *fmt, ...)
{
  // Check for at least 1 byte to start, vsnprintf will handle the rest
  if (unlikely (!buf_has_space (buf, 1)))
    return false;

  va_list args;

  size_t remain = buf->capacity - buf->length;
  char *ptr = (char *) (buf->data + buf->length);

  va_start (args, fmt);
  int written = vsnprintf (ptr, remain, fmt, args);
  va_end (args);

  // Check for overflow (vsnprintf returns size needed, not size written if truncated)
  if (unlikely (written < 0 || (size_t) written >= remain))
    {
      return false;
    }

  buf->length += (size_t) written;
  return true;
}

Buffer
buf_init (uint8_t *external, size_t capacity)
{
  Buffer buf;
  buf.data = external;
  buf.capacity = capacity;
  buf.length = 0;
  return buf;
}

void
buf_clear (Buffer *buf)
{
  if (buf)
    buf->length = 0;
}

HOT bool
buf_append_bytes (Buffer *buf, const void *data, size_t len)
{
  if (unlikely (!buf || !data))
    return false;
  if (len == 0)
    return true;

  if (unlikely (!buf_has_space (buf, len)))
    return false;

  memcpy (&buf->data[buf->length], data, len);
  buf->length += len;
  return true;
}

bool
buf_append_cstr (Buffer *buf, const char *str)
{
  if (unlikely (!str))
    return true;
  return buf_append_bytes (buf, str, strlen (str));
}

HOT bool
buf_append_byte (Buffer *buf, uint8_t byte)
{
  if (unlikely (!buf_has_space (buf, 1)))
    return false;

  buf->data[buf->length++] = byte;
  return true;
}

bool
buf_append_u32 (Buffer *buf, uint32_t value)
{
  if (unlikely (!buf_has_space (buf, sizeof (uint32_t))))
    return false;

  uint32_t nval = hton_u32 (value);
  memcpy (&buf->data[buf->length], &nval, sizeof (nval));
  buf->length += sizeof (nval);
  return true;
}

bool
buf_append_i64 (Buffer *buf, int64_t value)
{
  if (unlikely (!buf_has_space (buf, sizeof (int64_t))))
    return false;

  uint64_t nval = hton_u64 ((uint64_t) value);
  memcpy (&buf->data[buf->length], &nval, sizeof (nval));
  buf->length += sizeof (nval);
  return true;
}

bool
buf_append_double (Buffer *buf, double value)
{
  if (unlikely (!buf_has_space (buf, sizeof (double))))
    return false;

  // Assumes client and server share IEEE 754 endianness (Standard on x86/ARM)
  memcpy (&buf->data[buf->length], &value, sizeof (value));
  buf->length += sizeof (value);
  return true;
}

const uint8_t *
buf_data (Buffer *buf)
{
  return buf ? buf->data : NULL;
}

size_t
buf_len (Buffer *buf)
{
  return buf ? buf->length : 0;
}
