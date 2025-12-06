// buffer.c
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include "buffer.h"
#include "common/common.h"

static bool
buf_ensure_space (Buffer *buf, size_t additional)
{
  if (!buf)
    return false;

  size_t required = buf->length + additional;
  if (required <= buf->capacity)
    {
      return true;
    }

  size_t new_capacity = buf->capacity * 2;
  if (new_capacity < required)
    {
      new_capacity = required;
    }
  if (new_capacity < 64)
    {
      new_capacity = 64;	// Reasonable minimum for protocol messages
    }

  uint8_t *new_data = realloc (buf->data, new_capacity);
  if (!new_data)
    {
      return false;
    }

  buf->data = new_data;
  buf->capacity = new_capacity;
  return true;
}

Buffer *
buf_new_with_capacity (size_t capacity)
{
  Buffer *buf = malloc (sizeof (Buffer));
  if (!buf)
    {
      return NULL;
    }

  buf->length = 0;
  buf->capacity = capacity > 64 ? capacity : 64;
  buf->data = malloc (buf->capacity);
  if (!buf->data)
    {
      free (buf);
      return NULL;
    }

  return buf;
}

Buffer *
buf_new (void)
{
  return buf_new_with_capacity (64);
}

void
buf_clear (Buffer *buf)
{
  if (buf)
    {
      buf->length = 0;
    }
}

void
buf_free (Buffer *buf)
{
  if (!buf)
    return;
  free (buf->data);
  free (buf);
}

void
buf_append_bytes (Buffer *buf, const void *data, size_t len)
{
  if (!buf || !data || len == 0)
    return;

  if (!buf_ensure_space (buf, len))
    {
      die ("Out of memory in buf_append_bytes");
    }

  memcpy (&buf->data[buf->length], data, len);
  buf->length += len;
}

void
buf_append_cstr (Buffer *buf, const char *str)
{
  if (!buf || !str)
    return;
  buf_append_bytes (buf, str, strlen (str));
}

void
buf_append_byte (Buffer *buf, uint8_t byte)
{
  if (!buf)
    return;

  if (!buf_ensure_space (buf, 1))
    {
      die ("Out of memory in buf_append_byte");
    }

  buf->data[buf->length++] = byte;
}

void
buf_append_u32 (Buffer *buf, uint32_t value)
{
  if (!buf)
    return;

  if (!buf_ensure_space (buf, sizeof (uint32_t)))
    {
      die ("Out of memory in buf_append_u32");
    }

  // Convert to Network Byte Order before copying
  uint32_t nval = hton_u32 (value);

  memcpy (&buf->data[buf->length], &nval, sizeof (uint32_t));
  buf->length += sizeof (uint32_t);
}

void
buf_append_i64 (Buffer *buf, int64_t value)
{
  if (!buf)
    return;

  if (!buf_ensure_space (buf, sizeof (int64_t)))
    {
      die ("Out of memory in buf_append_i64");
    }

  // Convert to Network Byte Order before copying
  uint64_t nval = hton_u64 ((uint64_t) value);

  memcpy (&buf->data[buf->length], &nval, sizeof (int64_t));
  buf->length += sizeof (int64_t);
}

void
buf_append_double (Buffer *buf, double value)
{
  if (!buf)
    return;

  if (!buf_ensure_space (buf, sizeof (double)))
    {
      die ("Out of memory in buf_append_double");
    }

  // Note: Double is copied as-is (host byte order) for simplicity in this protocol.
  memcpy (&buf->data[buf->length], &value, sizeof (double));
  buf->length += sizeof (double);
}

const uint8_t *
buf_data (const Buffer *buf)
{
  return buf ? buf->data : NULL;
}

size_t
buf_len (const Buffer *buf)
{
  return buf ? buf->length : 0;
}
