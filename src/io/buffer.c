// #include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "buffer.h"
#include "common/common.h"


/**
 * @brief Checks if the buffer has sufficient space for additional bytes.
 */
bool
buf_has_space (const Buffer *buf, size_t additional)
{
  // Check for potential overflow before comparison
  if (buf->length > buf->capacity)
    {
      // This case should ideally not happen if buf->length is managed correctly.
      return false;
    }
  return buf->length + additional <= buf->capacity;
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

// ========== APPEND FUNCTIONS ==========

bool
buf_append_bytes (Buffer *buf, const void *data, size_t len)
{
  if (!buf || !data || len == 0)
    return true;

  if (!buf_has_space (buf, len))
    return false;

  memcpy (&buf->data[buf->length], data, len);
  buf->length += len;
  return true;
}

bool
buf_append_cstr (Buffer *buf, const char *str)
{
  if (!str)
    return true;
  return buf_append_bytes (buf, str, strlen (str));
}

bool
buf_append_byte (Buffer *buf, uint8_t byte)
{
  if (!buf_has_space (buf, 1))
    return false;

  buf->data[buf->length++] = byte;
  return true;
}

bool
buf_append_u32 (Buffer *buf, uint32_t value)
{
  if (!buf_has_space (buf, sizeof (uint32_t)))
    return false;

  uint32_t nval = hton_u32 (value);
  memcpy (&buf->data[buf->length], &nval, sizeof (nval));
  buf->length += sizeof (nval);
  return true;
}

bool
buf_append_i64 (Buffer *buf, int64_t value)
{
  if (!buf_has_space (buf, sizeof (int64_t)))
    return false;

  uint64_t nval = hton_u64 ((uint64_t) value);
  memcpy (&buf->data[buf->length], &nval, sizeof (nval));
  buf->length += sizeof (nval);
  return true;
}

bool
buf_append_double (Buffer *buf, double value)
{
  if (!buf_has_space (buf, sizeof (double)))
    return false;

  // Standard practice for double serialization is often to copy the bytes directly
  // or convert to a fixed-size integer representation for network transport.
  // Assuming simple byte-copy for this example (non-portable but fast).
  memcpy (&buf->data[buf->length], &value, sizeof (value));
  buf->length += sizeof (value);
  return true;
}

// ========== ACCESSORS ==========

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
