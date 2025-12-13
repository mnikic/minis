#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "out.h"
#include "buffer.h"
#include "common/common.h"


/**
 * @brief Appends a Nil marker (SER_NIL). (1 byte)
 * @return bool True on success, False if buffer overflow occurs.
 */
bool
out_nil (Buffer *out)
{
  // Check space for: 1 byte (SER_NIL)
  if (!buf_has_space (out, 1))
    return false;

  // Space guaranteed, rely on buf_append_byte to succeed.
  buf_append_byte (out, SER_NIL);
  return true;
}

/**
 * @brief Appends a string value with SER_STR marker and length prefix.
 * If val is NULL, it writes SER_NIL.
 * @return bool True on success, False if buffer overflow occurs.
 */
bool
out_str (Buffer *out, const char *val)
{
  if (!val)
    {
      return out_nil (out);
    }

  size_t len = strlen (val);

  if (len > UINT32_MAX)
    {
      // Protocol validation check
      return out_err (out, ERR_UNKNOWN, "String too large");
    }

  // Check space for: 1 byte (SER_STR) + 4 bytes (Length) + N bytes (Data)
  if (!buf_has_space (out, 1 + sizeof (uint32_t) + len))
    return false;

  // Space guaranteed, append without further checks
  uint32_t u32_len = (uint32_t) len;

  buf_append_byte (out, SER_STR);
  buf_append_u32 (out, u32_len);
  buf_append_cstr (out, val);	// We rely on the initial check to guarantee this succeeds

  return true;
}

/**
 * @brief Appends a string value with SER_STR marker and length prefix, using a specific size.
 * If string is NULL or size is 0, it writes SER_NIL.
 * @return bool True on success, False if buffer overflow occurs.
 */
bool
out_str_size (Buffer *out, const char *string, size_t size)
{
  if (!string || size == 0)
    {
      return out_nil (out);
    }

  if (size > UINT32_MAX)
    {
      // Protocol validation check
      return out_err (out, ERR_UNKNOWN, "String too large");
    }

  // Check space for: 1 byte (SER_STR) + 4 bytes (Length) + N bytes (Data)
  if (!buf_has_space (out, 1 + sizeof (uint32_t) + size))
    return false;

  // Space guaranteed, append without further checks
  uint32_t len = (uint32_t) size;

  buf_append_byte (out, SER_STR);
  buf_append_u32 (out, len);
  buf_append_bytes (out, string, len);

  return true;
}

/**
 * @brief Appends an Int marker (SER_INT) and a 64-bit integer value. (9 bytes total)
 * @return bool True on success, False if buffer overflow occurs.
 */
bool
out_int (Buffer *out, int64_t val)
{
  // Check space for: 1 byte (SER_INT) + 8 bytes (int64_t)
  if (!buf_has_space (out, 1 + sizeof (int64_t)))
    return false;

  // Space guaranteed, append without further checks
  buf_append_byte (out, SER_INT);
  buf_append_i64 (out, val);

  return true;
}

/**
 * @brief Appends a Double marker (SER_DBL) and a double value. (9 bytes total)
 * @return bool True on success, False if buffer overflow occurs.
 */
bool
out_dbl (Buffer *out, double val)
{
  // Check space for: 1 byte (SER_DBL) + 8 bytes (double)
  if (!buf_has_space (out, 1 + sizeof (double)))
    return false;

  // Space guaranteed, append without further checks
  buf_append_byte (out, SER_DBL);
  buf_append_double (out, val);

  return true;
}

/**
 * @brief Appends an Error marker (SER_ERR), error code, message length, and message.
 * @return bool True on success, False if buffer overflow occurs.
 */
bool
out_err (Buffer *out, int32_t code, const char *msg)
{
  if (!msg)
    {
      msg = "";
    }

  size_t msg_len = strlen (msg);

  if (msg_len > UINT32_MAX)
    {
      // If the error message itself is too long for the protocol, we must truncate or fail.
      // For simplicity, we'll return false here as truncating is complex.
      return false;
    }

  // Check space for: 1 byte (SER_ERR) + 4 bytes (Code) + 4 bytes (Msg Length) + N bytes (Msg Data)
  if (!buf_has_space
      (out, 1 + sizeof (uint32_t) + sizeof (uint32_t) + msg_len))
    return false;

  // Space guaranteed, append without further checks
  uint32_t u32_code = (uint32_t) code;
  uint32_t u32_len = (uint32_t) msg_len;

  buf_append_byte (out, SER_ERR);
  buf_append_u32 (out, u32_code);
  buf_append_u32 (out, u32_len);
  buf_append_cstr (out, msg);

  return true;
}

/**
 * @brief Appends an Array marker (SER_ARR) and the number of elements. (5 bytes total)
 * @return bool True on success, False if buffer overflow occurs.
 */
bool
out_arr (Buffer *out, size_t num)
{
  // Check space for: 1 byte (SER_ARR) + 4 bytes (Count)
  if (!buf_has_space (out, 1 + sizeof (uint32_t)))
    return false;

  // Space guaranteed, append without further checks
  buf_append_byte (out, SER_ARR);
  buf_append_u32 (out, (uint32_t) num);

  return true;
}

/**
 * @brief Appends an Array marker (SER_ARR) and a 4-byte zero placeholder. (5 bytes total)
 * @return size_t The position (index) in the buffer where the count placeholder starts (for patching), or 0 on failure.
 */
size_t
out_arr_begin (Buffer *out)
{
  // Check space for: 1 byte (SER_ARR) + 4 bytes (Placeholder)
  if (!buf_has_space (out, 1 + sizeof (uint32_t)))
    return 0;			// Failure, 0 is an invalid successful position

  // Space guaranteed, append without further checks
  buf_append_byte (out, SER_ARR);

  // pos is the current length, which is the start index of the 4-byte count placeholder
  size_t pos = buf_len (out);

  // Append 4 bytes of placeholder (0)
  buf_append_u32 (out, 0);

  return pos;
}

/**
 * @brief Patches the array element count into the placeholder written by out_arr_begin.
 * @param pos The position returned by out_arr_begin.
 * @param num The final count of elements in the array.
 * @return bool True on success, False on invalid position or state.
 */
bool
out_arr_end (Buffer *out, size_t pos, size_t num)
{
  // Validation checks remain the same as this is a patching operation, not an append.
  if (!out || pos == 0 || (pos + sizeof (uint32_t) > buf_len (out)))
    {
      return false;
    }

  uint8_t *data = out->data;

  // Validate that pos-1 is the SER_ARR marker
  if (data[pos - 1] != SER_ARR)
    {
      return false;
    }

  // Patch in the actual count
  // Must convert to Network Byte Order (Big-Endian) manually for memcpy
  uint32_t n_net = hton_u32 ((uint32_t) num);

  memcpy (&data[pos], &n_net, sizeof (uint32_t));
  return true;
}
