#ifndef BUFFER_H
#define BUFFER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "io/proto_defs.h"
#include "common/macros.h"
/**
 * @brief Fixed-capacity Buffer structure.
 * * This buffer uses externally owned memory (data) and capacity. 
 * It does not perform any memory allocation or reallocation.
 * All append operations check capacity and return false on overflow.
 */
typedef struct
{
  uint8_t *data;		// externally owned memory
  size_t capacity;		// total available bytes
  size_t length;		// current usage (where the next write starts)
  ProtoType proto;
} Buffer;

void buf_set_proto (Buffer * buf, ProtoType proto);

bool buf_append_fmt (Buffer * buf, const char *fmt, ...);

bool
buf_append_int_as_string (Buffer *buf, int64_t value);

/**
 * @brief Returns true is this buffer can accomodate enough additional bytes.
 */
bool buf_has_space (const Buffer * buf, size_t additional);

/**
 * @brief Constructs a Buffer using external memory.
 * * @param external Pointer to the memory block.
 * @param capacity Total size of the memory block in bytes.
 * @return Buffer Initialized buffer struct.
 */
Buffer buf_init (uint8_t * external, size_t capacity);

/**
 * @brief Resets the logical length of the buffer to 0.
 * * @param buf Pointer to the Buffer.
 */
void buf_clear (Buffer * buf);

// Append functions that return false if capacity is exceeded.
HOT bool buf_append_bytes (Buffer * buf, const void *data, size_t len);
bool buf_append_cstr (Buffer * buf, const char *str);
HOT bool buf_append_byte (Buffer * buf, uint8_t byte);
bool buf_append_u32 (Buffer * buf, uint32_t value);
bool buf_append_i64 (Buffer * buf, int64_t value);
bool buf_append_double (Buffer * buf, double value);

/**
 * @brief Gets the underlying data pointer.
 */
const uint8_t *buf_data (Buffer * buf);

/**
 * @brief Gets the current length (used bytes) of the buffer.
 */
size_t buf_len (Buffer * buf);

#endif
