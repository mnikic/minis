/*
 * common.h
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */
#ifndef COMMON_H_
#define COMMON_H_

#include <stdint.h>
#include <stddef.h>
#include <arpa/inet.h>

#define ERR_UNKNOWN 1
#define ERR_2BIG 2
#define ERR_TYPE 3
#define ERR_ARG 4

#define SER_NIL 0
#define SER_ERR 1		// An error code and message
#define SER_STR 2		// A string
#define SER_INT 3		// A int64
#define SER_DBL 4		// A double
#define SER_ARR 5		// An array

#define DEFAULT_PORT 1234

#define K_MAX_MSG 4096
#define K_MAX_ARGS 100
#define container_of(ptr, type, member) __extension__ ({\
    const __typeof__( ((type *)0)->member ) *__mptr = (ptr);\
    (type *)( (char *)__mptr - offsetof(type,member) );})

uint64_t get_monotonic_usec (void);

uint64_t str_hash (const uint8_t * data, size_t len);

void msg (const char *msg);

__attribute__((noreturn))
     void
     die (const char *msg);

uint16_t
parse_port (int argc, char *argv[]);

     static inline uint64_t htoll (uint64_t x)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
  // If the host is little-endian, we must swap the 32-bit halves and then swap
  // the bytes within each half.
  // Explicitly cast to uint32_t before passing to htonl to avoid -Wconversion warnings/errors.
  uint32_t low_part = (uint32_t) x;
  uint32_t high_part = (uint32_t) (x >> 32);

  return (((uint64_t) htonl (low_part)) << 32) | (uint64_t) htonl (high_part);
#else
  // Host is big-endian (network order), so no swap is needed.
  return x;
#endif
}

static inline uint64_t
ntohll (uint64_t x)
{
  // For network-to-host, the logic is the same: swap if the host is little-endian.
  return htoll (x);
}

// Portable implementation of Host to Network 32-bit (htonl)
inline uint32_t
hton_u32 (uint32_t host_val)
{
  // Manual byte swap: 0x12345678 (Host) -> 0x78563412 (Network/BE)
  return ((host_val & 0xFF) << 24) |
    ((host_val & 0xFF00) << 8) |
    ((host_val & 0xFF0000) >> 8) | ((host_val & 0xFF000000) >> 24);
}

// Portable implementation of Host to Network 64-bit
inline uint64_t
hton_u64 (uint64_t host_val)
{
  // Manual byte swap (Host to Big-Endian)
  uint64_t v = 0;
  v |= (host_val & 0x00000000000000FFULL) << 56;
  v |= (host_val & 0x000000000000FF00ULL) << 40;
  v |= (host_val & 0x0000000000FF0000ULL) << 24;
  v |= (host_val & 0x00000000FF000000ULL) << 8;
  v |= (host_val & 0x000000FF00000000ULL) >> 8;
  v |= (host_val & 0x0000FF0000000000ULL) >> 24;
  v |= (host_val & 0x00FF000000000000ULL) >> 40;
  v |= (host_val & 0xFF00000000000000ULL) >> 56;
  return v;
}

#endif /* COMMON_H_ */
