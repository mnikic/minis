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
#include <string.h>
#include <sys/types.h>

#include "macros.h"

#define ERR_UNKNOWN 1
#define ERR_2BIG 2
#define ERR_TYPE 3
#define ERR_ARG 4
#define ERR_MALFORMED 5

#define SER_NIL 0
#define SER_ERR 1		// An error code and message
#define SER_STR 2		// A string
#define SER_INT 3		// A int64
#define SER_DBL 4		// A double
#define SER_ARR 5		// An array

#define DEFAULT_PORT 1234

// Max size of a single request message (payload data after the 4-byte length prefix).
// This must be consistent between the client and the server's read buffer capacity.
#define K_MAX_MSG (200 * 1024UL)
// Number of slots in the ring buffer
#define K_SLOT_COUNT 512UL

#define K_ZEROCPY_THRESHOLD (100 * 1024)	// when to use MSG_ZEROCOPY

#define K_MAX_ARGS 1024
#define K_MAX_KEY 42
#define K_MAX_VAL (1024 * 7)
#define K_RBUF_SIZE (4UL + K_MAX_MSG + 1UL)
#define K_WBUF_SIZE (512UL * 1024UL)

#define MAX_CONNECTIONS 20000

// Android/Bionic often misses these flags despite Kernel support (4.14+).
// We manually define them to the stable Linux ABI values.

#ifndef MSG_ZEROCOPY
#define MSG_ZEROCOPY    0x4000000
#endif

#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY     60
#endif

#ifndef SO_EE_ORIGIN_ZEROCOPY
#define SO_EE_ORIGIN_ZEROCOPY  5
#endif

#ifdef DEBUG_LOGGING
    // If DEBUG_LOGGING is defined, the macros call the implementation functions.
#define DBG_LOG(msg_str) msg (msg_str)
#define DBG_LOGF(...) msgf (__VA_ARGS__)
#else
    // If DEBUG_LOGGING is NOT defined, the macros resolve to a no-op statement.
    // The use of (void)0 ensures zero overhead (no function call, no parameter evaluation).
#define DBG_LOG(msg_str) (void)0
#define DBG_LOGF(...) (void)0
#endif

void COLD msgf (const char *fmt, ...);
void COLD msg (const char *msg);

#ifdef K_ENABLE_BENCHMARK
#include <time.h>

static uint64_t req_count;
static uint64_t do_request_us;
static uint64_t cache_execute_us;
static uint64_t try_flush_buffer_us;
static uint64_t connection_io_us;

static inline uint64_t
get_time_usec (void)
{
  struct timespec tvs = { 0, 0 };
  clock_gettime (CLOCK_MONOTONIC, &tvs);
  return (uint64_t) ((tvs.tv_sec * 1000000) + (tvs.tv_nsec / 1000));
}

static inline void
record_time (const char *label, uint64_t micros)
{
  if (strcmp (label, "do_request") == 0)
    {
      do_request_us += micros;
    }
  else if (strcmp (label, "cache_execute") == 0)
    {
      cache_execute_us += micros;
    }
  else if (strcmp (label, "try_flush_buffer") == 0)
    {
      try_flush_buffer_us += micros;
    }
  else if (strcmp (label, "connection_io") == 0)
    {
      connection_io_us += micros;
    }
  else
    msgf ("unknown label is %s, %u", label, micros);
  req_count += 1;
}

static inline void
dump_stats (void)
{
  msgf ("time spend in do_request %u us", do_request_us);
  msgf ("time spend in cache_execute %u us", cache_execute_us);
  msgf ("time spend in try_flush_buffer %u us", try_flush_buffer_us);
  msgf ("time spend in connection_io %u us", connection_io_us);
  msgf ("total requests %u", req_count);
}

#define TIME_EXPR(label, expr) __extension__ ({\
  uint64_t __start = get_time_usec ();  \
  __auto_type __ret = (expr);\
  record_time(label,get_time_usec () - __start);\
  __ret; \
})
#define TIME_STMT(label, stmt) __extension__ ({\
  uint64_t __start = get_monotonic_usec ();  \
  (stmt);\
  record_time(label,get_monotonic_usec () - __start);\
})
#else // K_ENABLE_BENCHMARK is NOT defined

static inline void
record_time (const char *label, uint64_t nanos)
{
  (void) label;
  (void) nanos;
}

static inline void
dump_stats (void)
{
}

#define TIME_EXPR(label, expr) (expr)
#define TIME_STMT(label, stmt) (stmt)

#endif // K_ENABLE_BENCHMARK
uint64_t str_hash (const uint8_t * data, size_t len);

NORETURN COLD void die (const char *msg);

COLD uint16_t parse_port (int argc, char *argv[]);

uint64_t htoll (uint64_t number);
uint64_t ntohll (uint64_t number);

// Portable implementation of Host to Network 32-bit (htonl)
uint32_t hton_u32 (uint32_t host_val);
// Portable implementation of Host to Network 64-bit
uint64_t hton_u64 (uint64_t host_val);

#endif /* COMMON_H_ */
