/*
 * common.h
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */
#ifndef COMMON_H_
#define COMMON_H_

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <arpa/inet.h>
#include <string.h>
#include <sys/types.h>

#include "common/macros.h"

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

#define MINIS_DB_FILE      "dump.mdb"
#define MINIS_DB_MAGIC     "M1NI"
#define MINIS_DB_VERSION   1

// Defaults & Limits
#ifdef MINIS_ANDROID
    // ANDROID / TERMUX PROFILE
    // Tuning for lower RAM and smaller Kernel TCP buffers
#define K_MAX_MSG (64 * 1024UL)	// 64KB max payload (vs 200KB)
#define K_MAX_ARGS 1024
#define K_WBUF_SIZE (256UL * 1024UL)	// 256KB write buffer (vs 2MB)
#define K_SLOT_COUNT 128UL	// Smaller ring buffer (vs 2048)
#else
    // STANDARD SERVER PROFILE
    // Tuning for high-performance Linux Servers
#define K_MAX_MSG (200 * 1024UL)
#define K_MAX_ARGS 1024
#define K_WBUF_SIZE (2048UL * 1024UL)
#define K_SLOT_COUNT 2048UL
#endif
#define K_ZEROCPY_THRESHOLD (100 * 1024)	// when to use MSG_ZEROCOPY

#define K_RBUF_SIZE (4UL + K_MAX_MSG + 1UL)

#define MAX_CONNECTIONS 20000
#define SNAPSHOT_INTERVAL_US (60 * 1000000ULL)

// Android/Bionic often misses these flags despite Kernel support (4.14+).
// We manually define them to the stable Linux ABI values.

#ifndef MSG_ZEROCOPY
#define MSG_ZEROCOPY    0x4000000
#endif

#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY     60
#endif

// Defined in common.c
extern bool g_verbose_mode;

#ifndef SO_EE_ORIGIN_ZEROCOPY
#define SO_EE_ORIGIN_ZEROCOPY  5
#endif

#define DBG_LOGF(...) do { \
    if (unlikely(g_verbose_mode)) { \
        msgf(__VA_ARGS__); \
    } \
} while(0)

#define DBG_LOG(str) do { \
    if (unlikely(g_verbose_mode)) { \
        msg(str); \
    } \
} while(0)

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

HOT static ALWAYS_INLINE uint64_t
hton_u64 (uint64_t host64)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return __builtin_bswap64 (host64);
#else
  return host64;
#endif
}

HOT static ALWAYS_INLINE uint32_t
hton_u32 (uint32_t host32)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return __builtin_bswap32 (host32);
#else
  return host32;
#endif
}

HOT static ALWAYS_INLINE uint64_t
ntoh_u64 (uint64_t net64)
{
  return hton_u64 (net64);
}

HOT static ALWAYS_INLINE uint32_t
ntoh_u32 (uint32_t net32)
{
  return hton_u32 (net32);
}

// Legacy alias if you use this name elsewhere
HOT static ALWAYS_INLINE uint64_t
ntohll (uint64_t number)
{
  return ntoh_u64 (number);
}

#endif /* COMMON_H_ */
