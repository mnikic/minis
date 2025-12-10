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
#define ERR_MALFORMED 5

#define SER_NIL 0
#define SER_ERR 1		// An error code and message
#define SER_STR 2		// A string
#define SER_INT 3		// A int64
#define SER_DBL 4		// A double
#define SER_ARR 5		// An array

#define DEFAULT_PORT 1234

// Max size of a single request message (payload data after the 4-byte length prefix).
// Currently at 8KB (8192 bytes) to accommodate larger commands.
// This must be consistent between the client and the server's read buffer capacity.
#define K_MAX_MSG (1024 * 1024)
#define K_MAX_ARGS 2048
#define K_MAX_KEY 42
#define K_MAX_VAL (1024 * 7)

#define container_of(ptr, type, member) __extension__ ({                     \
    /* Type check: ensure 'ptr' is a pointer to the member's type */         \
    __typeof__(((type *)0)->member) *__mptr = (ptr);                  \
                                                                            \
    /* Calculate the address of the containing structure. Using char* for */\
    /* byte-level arithmetic is safe and avoids integer cast warnings. */    \
    (type *)((char *)__mptr - offsetof(type, member));                      \
})

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

uint64_t get_monotonic_usec (void);

uint64_t str_hash (const uint8_t * data, size_t len);

__attribute__((noreturn))
     void
     die (const char *msg);

uint16_t
parse_port (int argc, char *argv[]);
uint64_t
htoll (uint64_t number);
uint64_t
ntohll (uint64_t number);

// Portable implementation of Host to Network 32-bit (htonl)
uint32_t
hton_u32 (uint32_t host_val);
// Portable implementation of Host to Network 64-bit
uint64_t
hton_u64 (uint64_t host_val);

     void msgf (const char *fmt, ...);
     void msg (const char *msg);

#endif /* COMMON_H_ */
