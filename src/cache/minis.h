#ifndef _MINIS_H_
#define _MINIS_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "minis_private.h"

typedef struct Minis Minis;

typedef enum
{
  MINIS_OK = 0,
  MINIS_ERR_NIL,		// Key or Field not found
  MINIS_ERR_TYPE,		// WRONGTYPE Operation against a key holding the wrong kind of value
  MINIS_ERR_ARG,		// Invalid argument (parsing failed, integer overflow)
  MINIS_ERR_OOM,		// Out of memory
  MINIS_ERR_UNKNOWN
} MinisError;

// Callback Types (Visitor Pattern)

// For KEYS/SCAN
typedef void (*MinisKeyCb) (const char *key, void *ctx);

// For HGETALL
typedef void (*MinisHashCb) (const char *field, const char *value, void *ctx);

// For ZQUERY
typedef void (*MinisZSetCb) (const char *member, size_t len, double score,
			     void *ctx);

// Lifecycle Management

Minis *minis_init (void);
void minis_free (Minis * minis);

// Perform active expiration cycles
void minis_evict (Minis * minis, uint64_t now_us);

// Get time until next expiration (for sleep/epoll timeout)
uint64_t minis_next_expiry (Minis * minis);

// Basic Key Operations

MinisError minis_del (Minis * minis, const char *key, int *out_deleted,
		      uint64_t now_us);
MinisError minis_exists (Minis * minis, const char *key, int *out_exists,
			 uint64_t now_us);
MinisError minis_expire (Minis * minis, const char *key, int64_t ttl_ms,
			 int *out_set, uint64_t now_us);
MinisError minis_ttl (Minis * minis, const char *key, int64_t * out_ttl_ms,
		      uint64_t now_us);
MinisError minis_keys (Minis * minis, const char *pattern, MinisKeyCb cb,
		       void *ctx, uint64_t now_us);

#endif // _MINIS_H_
