#ifndef _MINIS_H_
#define _MINIS_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "cache/entry.h"
#include "cache/hash.h"
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
typedef bool (*MinisHashCb) (const HashEntry * entry, void *ctx);

// For ZQUERY
typedef void (*MinisZSetCb) (const char *member, size_t len, double score,
			     void *ctx);

typedef bool (*MinisOneEntryVisitor) (const Entry * entry, void *ctx);

// For list replies: element idx of total, len bytes that may hold NUL
// bytes. Return false to stop.
typedef bool (*MinisListCb) (const char *val, size_t len, size_t idx,
			     size_t total, void *ctx);

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
MinisError
minis_mdel (Minis * minis, const char **keys, size_t count,
	    uint64_t * out_deleted, uint64_t now_us);


MinisError
minis_exists (Minis * minis, const char **keys, size_t nkeys,
	      int64_t * out_exists, uint64_t now_us);
MinisError minis_expire (Minis * minis, const char *key, int64_t ttl_ms,
			 int *out_set, uint64_t now_us);
MinisError minis_ttl (Minis * minis, const char *key, int64_t * out_ttl_ms,
		      uint64_t now_us);
MinisError minis_keys (Minis * minis, const char *pattern, MinisKeyCb cb,
		       void *ctx, uint64_t now_us);

// SCAN: visits about count keys from cursor (0 starts a new scan) and
// sets *out_cursor to where to go on, 0 when done. pattern (or NULL)
// and type (or -1) filter the keys passed to cb, but count counts all
// keys looked at, like Redis. Read only, so two calls with the same
// arguments see the same keys if nothing changed in between.
MinisError minis_scan (Minis * minis, uint64_t cursor, size_t count,
		       const char *pattern, int type, MinisKeyCb cb,
		       void *ctx, uint64_t now_us, uint64_t * out_cursor);

// Persistence
MinisError minis_save (Minis * minis, const char *base_dir, uint64_t now_us);
MinisError minis_load (Minis * minis, const char *base_dir, uint64_t now_us);

// Saves the shards changed since their last save to base_dir, in the
// files minis_save writes and minis_load reads
void minis_incremental_save (Minis * minis, const char *base_dir,
			     uint64_t now_us);

uint64_t minis_dirty_count (Minis * minis);

// How many LPUSH/RPUSH calls have added elements so far. Blocked BLPOP
// clients only need another look when this moves.
uint64_t minis_list_pushes (Minis * minis);

#endif // _MINIS_H_
