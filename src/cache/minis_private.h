#ifndef MINIS_PRIVATE_H_
#define MINIS_PRIVATE_H_

#include <stdint.h>
#include <stdlib.h>

#include "cache/hashtable.h"
#include "cache/heap.h"
#include "cache/thread_pool.h"
#include "common/lock.h"
#include "common/macros.h"
#include "common/common.h"


#define NUM_SHARDS 16
#define SHARD_MASK 15
#define CACHE_LINE_SIZE 64

typedef struct
{
  HMap db;
  ENGINE_LOCK_T lock;
  uint64_t dirty_count;
}
__attribute__((aligned (CACHE_LINE_SIZE))) Shard;

struct Minis
{
  Shard shards[NUM_SHARDS];
  Heap heap;
  ThreadPool tp;
  ENGINE_LOCK_T heap_lock;
  uint64_t list_pushes;		// Pushes so far, for waking BLPOP clients
  // The server's hint of the running request's key and its hash, see
  // minis_hint_key
  const char *hint_key;
  size_t hint_len;
  uint64_t hint_hcode;
};

// The hash of a key, str_hash, unless it is the hinted one
static ALWAYS_INLINE uint64_t
key_hash (const struct Minis *minis, const char *key, size_t key_len)
{
#ifndef MINIS_EMBEDDED
  if (key == minis->hint_key && key_len == minis->hint_len && key)
    return minis->hint_hcode;
#else
  (void) minis;
#endif
  return str_hash ((const uint8_t *) key, key_len);
}

// Keys may hold NUL bytes, hence their lengths
static ALWAYS_INLINE int
get_shard_id (const char *key, size_t key_len)
{
  uint64_t hash = str_hash ((const uint8_t *) key, key_len);
  return hash & SHARD_MASK;
}

static ALWAYS_INLINE void
lock_shard (Shard *shard)
{
  (void) shard;
  ENGINE_LOCK (&shard->lock);
}

static ALWAYS_INLINE Shard *
lock_shard_for_key (struct Minis *minis, const char *key, size_t key_len)
{
  Shard *shard = &minis->shards[key_hash (minis, key, key_len) & SHARD_MASK];
  ENGINE_LOCK (&shard->lock);
  return shard;
}

static ALWAYS_INLINE void
unlock_shard (Shard *shard)
{
  (void) shard;
  ENGINE_UNLOCK (&shard->lock);
}

static ALWAYS_INLINE void
lock_heap (struct Minis *minis)
{
  (void) minis;
  ENGINE_LOCK (&minis->heap_lock);
}

static ALWAYS_INLINE void
unlock_heap (struct Minis *minis)
{
  (void) minis;
  ENGINE_UNLOCK (&minis->heap_lock);
}

static ALWAYS_INLINE size_t
lock_shards_batch (struct Minis *minis, const char **keys,
		   const size_t *key_lens, size_t count,
		   size_t stride, int *out_shards_buf)
{
  (void) minis;
  (void) count;
  (void) keys;
  (void) key_lens;
  (void) stride;
  (void) out_shards_buf;
#ifdef MINIS_EMBEDDED
  size_t num_shards = 0;
  uint16_t shard_map = 0;

  // Generic loop handling both MSET (stride 2) and MGET/MDEL (stride 1)
  for (size_t i = 0; i < count; i += stride)
    {
      int shard_id = get_shard_id (keys[i], key_lens[i]);
      if (!(shard_map & (1 << shard_id)))
	{
	  shard_map |= (1 << shard_id);
	  out_shards_buf[num_shards++] = shard_id;
	}
    }

  // Sort to prevent deadlocks (Lock Ordering)
  if (num_shards > 1)
    qsort (out_shards_buf, num_shards, sizeof (int), cmp_int);

  // Acquire locks
  for (size_t i = 0; i < num_shards; i++)
    lock_shard (&minis->shards[out_shards_buf[i]]);

  return num_shards;
#else
  return 0;
#endif
}

// Commands with many keys take them in groups of this many, hashed and
// prefetched together by minis_prefetch_keys
#define K_KEY_GROUP 16

// Hashes N <= K_KEY_GROUP keys into HCODES, then prefetches all their
// hash slots and then all their entries, so that the cache misses of the
// keys overlap instead of each key waiting for its own. More than 16 keys
// at once fill the cache before they are used. The keys' shards must be
// locked, or be the server's, whose tables only its main thread changes.
void minis_prefetch_keys (struct Minis *minis, const char **keys,
			  const size_t *key_lens, size_t n, uint64_t *hcodes);

// 2. UNLOCK (Reverse Order)
static ALWAYS_INLINE void
unlock_shards_batch (struct Minis *minis, const int *shards,
		     size_t num_shards)
{
#ifdef MINIS_EMBEDDED
  // Unlock in reverse order of acquisition
  size_t i = num_shards;
  while (i > 0)
    unlock_shard (&minis->shards[shards[--i]]);
#endif
}

#endif // MINIS_PRIVATE_H_
