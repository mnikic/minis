#ifndef _MINIS_PRIVATE_H_
#define _MINIS_PRIVATE_H_

// Include internal dependencies needed for the struct layout
#include "cache/hashtable.h"
#include "cache/heap.h"
#include "cache/thread_pool.h"
#include "common/lock.h"
#include "common/macros.h"
#include "common/common.h"
#include <stdint.h>

#define NUM_SHARDS 16
#define SHARD_MASK 15
#define CACHE_LINE_SIZE 64

typedef struct
{
  HMap db;
  ENGINE_LOCK_T lock;
  uint64_t dirty_count;
}
__attribute__((aligned (64))) Shard;

struct Minis
{
  Shard shards[NUM_SHARDS];
  Heap heap;
  ThreadPool tp;
  ENGINE_LOCK_T heap_lock;
};

static ALWAYS_INLINE int
get_shard_id (const char *key)
{
  uint64_t hash = cstr_hash (key);
  return hash & SHARD_MASK;
}

static ALWAYS_INLINE void
lock_shard (Shard *shard)
{
  (void) shard;
  ENGINE_LOCK (&shard->lock);
}

static ALWAYS_INLINE Shard *
lock_shard_for_key (struct Minis *minis, const char *key)
{
  Shard *shard = &minis->shards[get_shard_id (key)];
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
unlock_shard_for_key (struct Minis *minis, const char *key)
{
  (void) minis;
  (void) key;
  ENGINE_UNLOCK (&minis->shards[get_shard_id (key)].lock);
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
lock_shards_batch (struct Minis *minis, const char **keys, size_t count,
		   size_t stride, int *out_shards_buf)
{
  (void) minis;
  (void) count;
  (void) keys;
  (void) stride;
  (void) out_shards_buf;
#ifdef MINIS_EMBEDDED
  size_t num_shards = 0;
  uint16_t shard_map = 0;

  // Generic loop handling both MSET (stride 2) and MGET/MDEL (stride 1)
  for (size_t i = 0; i < count; i += stride)
    {
      int shard_id = get_shard_id (keys[i]);
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

// 2. UNLOCK (Reverse Order)
static ALWAYS_INLINE void
unlock_shards_batch (struct Minis *minis, int *shards, size_t num_shards)
{
#ifdef MINIS_EMBEDDED
  // Unlock in reverse order of acquisition
  size_t i = num_shards;
  while (i > 0)
    unlock_shard (&minis->shards[shards[--i]]);
#endif
}

#endif // _MINIS_PRIVATE_H_
