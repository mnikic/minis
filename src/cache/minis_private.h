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
  char pad[CACHE_LINE_SIZE -
	   ((sizeof (HMap) + sizeof (ENGINE_LOCK_T)) % CACHE_LINE_SIZE)];
}
Shard;

struct Minis
{
  Shard shards[NUM_SHARDS];
  Heap heap;
  ThreadPool tp;
  uint64_t dirty_count;
  uint64_t last_save_dirty_count;
  ENGINE_LOCK_T heap_lock;
};

static ALWAYS_INLINE int
get_shard_id (const char *key)
{
  uint64_t hash = cstr_hash (key);
  return hash & SHARD_MASK;
}

static ALWAYS_INLINE void
lock_shard (struct Minis *minis, int shard_id)
{
  (void) minis;
  (void) shard_id;
  ENGINE_LOCK (&minis->shards[shard_id].lock);
}

static ALWAYS_INLINE void
lock_shard_for_key (struct Minis *minis, const char *key)
{
  (void) minis;
  (void) key;
  ENGINE_LOCK (&minis->shards[get_shard_id (key)].lock);
}

static ALWAYS_INLINE void
unlock_shard (struct Minis *minis, int shard_id)
{
  (void) minis;
  (void) shard_id;
  ENGINE_UNLOCK (&minis->shards[shard_id].lock);
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

#endif // _MINIS_PRIVATE_H_
