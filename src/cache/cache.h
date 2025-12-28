/*
 * cache.h
 *
 *  Created on: May 20th, 2024
 *      Author: loshmi
 */

#ifndef CACHE_H
#define  CACHE_H

#include <stdint.h>

#include "io/buffer.h"
#include "cache/zset.h"
#include "cache/hashtable.h"
#include "cache/heap.h"
#include "cache/thread_pool.h"

typedef struct
{
  // The hashmap for the key -> value thing
  HMap db;

  // timers for TTLs
  Heap heap;

  // the thread pool
  ThreadPool tp;

  // count of mutations from startup
  uint64_t dirty_count;
} Cache;

typedef enum
{
  T_STR = 0, T_ZSET = 1,
} EntryType;

typedef struct entry
{
  HNode node;
  char *key;
  char *val;
  EntryType type;
  uint64_t expire_at_us;
  ZSet *zset;
  // for TTLs
  size_t heap_idx;		// Index in the TTL heap. (size_t)-1 means not in heap.
} Entry;



Cache *cache_init (void);
void cache_evict (Cache * cache, uint64_t now_us);
uint64_t cache_next_expiry (Cache * cache);
bool cache_execute (Cache * cache, const char **cmd, size_t size,
		    Buffer * out, uint64_t now_us);
void cache_free (Cache * cache);

bool
entry_set_ttl (Cache * cache, uint64_t now_us, Entry * ent, int64_t ttl_ms);
Entry *entry_new_str (Cache * cache, const char *key, const char *val);
Entry *entry_new_zset (Cache * cache, const char *key);

bool entry_set_expiration (Cache * cache, Entry * ent, uint64_t expire_at_us);
#endif /* CACHE_H */
