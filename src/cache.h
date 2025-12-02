/*
 * cache.h
 *
 *  Created on: May 20th, 2024
 *      Author: loshmi
 */

#ifndef CACHE_H
#define  CACHE_H

#include "buffer.h"
#include "heap.h"
#include "thread_pool.h"
#include "zset.h"

typedef struct
{
  // The hashmap for the key -> value thing
  HMap db;

  // timers for TTLs
  Heap heap;

  // the thread pool
  ThreadPool tp;
} Cache;

Cache *cache_init (void);
void cache_evict (Cache * cache, uint64_t now_us);
uint64_t cache_next_expiry (Cache * cache);
void cache_execute (Cache * cache, char **cmd, size_t size, Buffer * out);

#endif /* CACHE_H */
