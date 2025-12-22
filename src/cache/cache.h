/*
 * cache.h
 *
 *  Created on: May 20th, 2024
 *      Author: loshmi
 */

#ifndef CACHE_H
#define  CACHE_H

#include "io/buffer.h"
#include "hashtable.h"
#include "heap.h"
#include "thread_pool.h"
#include <stdint.h>

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
bool cache_execute (Cache * cache, char **cmd, size_t size, Buffer * out,
		    uint64_t now_us);
void cache_free (Cache * cache);

#endif /* CACHE_H */
