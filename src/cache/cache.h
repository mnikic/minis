/*
 * cache.h
 *
 *  Created on: May 20th, 2024
 *      Author: loshmi
 */
#ifndef CACHE_H
#define  CACHE_H

#include <stdint.h>
#include <stdbool.h>

#include "cache/minis.h"
#include "io/buffer.h"

typedef Minis Cache;

// Lifecycle wrappers (mapped to minis_*)
Cache *cache_init (void);
void cache_free (Cache * cache);
void cache_evict (Cache * cache, uint64_t now_us);
uint64_t cache_next_expiry (Cache * cache);

// The Main Entry Point for the Server. Arguments are NUL terminated, and
// lens gives their real lengths, as they may hold NUL bytes too.
bool cache_execute_n (Cache * cache, const char **cmd, const size_t *lens,
		      size_t size, Buffer * out, uint64_t now_us);

// cache_execute_n for arguments that are C strings
bool cache_execute (Cache * cache, const char **cmd, size_t size,
		    Buffer * out, uint64_t now_us);
uint64_t cache_dirty_count (Cache * cache);
// See minis_prefetch_slot and minis_prefetch_entry
void cache_prefetch_slot (Cache * cache, uint64_t hcode);
void cache_prefetch_entry (Cache * cache, uint64_t hcode);

typedef enum
{
  BPOP_DONE,			// A reply (element or error) is in out
  BPOP_BLOCK,			// Nothing to pop, out is untouched
  BPOP_FAIL			// The reply did not fit in out
} BPopResult;

// BLPOP/BRPOP key [key ...] timeout, without the waiting: pops from the
// first non-empty list or returns BPOP_BLOCK with *out_timeout_us set (0
// is forever). A retry, when the client already waits, skips keys of
// other types instead of failing with WRONGTYPE, so that a client whose
// key was overwritten keeps waiting, like in Redis. cache_execute runs
// these with a timeout that is already over.
BPopResult cache_bpop (Cache * cache, const char **cmd, const size_t *lens,
		       size_t size, bool retry, Buffer * out, uint64_t now_us,
		       uint64_t * out_timeout_us);

// Changes whenever a list push happens
uint64_t cache_list_pushes (Cache * cache);

bool cache_load (Cache * cache, const char *dir, uint64_t now_us);
bool cache_save (Cache * cache, const char *dir, uint64_t now_us);

#endif /* CACHE_H */
