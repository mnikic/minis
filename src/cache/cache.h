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

// The Main Entry Point for the Server
bool cache_execute (Cache * cache, const char **cmd, size_t size,
		    Buffer * out, uint64_t now_us);

#endif /* CACHE_H */
