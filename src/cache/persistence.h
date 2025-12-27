/*
 * avl.h
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#ifndef PERSISTENCE_H_
#define  PERSISTENCE_H_

#include <stdbool.h>

#include "cache/cache.h"

// Save the entire cache to a file (Atomic RDB style)
bool cache_save_to_file(Cache *cache, const char *filename);

// Load the cache from a file (clears existing data if needed)
bool cache_load_from_file(Cache *cache, const char *filename);

#endif // PERSISTENCE_H_ 
