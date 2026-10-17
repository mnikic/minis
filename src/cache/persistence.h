/*
 * persistence.h
 *
 *  Created on: Jun 29, 2025
 *      Author: loshmi
 */
#ifndef PERSISTENCE_H_
#define  PERSISTENCE_H_

#include <stdbool.h>
#include <stdint.h>

#include "cache/minis.h"

// Save the entire cache to a file 
bool cache_save_to_file (const Minis * minis, const char *filename,
			 uint64_t now_us);

bool
minis_save_shard_file (const Shard * shard, const char *filename,
		       uint64_t now_us);

// Load the cache from a file (clears existing data if needed)
bool
cache_load_from_file (Minis * minis, const char *filename, int shard_id_hint,
		      uint64_t now_us);

// Expiries run on the monotonic clock, which restarts at boot, so
// snapshots store them as wall clock time. Tests can swap the wall clock
// (NULL restores the real one) to stage a reboot.
void persistence_set_wall_clock (uint64_t (*wall_clock_us) (void));

#endif // PERSISTENCE_H_
