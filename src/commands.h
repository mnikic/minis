/*
 * commands.h
 *
 *  Created on: May 20th, 2024
 *      Author: loshmi
 */

#ifndef COMMANDS_H
#define  COMMANDS_H

#include "strings.h"
#include "heap.h"
#include "thread_pool.h"
#include "zset.h"

typedef struct {
	// The hashmap for the key -> value thing
	HMap db;

	// timers for TTLs
	Heap heap;

	// the thread pool
	TheadPool tp;
} Storage;

extern Storage* commands_init(void);
extern void commands_evict(Storage *storage, uint64_t now_us);
extern uint64_t commands_next_expiry(Storage* storage);
extern void commands_execute(Storage* str, char **cmd, size_t size, String *out);

#endif /* COMMANDS_H */
