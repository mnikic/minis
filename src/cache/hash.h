/*
 * hash.h
 *
 *  Created on: Dec 28th, 2025
 *      Author: loshmi
 */

#ifndef  HASH_H_
#define   HASH_H_

#include <stddef.h>
#include <stdint.h>

#include "cache/hashtable.h"

// Fields and values may hold any bytes, NUL included; both are also NUL
// terminated for C string readers. The field is in the entry's own
// block, and so is a short value, in room after the field.
typedef struct HashEntry
{
  HNode node;
  char *value;
  size_t value_len;
  size_t field_len;
  uint32_t value_cap;		// The room for a value in the block, or 0
  char field[];
} HashEntry;

HashEntry *hash_lookup (HMap * hmap, const char *field, size_t field_len);

HashEntry *fetch_hash_entry (HNode * node);

typedef enum
{
  HASH_OOM,			// Out of memory, nothing changed
  HASH_UNCHANGED,		// The field already had this value
  HASH_UPDATED,			// The field was there, its value changed
  HASH_ADDED			// A new field
} HashSetResult;

HashSetResult hash_set (HMap * hmap, const char *field, size_t field_len,
			const char *value, size_t value_len);

int hash_del (HMap * hmap, const char *field, size_t field_len);

void hash_dispose (HMap * hmap);

#endif // HASH_H_
