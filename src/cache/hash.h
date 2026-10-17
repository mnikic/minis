/*
 * hash.h
 *
 *  Created on: Dec 28th, 2025
 *      Author: loshmi
 */

#ifndef  HASH_H_
#define   HASH_H_

#include <stddef.h>

#include "cache/hashtable.h"

// Fields and values may hold any bytes, NUL included; both are also NUL
// terminated for C string readers
typedef struct HashEntry
{
  HNode node;
  char *field;
  size_t field_len;
  char *value;
  size_t value_len;
} HashEntry;

HashEntry *hash_lookup (HMap * hmap, const char *field, size_t field_len);

HashEntry *fetch_hash_entry (HNode * node);

// 1 if the field is new, 0 if it was there (or on OOM)
int hash_set (HMap * hmap, const char *field, size_t field_len,
	      const char *value, size_t value_len);

int hash_del (HMap * hmap, const char *field, size_t field_len);

void hash_dispose (HMap * hmap);

#endif // HASH_H_
