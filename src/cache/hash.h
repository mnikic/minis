/*
 * hash.h
 *
 *  Created on: Dec 28th, 2025
 *      Author: loshmi
 */

#ifndef  _HASH_H_
#define   _HASH_H_

#include "cache/hashtable.h"

typedef struct HashEntry
{
  HNode node;
  char *field;
  char *value;
} HashEntry;

HashEntry *hash_lookup (HMap * hmap, const char *field);

HashEntry *fetch_hash_entry (HNode * node);

int hash_set (HMap * hmap, const char *field, const char *value);

int hash_del (HMap * hmap, const char *field);

void hash_dispose (HMap * hmap);

#endif // _HASH_H_
