#ifndef _ENTRY_H_
#define _ENTRY_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "cache/hashtable.h"
#include "cache/zset.h"

typedef struct Minis Minis;

typedef enum
{
  T_STR = 0,
  T_ZSET = 1,
  T_HASH = 2
} EntryType;

typedef struct entry
{
  HNode node;
  char *key;
  char *val;
  EntryType type;
  uint64_t expire_at_us;
  ZSet *zset;
  HMap *hash;
  size_t heap_idx;
} Entry;


Entry *entry_new_zset (Minis * minis, const char *key);
Entry *entry_new_str (Minis * minis, const char *key, const char *val);
Entry *entry_new_hash (Minis * minis, const char *key);

Entry *entry_fetch (HNode * node_to_fetch);
Entry *fetch_entry_expiry_aware (Minis * minis, HNode * node,
				 uint64_t now_us);
Entry *fetch_or_create (Minis * minis, const char *key, uint64_t now_us);
Entry *fetch_entry_from_heap_ref (size_t *ref);
Entry *entry_lookup (Minis * minis, const char *key, uint64_t now_us);

bool entry_set_ttl (Minis * minis, uint64_t now_us, Entry * ent,
		    int64_t ttl_ms);
bool entry_set_expiration (Minis * minis, Entry * ent, uint64_t expire_at_us);

void cb_destroy_entry (HNode * node, void *arg);
void entry_del (Minis * minis, Entry * ent, uint64_t now_us);
void entry_dispose_atomic (Minis * minis, Entry * ent);

int entry_eq (HNode * lhs, HNode * rhs);

#endif // _ENTRY_H_
