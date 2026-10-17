#ifndef ENTRY_H_
#define ENTRY_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "cache/hashtable.h"
#include "cache/minis_private.h"
#include "cache/zset.h"
#include "cache/list.h"
#include "common/macros.h"
#include "common/common.h"

typedef struct Minis Minis;

typedef enum
{
  T_STR = 0,
  T_ZSET = 1,
  T_HASH = 2,
  T_LIST = 3
} EntryType;

typedef struct entry
{
  HNode node;			// HMap linkage
  EntryType type;		// T_STR, T_ZSET, T_HASH or T_LIST
  uint64_t expire_at_us;	// Expiration time
  size_t heap_idx;		// Heap index for TTL
  size_t val_len;		// T_STR: val's length, it may hold NUL bytes

  union
  {
    char *val;			// Used if type == T_STR, also NUL terminated
    ZSet *zset;			// Used if type == T_ZSET
    HMap *hash;			// Used if type == T_HASH
    List *list;			// Used if type == T_LIST
  };

  // Must be at the very end
  char key[];
} Entry;


Entry *entry_new_zset (Minis * minis, const char *key);
Entry *entry_new_str (Minis * minis, const char *key, const char *val,
		      size_t val_len);
// Replaces a T_STR entry's value. False on OOM, leaving the old one.
bool entry_set_str (Entry * ent, const char *val, size_t val_len);
Entry *entry_new_hash (Minis * minis, const char *key);
Entry *entry_new_list (Minis * minis, const char *key);

Entry *entry_fetch (HNode * node_to_fetch);
Entry *fetch_entry_expiry_aware (Minis * minis, HNode * node,
				 uint64_t now_us);
Entry *fetch_entry_from_heap_ref (size_t *ref);

Entry *entry_lookup (Minis * minis, Shard * shard, const char *key,
		     uint64_t now_us);

bool entry_set_ttl (Minis * minis, uint64_t now_us, Entry * ent,
		    int64_t ttl_ms);
bool entry_set_expiration (Minis * minis, Entry * ent, uint64_t expire_at_us);

void cb_destroy_entry (HNode * node, void *arg);
void entry_del (Minis * minis, Entry * ent, uint64_t now_us);
void entry_dispose_atomic (Minis * minis, Entry * ent);
bool entry_eq_str (HNode * node, const void *key);

#endif // ENTRY_H_
