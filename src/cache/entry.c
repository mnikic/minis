#include <stdlib.h>
#include <string.h>

#include "cache/hash.h"
#include "cache/entry.h"
#include "cache/heap.h"
#include "minis_private.h"
#include "common/macros.h"
#include "common/common.h"

Entry *
entry_fetch (HNode *node_to_fetch)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node_to_fetch, Entry, node);
#pragma GCC diagnostic pop
}

bool
entry_eq_str (HNode *node, const void *key)
{
  Entry *ent = entry_fetch (node);
  const EntryKey *ekey = (const EntryKey *) key;
  return ent->key_len == ekey->len
    && memcmp (ent->key, ekey->key, ekey->len) == 0;
}

bool
entry_set_expiration (Minis *minis, Entry *ent, uint64_t expire_at_us)
{
  ent->expire_at_us = expire_at_us;
  size_t pos = ent->heap_idx;
  if (pos == (size_t) -1)
    {
      HeapItem item;
      item.ref = &ent->heap_idx;
      item.val = expire_at_us;
      return heap_add (&minis->heap, &item);
    }
  heap_get (&minis->heap, pos)->val = expire_at_us;
  heap_update (&minis->heap, pos);
  return true;
}

bool
entry_set_ttl (Minis *minis, uint64_t now_us, Entry *ent, int64_t ttl_ms)
{
  if (ttl_ms < 0 && ent->heap_idx != (size_t) -1)
    {
      (void) heap_remove_idx (&minis->heap, ent->heap_idx);
      ent->heap_idx = (size_t) -1;
      ent->expire_at_us = 0;
    }
  else if (ttl_ms >= 0)
    {
      uint64_t new_expire_at_us = now_us + (uint64_t) (ttl_ms * 1000);
      if (new_expire_at_us == ent->expire_at_us)
	return false;
      return entry_set_expiration (minis, ent, new_expire_at_us);
    }
  return true;
}

static void
entry_destroy_internal (Entry *ent)
{
  if (!ent)
    return;

  switch (ent->type)
    {
    case T_ZSET:
      if (ent->zset)
	{
	  zset_dispose (ent->zset);
	  free (ent->zset);
	}
      break;
    case T_HASH:
      if (ent->hash)
	{
	  hash_dispose (ent->hash);
	  free (ent->hash);
	}
      break;
    case T_LIST:
      if (ent->list)
	{
	  list_dispose (ent->list);
	  free (ent->list);
	}
      break;
    case T_STR:
      if (ent->val)
	free (ent->val);
      break;
    default:
      break;
    }

  free (ent);
}

static void
entry_del_async (void *arg)
{
  entry_destroy_internal ((Entry *) arg);
}

void
entry_del (Minis *minis, Entry *ent, uint64_t now_us)
{
  entry_set_ttl (minis, now_us, ent, -1);

  const size_t k_large_container_size = 10000;
  bool too_big = false;

  switch (ent->type)
    {
    case T_ZSET:
      if (ent->zset)
	too_big = hm_size (&ent->zset->hmap) > k_large_container_size;
      break;
    case T_HASH:
      if (ent->hash)
	too_big = hm_size (ent->hash) > k_large_container_size;
      break;
    case T_LIST:
      if (ent->list)
	too_big = list_len (ent->list) > k_large_container_size;
      break;
    case T_STR:
      // Fallthrough
    default:
      break;
    }

  if (too_big)
    thread_pool_queue (&minis->tp, &entry_del_async, ent);
  else
    entry_destroy_internal (ent);
}

void
entry_dispose_atomic (Minis *minis, Entry *ent)
{
  int shard_id = get_shard_id (ent->key, ent->key_len);

  uint64_t hcode = str_hash ((const uint8_t *) ent->key, ent->key_len);
  EntryKey ekey = {.key = ent->key,.len = ent->key_len };
  HNode *removed =
    hm_pop (&minis->shards[shard_id].db, &ekey, hcode, &entry_eq_str);

  if (removed)
    entry_del (minis, ent, (uint64_t) - 1);
}

Entry *
fetch_entry_expiry_aware (Minis *minis, HNode *node, uint64_t now_us)
{
  Entry *entry = entry_fetch (node);
  if (entry->expire_at_us != 0 && entry->expire_at_us < now_us)
    {
      entry_dispose_atomic (minis, entry);
      return NULL;
    }
  return entry;
}

Entry *
entry_lookup (Minis *minis, Shard *shard, const char *key, size_t key_len,
	      uint64_t now_us)
{
  uint64_t hcode = str_hash ((const uint8_t *) key, key_len);
  EntryKey ekey = {.key = key,.len = key_len };
  HNode *node = hm_lookup (&shard->db, &ekey, hcode, &entry_eq_str);

  if (!node)
    return NULL;

  return fetch_entry_expiry_aware (minis, node, now_us);
}

static Entry *
entry_new (Minis *minis, const char *key, size_t klen, EntryType type)
{

  Entry *ent = malloc (sizeof (Entry) + klen + 1);
  if (!ent)
    return NULL;
  memcpy (ent->key, key, klen);
  ent->key[klen] = '\0';
  ent->key_len = klen;

  ent->node.hcode = str_hash ((const uint8_t *) key, klen);
  ent->type = type;
  ent->heap_idx = (size_t) -1;
  ent->expire_at_us = 0;

  ent->val = NULL;
  ent->val_len = 0;
  EntryKey ekey = {.key = key,.len = klen };
  hm_insert (&minis->shards[get_shard_id (key, klen)].db, &ent->node, &ekey,
	     &entry_eq_str);

  return ent;
}

Entry *
entry_new_zset (Minis *minis, const char *key, size_t key_len)
{
  Entry *ent = entry_new (minis, key, key_len, T_ZSET);
  if (!ent)
    return NULL;

  ent->zset = calloc (1, sizeof (ZSet));
  if (!ent->zset)
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }
  return ent;
}

bool
entry_set_str (Entry *ent, const char *val, size_t val_len)
{
  char *copy = malloc (val_len + 1);
  if (!copy)
    return false;
  memcpy (copy, val, val_len);
  copy[val_len] = '\0';
  free (ent->val);
  ent->val = copy;
  ent->val_len = val_len;
  return true;
}

Entry *
entry_new_str (Minis *minis, const char *key, size_t key_len,
	       const char *val, size_t val_len)
{
  Entry *ent = entry_new (minis, key, key_len, T_STR);
  if (!ent)
    return NULL;

  if (!entry_set_str (ent, val, val_len))
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }
  return ent;
}

Entry *
entry_new_hash (Minis *minis, const char *key, size_t key_len)
{
  Entry *ent = entry_new (minis, key, key_len, T_HASH);
  if (!ent)
    return NULL;

  ent->hash = calloc (1, sizeof (HMap));
  if (!ent->hash)
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }

  if (!hm_init (ent->hash, 0))
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }
  return ent;
}

Entry *
entry_new_list (Minis *minis, const char *key, size_t key_len)
{
  Entry *ent = entry_new (minis, key, key_len, T_LIST);
  if (!ent)
    return NULL;

  ent->list = malloc (sizeof (List));
  if (!ent->list)
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }
  list_init (ent->list);
  return ent;
}

Entry *
fetch_entry_from_heap_ref (size_t *ref)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (ref, Entry, heap_idx);
#pragma GCC diagnostic pop
}

void
cb_destroy_entry (HNode *node, void *arg)
{
  (void) arg;
  Entry *ent = entry_fetch (node);
  entry_destroy_internal (ent);
}
