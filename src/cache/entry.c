#include <stdlib.h>
#include <string.h>

#include "cache/hash.h"
#include "cache/entry.h"
#include "cache/heap.h"
#include "minis_private.h"
#include "common/macros.h"
#include "common/common.h"

static int
hnode_same (HNode *lhs, HNode *rhs)
{
  return lhs == rhs;
}

Entry *
entry_fetch (HNode *node_to_fetch)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node_to_fetch, Entry, node);
#pragma GCC diagnostic pop
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
      zset_dispose (ent->zset);
      break;
    case T_HASH:
      hash_dispose (ent->hash);
      break;
    case T_STR:
      // fallsthrough
    default:
      break;
    }
  if (ent->key)
    free (ent->key);
  if (ent->val)
    free (ent->val);
  if (ent->hash)
    free (ent->hash);
  if (ent->zset)
    free (ent->zset);
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
      too_big = hm_size (&ent->zset->hmap) > k_large_container_size;
      break;
    case T_HASH:
      too_big = hm_size (ent->hash) > k_large_container_size;
      break;
    case T_STR:
      // fallsthrough
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
  HNode *removed = hm_pop (&minis->db, &ent->node, &hnode_same);
  if (removed)
    {
      entry_del (minis, ent, (uint64_t) - 1);
    }
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

int
entry_eq (HNode *lhs, HNode *rhs)
{
  Entry *lentry = entry_fetch (lhs);
  Entry *rentry = entry_fetch (rhs);
  return lhs->hcode == rhs->hcode
    && (lentry && rentry && lentry->key && rentry->key
	&& strcmp (lentry->key, rentry->key) == 0);
}

Entry *
entry_lookup (Minis *minis, const char *key, uint64_t now_us)
{
  Entry entry_key;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
  entry_key.key = (char *) key;
#pragma GCC diagnostic pop
  entry_key.node.hcode = cstr_hash (key);

  HNode *node = hm_lookup (&minis->db, &entry_key.node, &entry_eq);
  if (!node)
    return NULL;

  return fetch_entry_expiry_aware (minis, node, now_us);
}

static Entry *
entry_new (Minis *minis, const char *key, EntryType type)
{
  Entry *ent = calloc (1, sizeof (Entry));
  if (!ent)
    return NULL;
  ent->key = calloc (strlen (key) + 1, sizeof (char));
  if (!ent->key)
    {
      free (ent);
      return NULL;
    }
  strcpy (ent->key, key);
  ent->node.hcode = str_hash ((const uint8_t *) key, strlen (key));
  ent->type = type;
  ent->heap_idx = (size_t) -1;
  hm_insert (&minis->db, &ent->node, &entry_eq);
  return ent;
}

Entry *
entry_new_zset (Minis *minis, const char *key)
{
  Entry *ent = entry_new (minis, key, T_ZSET);
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

Entry *
entry_new_str (Minis *minis, const char *key, const char *val)
{
  Entry *ent = entry_new (minis, key, T_STR);
  if (!ent)
    return NULL;
  ent->val = calloc (strlen (val) + 1, sizeof (char));
  if (!ent->val)
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }
  strcpy (ent->val, val);
  return ent;
}

Entry *
entry_new_hash (Minis *minis, const char *key)
{
  Entry *ent = entry_new (minis, key, T_HASH);
  if (!ent)
    return NULL;
  ent->hash = calloc (1, sizeof (HMap));
  if (!ent->hash)
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }
  // it will receive the default size if we pass in 0.
  if (!hm_init (ent->hash, 0))
    {
      entry_dispose_atomic (minis, ent);
      return NULL;
    }
  return ent;
}

Entry *
fetch_or_create (Minis *minis, const char *key, uint64_t now_us)
{
  Entry *entry = entry_lookup (minis, key, now_us);
  if (!entry)
    return entry_new_hash (minis, key);
  return entry;
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
