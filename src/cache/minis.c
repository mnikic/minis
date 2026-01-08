#include "cache/minis.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <assert.h>
#include <stdatomic.h>

#include "cache/hashtable.h"
#include "cache/heap.h"
#include "cache/entry.h"
#include "cache/minis_private.h"
#include "cache/thread_pool.h"
#include "cache/persistence.h"
#include "common/common.h"
#include "common/glob.h"
#include "common/lock.h"

Minis *
minis_init (void)
{
  Minis *minis = calloc (1, sizeof (Minis));
  if (!minis)
    die ("Out of memory in minis_init");

  for (int i = 0; i < NUM_SHARDS; i++)
    {
      if (!hm_init (&minis->shards[i].db, 1))
	die ("Out of memory in hm_init");
      ENGINE_LOCK_INIT (&minis->shards[i].lock);
    }

  thread_pool_init (&minis->tp, 4);
  heap_init (&minis->heap);
  ENGINE_LOCK_INIT (&minis->heap_lock);
  return minis;
}

void
minis_free (Minis *minis)
{
  if (!minis)
    return;

  // No locks needed here (assumed single-threaded shutdown)
  for (int i = 0; i < NUM_SHARDS; i++)
    hm_scan (&minis->shards[i].db, &cb_destroy_entry, NULL);

  thread_pool_destroy (&minis->tp);
  heap_free (&minis->heap);

  for (int i = 0; i < NUM_SHARDS; i++)
    {
      hm_destroy (&minis->shards[i].db);
      ENGINE_LOCK_DESTROY (&minis->shards[i].lock);
    }

  ENGINE_LOCK_DESTROY (&minis->heap_lock);
  free (minis);
}

void
minis_evict (Minis *minis, uint64_t now_us)
{
  const size_t k_max_works = 2000;
  size_t nworks = 0;

  while (nworks < k_max_works)
    {
      char *key_copy = NULL;

      // Lock Heap to peek at the top
      lock_heap (minis);

      if (heap_empty (&minis->heap))
	{
	  unlock_heap (minis);
	  break;
	}

      HeapItem *top = heap_top (&minis->heap);
      if (top->val > now_us)
	{
	  // Not expired yet
	  unlock_heap (minis);
	  break;
	}

      // Found candidate. 
      // CRITICAL: We cannot lock the Shard while holding Heap Lock (Deadlock risk).
      // But if we unlock Heap, the Entry might be freed by another thread.
      // So while we hold Heap Lock, 'del' cannot complete (it waits for Heap Lock).
      // So 'ref' is valid to READ. We copy the key so we can find the shard later.
      Entry *ent = fetch_entry_from_heap_ref (top->ref);
      if (ent)
	key_copy = strdup (ent->key);

      unlock_heap (minis);

      if (!key_copy)
	break;

      // Lock Shard (Outer Lock) and Verify
      Shard *shard = lock_shard_for_key (minis, key_copy);

      // Re-lookup entry. It might have been deleted by user between steps 2 and 3.
      Entry *victim = entry_lookup (minis, shard, key_copy, now_us);

      if (victim && victim->expire_at_us != 0
	  && victim->expire_at_us <= now_us)
	{
	  entry_dispose_atomic (minis, victim);
	  nworks++;
	}

      unlock_shard (shard);
      free (key_copy);
    }
}

uint64_t
minis_next_expiry (Minis *minis)
{
  uint64_t exp = (uint64_t) - 1;
  lock_heap (minis);
  if (!heap_empty (&minis->heap))
    {
      exp = heap_top (&minis->heap)->val;
    }
  unlock_heap (minis);
  return exp;
}

// Internal helper for DEL (Assumes Shard Lock is HELD)
static int
minis_del_internal_locked (Minis *minis, Shard *shard, const char *key,
			   uint64_t now_us)
{
  HNode *node = hm_pop (&shard->db, key, cstr_hash (key), &entry_eq_str);
  int ret_val = 0;

  if (node)
    {
      Entry *entry = entry_fetch (node);
      if (!(entry->expire_at_us != 0 && entry->expire_at_us < now_us))
	{
	  ret_val = 1;
	  __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
	}
      // This will lock Heap internally to remove TTL record
      entry_del (minis, entry, (uint64_t) - 1);
    }

  return ret_val;
}

MinisError
minis_del (Minis *minis, const char *key, int *out_deleted, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  *out_deleted = minis_del_internal_locked (minis, shard, key, now_us);
  unlock_shard (shard);
  return MINIS_OK;
}

typedef struct
{
  Minis *minis;
  MinisKeyCb cb;
  void *ctx;
  const char *pattern;
  uint64_t now_us;
} KeyScanCtx;

static void
cb_scan_keys (HNode *node, void *arg)
{
  KeyScanCtx *ctx = (KeyScanCtx *) arg;
  Entry *ent = fetch_entry_expiry_aware (ctx->minis, node, ctx->now_us);
  if (!ent)
    return;
  if (glob_match (ctx->pattern, ent->key))
    {
      ctx->cb (ent->key, ctx->ctx);
    }
}

MinisError
minis_mdel (Minis *minis, const char **keys, size_t count,
	    uint64_t *out_deleted, uint64_t now_us)
{
  int shards[NUM_SHARDS];
  size_t n_locked = lock_shards_batch (minis, keys, count, 1, shards);

  uint64_t total = 0;
  for (size_t i = 0; i < count; ++i)
    {
      int shard_id = get_shard_id (keys[i]);
      int deleted =
	minis_del_internal_locked (minis, &minis->shards[shard_id], keys[i],
				   now_us);
      total += (uint64_t) deleted;
    }

  unlock_shards_batch (minis, shards, n_locked);

  if (out_deleted)
    *out_deleted = total;
  return MINIS_OK;
}

MinisError
minis_exists (Minis *minis, const char **keys, size_t nkeys,
	      int64_t *out_exists, uint64_t now_us)
{
  // Note: For EXISTS with multiple keys, we have a choice:
  // 1. Lock all involved shards (Atomic snapshot)
  // 2. Lock one by one (Rolling check)
  // Redis EXISTS is atomic, but for simple checking, 'Rolling' is usually acceptable 
  // and much faster/simpler than the MDEL sorting dance.
  // Let's go with rolling checks for concurrency.

  int64_t hits = 0;
  for (size_t i = 0; i < nkeys; ++i)
    {
      Shard *shard = lock_shard_for_key (minis, keys[i]);
      if (entry_lookup (minis, shard, keys[i], now_us))
	hits++;
      unlock_shard (shard);
    }

  if (out_exists)
    *out_exists = hits;

  return MINIS_OK;
}

MinisError
minis_expire (Minis *minis, const char *key, int64_t ttl_ms, int *out_set,
	      uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  Entry *ent = entry_lookup (minis, shard, key, now_us);
  int success = 0;
  if (ent)
    {
      // This internally locks Heap. Safe order: Shard -> Heap.
      entry_set_ttl (minis, now_us, ent, ttl_ms);
      __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
      success = 1;
    }

  unlock_shard (shard);

  if (out_set)
    *out_set = success;
  return MINIS_OK;
}

MinisError
minis_ttl (Minis *minis, const char *key, int64_t *out_ttl_ms,
	   uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  Entry *ent = entry_lookup (minis, shard, key, now_us);
  if (!ent)
    {
      *out_ttl_ms = -2;
    }
  else if (ent->expire_at_us == 0)
    {
      *out_ttl_ms = -1;
    }
  else
    {
      *out_ttl_ms = (int64_t) ((ent->expire_at_us - now_us) / 1000);
    }

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_keys (Minis *minis, const char *pattern, MinisKeyCb cb, void *ctx,
	    uint64_t now_us)
{
  KeyScanCtx kctx = {
    .minis = minis,
    .cb = cb,
    .ctx = ctx,
    .pattern = pattern,
    .now_us = now_us
  };

  // We iterate shards one by one. 
  // We do NOT lock the whole world. This implies the 'keys' result 
  // is not an atomic snapshot, which is standard for high-perf databases.
  for (int i = 0; i < NUM_SHARDS; ++i)
    {
      Shard *shard = &minis->shards[i];
      lock_shard (shard);
      hm_scan (&minis->shards[i].db, &cb_scan_keys, &kctx);
      unlock_shard (shard);
    }
  return MINIS_OK;
}

MinisError
minis_save (Minis *minis, const char *filename, uint64_t now_us)
{
  for (int i = 0; i < NUM_SHARDS; i++)
    lock_shard (&minis->shards[i]);

  bool success = cache_save_to_file (minis, filename, now_us);

  for (int i = 0; i < NUM_SHARDS; i++)
    unlock_shard (&minis->shards[i]);

  return success ? MINIS_OK : MINIS_ERR_UNKNOWN;
}

MinisError
minis_load (Minis *minis, const char *filename, uint64_t now_us)
{
  // Load is exclusive (usually startup), but let's lock for safety.
  for (int i = 0; i < NUM_SHARDS; i++)
    lock_shard (&minis->shards[i]);

  bool success = cache_load_from_file (minis, filename, now_us);

  for (int i = 0; i < NUM_SHARDS; i++)
    unlock_shard (&minis->shards[i]);

  return success ? MINIS_OK : MINIS_ERR_NIL;
}

uint64_t
minis_dirty_count (Minis *minis)
{
  uint64_t count = 0;
  for (int i = 0; i < NUM_SHARDS; ++i)
    count +=
      __atomic_load_n (&minis->shards[i].dirty_count, __ATOMIC_RELAXED);
  return count;
}
