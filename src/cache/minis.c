#include "minis.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <stdint.h>
#include <assert.h>

#include "cache/hashtable.h"
#include "cache/heap.h"
#include "cache/entry.h"
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
  if (!hm_init (&minis->db, 1))
    die ("Out of memory in hm_init");
  thread_pool_init (&minis->tp, 4);
  heap_init (&minis->heap);
  ENGINE_LOCK_INIT (&minis->lock);
  return minis;
}

void
minis_free (Minis *minis)
{
  if (!minis)
    return;
  hm_scan (&minis->db, &cb_destroy_entry, NULL);
  thread_pool_destroy (&minis->tp);
  heap_free (&minis->heap);
  hm_destroy (&minis->db);
  ENGINE_LOCK_DESTROY (&minis->lock);
  free (minis);
}

void
minis_evict (Minis *minis, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  const size_t k_max_works = 2000;
  size_t nworks = 0;
  while (!heap_empty (&minis->heap) && heap_top (&minis->heap)->val < now_us)
    {
      Entry *ent = fetch_entry_from_heap_ref (heap_top (&minis->heap)->ref);
      entry_dispose_atomic (minis, ent);
      if (nworks++ >= k_max_works)
	break;
    }
  ENGINE_UNLOCK (&minis->lock);
}

uint64_t
minis_next_expiry (Minis *minis)
{
  uint64_t exp = (uint64_t) - 1;
  ENGINE_LOCK (&minis->lock);
  if (!heap_empty (&minis->heap))
    {
      exp = heap_top (&minis->heap)->val;
    }
  ENGINE_UNLOCK (&minis->lock);
  return exp;
}

// --- Basic Operations ---
static void
minis_del_internal (Minis *minis, const char *key, int *out_deleted,
		    uint64_t now_us)
{
  Entry entry_key = entry_dummy (key);
  HNode *node = hm_pop (&minis->db, &entry_key.node, &entry_eq);
  int ret_val = 0;

  if (node)
    {
      Entry *entry = entry_fetch (node);
      // If not expired, count as deleted
      if (!(entry->expire_at_us != 0 && entry->expire_at_us < now_us))
	{
	  ret_val = 1;
	  minis->dirty_count++;
	}
      entry_del (minis, entry, (uint64_t) - 1);	// -1 ensures strict free
    }

  if (out_deleted)
    *out_deleted = ret_val;
}

MinisError
minis_del (Minis *minis, const char *key, int *out_deleted, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  minis_del_internal (minis, key, out_deleted, now_us);
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_mdel (Minis *minis, const char **keys, size_t count,
	    uint64_t *out_deleted, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  uint64_t total = 0;
  for (size_t i = 0; i < count; ++i)
    {
      int deleted = 0;
      minis_del_internal (minis, keys[i], &deleted, now_us);
      total += (uint64_t) deleted;
    }
  if (out_deleted)
    *out_deleted = total;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_exists (Minis *minis, const char *key, int *out_exists, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (out_exists)
    *out_exists = ent ? 1 : 0;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_expire (Minis *minis, const char *key, int64_t ttl_ms, int *out_set,
	      uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  int success = 0;
  if (ent)
    {
      entry_set_ttl (minis, now_us, ent, ttl_ms);
      minis->dirty_count++;
      success = 1;
    }
  if (out_set)
    *out_set = success;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_ttl (Minis *minis, const char *key, int64_t *out_ttl_ms,
	   uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
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
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

// Struct for Keys callback
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
minis_keys (Minis *minis, const char *pattern, MinisKeyCb cb, void *ctx,
	    uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  KeyScanCtx kctx = {
    .minis = minis,
    .cb = cb,
    .ctx = ctx,
    .pattern = pattern,
    .now_us = now_us
  };
  hm_scan (&minis->db, &cb_scan_keys, &kctx);
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_save (Minis *minis, const char *filename, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);

  bool success = cache_save_to_file (minis, filename, now_us);

  if (success)
    {
      // Update dirty tracking so we know we are clean
      minis->last_save_dirty_count = minis->dirty_count;
    }

  ENGINE_UNLOCK (&minis->lock);
  return success ? MINIS_OK : MINIS_ERR_UNKNOWN;
}

MinisError
minis_load (Minis *minis, const char *filename, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  bool success = cache_load_from_file (minis, filename, now_us);

  // On load, dirty counts should match (we are in sync with disk)
  if (success)
    {
      minis->last_save_dirty_count = minis->dirty_count;
    }

  ENGINE_UNLOCK (&minis->lock);
  return success ? MINIS_OK : MINIS_ERR_NIL;	// NIL = File not found/empty
}
