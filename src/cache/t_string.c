#include <stdatomic.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cache/minis.h"
#include "cache/entry.h"
#include "cache/minis_private.h"

MinisError
minis_get (Minis *minis, const char *key, size_t key_len, MinisOneEntryVisitor visitor,
	   void *ctx, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);
  const Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_STR)
    {

      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  visitor (ent, ctx);
  unlock_shard (shard);
  return MINIS_OK;
}

static bool
minis_set_internal (Minis *minis, Shard *shard, const char *key, size_t key_len,
		    const char *val, size_t val_len, uint64_t now_us)
{
  // Note: Caller must hold the lock for 'shard_id'
  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  // Safe: Dispose grabs Heap Lock. Order Shard->Heap is valid.
	  entry_dispose_atomic (minis, ent);

	  // entry_new_str MUST use shard_id to insert into the correct DB
	  if (!entry_new_str (minis, key, key_len, val, val_len))
	    return false;
	}
      else
	{
	  if (ent->val_len == val_len && memcmp (ent->val, val, val_len) == 0)
	    {
	      if (ent->expire_at_us != 0)
		{
		  entry_set_ttl (minis, now_us, ent, -1);
		  __atomic_fetch_add (&shard->dirty_count, 1,
				      __ATOMIC_RELAXED);
		}
	      return true;
	    }

	  if (!entry_set_str (ent, val, val_len))
	    return false;
	  entry_set_ttl (minis, now_us, ent, -1);
	}
    }
  else
    {
      if (!entry_new_str (minis, key, key_len, val, val_len))
	return false;
    }
  __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
  return true;
}

MinisError
minis_set (Minis *minis, const char *key, size_t key_len, const char *val, size_t val_len,
	   uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  if (!minis_set_internal (minis, shard, key, key_len, val, val_len, now_us))
    {
      unlock_shard (shard);
      return MINIS_ERR_OOM;
    }

  unlock_shard (shard);
  return MINIS_OK;
}

// SET with a TTL, both under one shard lock. ttl_ms must be > 0.
MinisError
minis_set_ex (Minis *minis, const char *key, size_t key_len, const char *val, size_t val_len,
	      int64_t ttl_ms, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  if (!minis_set_internal (minis, shard, key, key_len, val, val_len, now_us))
    {
      unlock_shard (shard);
      return MINIS_ERR_OOM;
    }

  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);
  entry_set_ttl (minis, now_us, ent, ttl_ms);
  // An unchanged value is not counted by minis_set_internal, the TTL is new
  __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_mset (Minis *minis, const char **key_vals, const size_t *lens,
	    size_t total_num, uint64_t now_us)
{
  int shards[NUM_SHARDS];
  // Stride 2 for key-value pairs
  size_t n_locked =
    lock_shards_batch (minis, key_vals, lens, total_num, 2, shards);

  MinisError err = MINIS_OK;
  for (size_t i = 0; i < total_num; i += 2)
    {
      int shard_id = get_shard_id (key_vals[i], lens[i]);
      // We already hold the lock, so call internal
      if (!minis_set_internal
	  (minis, &minis->shards[shard_id], key_vals[i], lens[i],
	   key_vals[i + 1], lens[i + 1], now_us))
	{
	  err = MINIS_ERR_OOM;
	  break;
	}
    }

  unlock_shards_batch (minis, shards, n_locked);
  return err;
}

MinisError
minis_mget (Minis *minis, const char **keys, const size_t *key_lens,
	    size_t count, MinisOneEntryVisitor visitor, void *ctx,
	    uint64_t now_us)
{
  int shards[NUM_SHARDS];
  size_t n_locked =
    lock_shards_batch (minis, keys, key_lens, count, 1, shards);

  // A group's keys are prefetched, then all looked up and their values
  // prefetched, and only then visited, so that the misses overlap
  bool more = true;
  for (size_t start = 0; more && start < count; start += K_KEY_GROUP)
    {
      size_t n = count - start < K_KEY_GROUP ? count - start : K_KEY_GROUP;
      const char **gkeys = keys + start;
      const size_t *glens = key_lens + start;
      uint64_t hcodes[K_KEY_GROUP];
      Entry *found[K_KEY_GROUP];

      minis_prefetch_keys (minis, gkeys, glens, n, hcodes);
      // A lookup may drop its own key if it expired, never another one,
      // so what the earlier lookups found stays valid
      for (size_t i = 0; i < n; i++)
	{
	  found[i] = entry_lookup_hashed (minis,
					  &minis->shards[shard_of (hcodes[i])],
					  gkeys[i], glens[i], hcodes[i], now_us);
	  // Like Redis, other types read as missing. Visitors use ent->val,
	  // which for them is a pointer to the container.
	  if (found[i] && found[i]->type != T_STR)
	    found[i] = NULL;
	  if (found[i])
	    __builtin_prefetch (found[i]->val);
	}
      for (size_t i = 0; i < n && more; i++)
	more = visitor (found[i], ctx);
    }

  unlock_shards_batch (minis, shards, n_locked);
  return MINIS_OK;
}

MinisError
minis_incr (Minis *minis, const char *key, size_t key_len, int64_t delta, int64_t *out_val,
	    uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);
  int64_t val = 0;

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  unlock_shard (shard);
	  return MINIS_ERR_TYPE;
	}
      // The whole value, NUL bytes included, has to be the number
      char *endptr = NULL;
      val = strtoll (ent->val, &endptr, 10);
      if (ent->val_len == 0 || endptr != ent->val + ent->val_len)
	{
	  unlock_shard (shard);
	  return MINIS_ERR_ARG;
	}
    }

  if ((delta > 0 && val > INT64_MAX - delta)
      || (delta < 0 && val < INT64_MIN - delta))
    {
      unlock_shard (shard);
      return MINIS_ERR_ARG;
    }

  val += delta;
  char val_str[32];
  snprintf (val_str, sizeof (val_str), "%" PRId64, val);

  // Simplified inline set logic
  bool stored;
  if (!ent)
    stored = entry_new_str (minis, key, key_len, val_str, strlen (val_str)) != NULL;
  else
    stored = entry_set_str (ent, val_str, strlen (val_str));

  if (!stored)
    {
      unlock_shard (shard);
      return MINIS_ERR_OOM;
    }

  __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
  if (out_val)
    *out_val = val;

  unlock_shard (shard);
  return MINIS_OK;
}
