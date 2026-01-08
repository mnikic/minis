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
minis_get (Minis *minis, const char *key, MinisOneEntryVisitor visitor,
	   void *ctx, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  const Entry *ent = entry_lookup (minis, shard, key, now_us);

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
minis_set_internal (Minis *minis, Shard *shard, const char *key,
		    const char *val, uint64_t now_us)
{
  // Note: Caller must hold the lock for 'shard_id'
  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  // Safe: Dispose grabs Heap Lock. Order Shard->Heap is valid.
	  entry_dispose_atomic (minis, ent);

	  // entry_new_str MUST use shard_id to insert into the correct DB
	  if (!entry_new_str (minis, key, val))
	    return false;
	}
      else
	{
	  if (ent->val && strcmp (ent->val, val) == 0)
	    {
	      if (ent->expire_at_us != 0)
		{
		  ent->expire_at_us = 0;
		  __atomic_fetch_add (&shard->dirty_count, 1,
				      __ATOMIC_RELAXED);
		}
	      return true;
	    }

	  char *new_val = strdup (val);
	  if (!new_val)
	    return false;
	  if (ent->val)
	    free (ent->val);
	  ent->val = new_val;
	  ent->expire_at_us = 0;
	}
    }
  else
    {
      if (!entry_new_str (minis, key, val))
	return false;
    }
  __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
  return true;
}

MinisError
minis_set (Minis *minis, const char *key, const char *val, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  if (!minis_set_internal (minis, shard, key, val, now_us))
    {
      unlock_shard (shard);
      return MINIS_ERR_OOM;
    }

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_mset (Minis *minis, const char **key_vals, size_t total_num,
	    uint64_t now_us)
{
  int shards[NUM_SHARDS];
  // Stride 2 for key-value pairs
  size_t n_locked = lock_shards_batch (minis, key_vals, total_num, 2, shards);

  MinisError err = MINIS_OK;
  for (size_t i = 0; i < total_num; i += 2)
    {
      int shard_id = get_shard_id (key_vals[i]);
      // We already hold the lock, so call internal
      if (!minis_set_internal
	  (minis, &minis->shards[shard_id], key_vals[i], key_vals[i + 1],
	   now_us))
	{
	  err = MINIS_ERR_OOM;
	  break;
	}
    }

  unlock_shards_batch (minis, shards, n_locked);
  return err;
}

MinisError
minis_mget (Minis *minis, const char **keys, size_t count,
	    MinisOneEntryVisitor visitor, void *ctx, uint64_t now_us)
{
  int shards[NUM_SHARDS];
  size_t n_locked = lock_shards_batch (minis, keys, count, 1, shards);

  for (size_t i = 0; i < count; ++i)
    {
      int shard_id = get_shard_id (keys[i]);
      Entry *val =
	entry_lookup (minis, &minis->shards[shard_id], keys[i], now_us);
      if (!visitor (val, ctx))
	break;
    }

  unlock_shards_batch (minis, shards, n_locked);
  return MINIS_OK;
}

MinisError
minis_incr (Minis *minis, const char *key, int64_t delta, int64_t *out_val,
	    uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  Entry *ent = entry_lookup (minis, shard, key, now_us);
  int64_t val = 0;

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  unlock_shard (shard);
	  return MINIS_ERR_TYPE;
	}
      char *endptr = NULL;
      val = strtoll (ent->val, &endptr, 10);
      if (ent->val[0] == '\0' || *endptr != '\0')
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
  if (!ent)
    {
      ent = entry_new_str (minis, key, val_str);
    }
  else
    {
      free (ent->val);
      ent->val = strdup (val_str);
    }

  if (!ent || !ent->val)
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
