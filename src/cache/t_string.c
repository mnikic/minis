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
  lock_shard_for_key (minis, key);
  const Entry *ent = entry_lookup (minis, get_shard_id (key), key, now_us);

  if (!ent)
    {
      unlock_shard_for_key (minis, key);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_STR)
    {

      unlock_shard_for_key (minis, key);
      return MINIS_ERR_TYPE;
    }

  visitor (ent, ctx);
  unlock_shard_for_key (minis, key);
  return MINIS_OK;
}

static bool
minis_set_internal (Minis *minis, int shard_id, const char *key,
		    const char *val, uint64_t now_us)
{
  // Note: Caller must hold the lock for 'shard_id'
  Entry *ent = entry_lookup (minis, shard_id, key, now_us);

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
		  minis->dirty_count++;
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

  minis->dirty_count++;
  return true;
}

MinisError
minis_set (Minis *minis, const char *key, const char *val, uint64_t now_us)
{
  int shard_id = get_shard_id (key);
  lock_shard (minis, shard_id);

  if (!minis_set_internal (minis, shard_id, key, val, now_us))
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_OOM;
    }

  unlock_shard (minis, shard_id);
  return MINIS_OK;
}

MinisError
minis_mset (Minis *minis, const char **key_vals, size_t total_num,
	    uint64_t now_us)
{
#ifdef MINIS_EMBEDDED
  int shards_to_lock[NUM_SHARDS];
  size_t num_shards = 0;
  uint16_t shard_map = 0;

  // Iterate keys (stepping by 2 because it's key, value, key, value)
  for (size_t i = 0; i < total_num; i += 2)
    {
      int shard_id = get_shard_id (key_vals[i]);
      if (!(shard_map & (1 << shard_id)))
	{
	  shard_map |= (1 << shard_id);
	  shards_to_lock[num_shards++] = shard_id;
	}
    }

  qsort (shards_to_lock, num_shards, sizeof (int), cmp_int);
  for (size_t i = 0; i < num_shards; i++)
    {
      lock_shard (minis, shards_to_lock[i]);
    }
#endif

  MinisError err = MINIS_OK;
  for (size_t i = 0; i < total_num; i = i + 2)
    {
      int shard_id = get_shard_id (key_vals[i]);
      if (!minis_set_internal
	  (minis, shard_id, key_vals[i], key_vals[i + 1], now_us))
	{
	  err = MINIS_ERR_OOM;
	  break;
	}
    }
#ifdef MINIS_EMBEDDED
  for (size_t i = num_shards - 1; i < num_shards; i--)
    unlock_shard (minis, shards_to_lock[i]);
#endif
  return err;
}

MinisError
minis_mget (Minis *minis, const char **keys, size_t count,
	    MinisOneEntryVisitor visitor, void *ctx, uint64_t now_us)
{

#ifdef MINIS_EMBEDDED
  int shards_to_lock[NUM_SHARDS];
  size_t num_shards = 0;
  uint16_t shard_map = 0;

  for (size_t i = 0; i < count; i++)
    {
      int shard_id = get_shard_id (keys[i]);
      if (!(shard_map & (1 << shard_id)))
	{
	  shard_map |= (1 << shard_id);
	  shards_to_lock[num_shards++] = shard_id;
	}
    }

  qsort (shards_to_lock, num_shards, sizeof (int), cmp_int);

  for (size_t i = 0; i < num_shards; i++)
    lock_shard (minis, shards_to_lock[i]);
#endif

  for (size_t i = 0; i < count; ++i)
    {
      int shard_id = get_shard_id (keys[i]);
      Entry *val = entry_lookup (minis, shard_id, keys[i], now_us);
      if (!visitor (val, ctx))
	break;
    }


#ifdef MINIS_EMBEDDED
  for (size_t i = num_shards - 1; i < num_shards; i--)
    unlock_shard (minis, shards_to_lock[i]);
#endif

  return MINIS_OK;
}

MinisError
minis_incr (Minis *minis, const char *key, int64_t delta, int64_t *out_val,
	    uint64_t now_us)
{
  int shard_id = get_shard_id (key);
  lock_shard (minis, shard_id);	// <--- Lock Specific Shard

  Entry *ent = entry_lookup (minis, shard_id, key, now_us);
  int64_t val = 0;

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  unlock_shard (minis, shard_id);
	  return MINIS_ERR_TYPE;
	}
      char *endptr = NULL;
      val = strtoll (ent->val, &endptr, 10);
      if (ent->val[0] == '\0' || *endptr != '\0')
	{
	  unlock_shard (minis, shard_id);
	  return MINIS_ERR_ARG;
	}
    }

  if ((delta > 0 && val > INT64_MAX - delta)
      || (delta < 0 && val < INT64_MIN - delta))
    {
      unlock_shard (minis, shard_id);
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
      ent->val = strdup (val_str);	// Use strdup for cleaner code
    }

  if (!ent || !ent->val)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_OOM;
    }

  minis->dirty_count++;
  if (out_val)
    *out_val = val;

  unlock_shard (minis, shard_id);
  return MINIS_OK;
}
