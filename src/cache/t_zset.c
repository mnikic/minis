#include <string.h>
#include <stddef.h>
#include <stdint.h>

#include "cache/minis.h"
#include "cache/minis_private.h"
#include "cache/zset.h"
#include "cache/entry.h"
#include "common/lock.h"

// Note: These helpers were defined static in minis.c. 
// If you are in a separate file, you need to expose them or replicate the macros.
// For now, assuming this is inside minis.c or has access to:
// lock_shard(minis, id) / unlock_shard(minis, id)

MinisError
minis_zadd (Minis *minis, const char *key, double score, const char *name,
	    int *out_added, uint64_t now_us)
{
  // 1. Calculate Shard ID first
  int shard_id = get_shard_id (key);

  // 2. Lock specific Shard
  lock_shard (minis, shard_id);

  // 3. Lookup using Shard ID
  Entry *ent = entry_lookup (minis, shard_id, key, now_us);

  if (!ent)
    {
      // entry_new_zset MUST use the shard_id internally or we pass it
      // Assuming entry_new_zset calculates ID or inserts into correct shard
      ent = entry_new_zset (minis, key);
    }

  if (!ent)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_OOM;
    }

  if (ent->type != T_ZSET)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_TYPE;
    }

  int res = zset_add (ent->zset, name, strlen (name), score);

  if (res == 1)
    minis->dirty_count++;	// Atomic update preferred if concurrent stats matter

  if (out_added)
    *out_added = res;

  unlock_shard (minis, shard_id);
  return MINIS_OK;
}

MinisError
minis_zrem (Minis *minis, const char *key, const char *name,
	    int *out_removed, uint64_t now_us)
{
  int shard_id = get_shard_id (key);
  lock_shard (minis, shard_id);

  Entry *ent = entry_lookup (minis, shard_id, key, now_us);

  if (!ent)
    {
      if (out_removed)
	*out_removed = 0;
      unlock_shard (minis, shard_id);
      return MINIS_OK;
    }

  if (ent->type != T_ZSET)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_TYPE;
    }

  ZNode *znode = zset_pop (ent->zset, name, strlen (name));
  int res = 0;

  if (znode)
    {
      minis->dirty_count++;
      znode_del (znode);
      res = 1;

      if (hm_size (&ent->zset->hmap) == 0)
	{
	  entry_dispose_atomic (minis, ent);
	}
    }

  if (out_removed)
    *out_removed = res;

  unlock_shard (minis, shard_id);
  return MINIS_OK;
}

MinisError
minis_zscore (Minis *minis, const char *key, const char *name,
	      double *out_score, uint64_t now_us)
{
  int shard_id = get_shard_id (key);
  lock_shard (minis, shard_id);

  Entry *ent = entry_lookup (minis, shard_id, key, now_us);

  // Standard NIL handling logic
  if (!ent)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_ZSET)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_TYPE;
    }

  ZNode *znode = zset_lookup (ent->zset, name, strlen (name));

  if (znode)
    {
      *out_score = znode->score;
      unlock_shard (minis, shard_id);
      return MINIS_OK;
    }
  unlock_shard (minis, shard_id);
  return MINIS_ERR_NIL;
}

MinisError
minis_zquery (Minis *minis, const char *key, double score, const char *name,
	      int64_t offset, int64_t limit, MinisZSetCb cb, void *ctx,
	      uint64_t now_us)
{
  int shard_id = get_shard_id (key);
  lock_shard (minis, shard_id);

  Entry *ent = entry_lookup (minis, shard_id, key, now_us);

  if (!ent)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_ZSET)
    {
      unlock_shard (minis, shard_id);
      return MINIS_ERR_TYPE;
    }

  if (limit <= 0)
    {
      unlock_shard (minis, shard_id);
      return MINIS_OK;
    }

  // NOTE: Callback runs INSIDE the lock. 
  // This is good for consistency but 'cb' must be fast (no I/O blocking).

  ZNode *znode = zset_query (ent->zset, score, name, strlen (name));
  znode = znode_offset (znode, offset);

  int64_t count = 0;
  while (znode && count < limit)
    {
      cb (znode->name, znode->len, znode->score, ctx);
      znode = znode_offset (znode, 1);
      count++;
    }

  unlock_shard (minis, shard_id);
  return MINIS_OK;
}
