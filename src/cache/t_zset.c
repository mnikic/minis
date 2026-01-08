#include <stdatomic.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

#include "cache/minis.h"
#include "cache/minis_private.h"
#include "cache/zset.h"
#include "cache/entry.h"

MinisError
minis_zadd (Minis *minis, const char *key, double score, const char *name,
	    int *out_added, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      ent = entry_new_zset (minis, key);
    }

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_OOM;
    }

  if (ent->type != T_ZSET)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  int res = zset_add (ent->zset, name, strlen (name), score);

  if (res >= 0)
    __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);

  if (out_added)
    *out_added = res;

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_zrem (Minis *minis, const char *key, const char *name,
	    int *out_removed, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      if (out_removed)
	*out_removed = 0;
      unlock_shard (shard);
      return MINIS_OK;
    }

  if (ent->type != T_ZSET)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  ZNode *znode = zset_pop (ent->zset, name, strlen (name));
  int res = 0;

  if (znode)
    {
      __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
      znode_del (znode);
      res = 1;

      if (hm_size (&ent->zset->hmap) == 0)
	entry_dispose_atomic (minis, ent);
    }

  if (out_removed)
    *out_removed = res;

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_zscore (Minis *minis, const char *key, const char *name,
	      double *out_score, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_ZSET)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  ZNode *znode = zset_lookup (ent->zset, name, strlen (name));

  if (znode)
    {
      *out_score = znode->score;
      unlock_shard (shard);
      return MINIS_OK;
    }
  unlock_shard (shard);
  return MINIS_ERR_NIL;
}

MinisError
minis_zquery (Minis *minis, const char *key, double score, const char *name,
	      int64_t offset, int64_t limit, MinisZSetCb cb, void *ctx,
	      uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_ZSET)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  if (limit <= 0)
    {
      unlock_shard (shard);
      return MINIS_OK;
    }

  ZNode *znode = zset_query (ent->zset, score, name, strlen (name));
  if (znode && offset > 0)
    znode = znode_offset (znode, offset);

  int64_t count = 0;
  while (znode && count < limit)
    {
      cb (znode->name, znode->len, znode->score, ctx);

      // Move to next node. 
      // Note: Assuming znode_offset(node, 1) is efficient (O(1) next pointer).
      znode = znode_offset (znode, 1);
      count++;
    }

  unlock_shard (shard);
  return MINIS_OK;
}
