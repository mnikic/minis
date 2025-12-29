#include <string.h>
#include <stddef.h>
#include <stdint.h>

#include "cache/minis.h"
#include "cache/zset.h"
#include "cache/entry.h"
#include "common/lock.h"

MinisError
minis_zadd (Minis *minis, const char *key, double score, const char *name,
	    int *out_added, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    ent = entry_new_zset (minis, key);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_OOM;
    }
  if (ent->type != T_ZSET)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  int res = zset_add (ent->zset, name, strlen (name), score);
  if (res == 1)
    minis->dirty_count++;
  if (out_added)
    *out_added = res;

  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_zrem (Minis *minis, const char *key, const char *name,
	    int *out_removed, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      if (out_removed)
	*out_removed = 0;
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_OK;		// ZREM on missing key is 0
    }
  if (ent->type != T_ZSET)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  ZNode *znode = zset_pop (ent->zset, name, strlen (name));
  int res = 0;
  if (znode)
    {
      minis->dirty_count++;
      znode_del (znode);
      res = 1;
    }
  if (out_removed)
    *out_removed = res;

  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_zscore (Minis *minis, const char *key, const char *name,
	      double *out_score, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_ZSET)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  ZNode *znode = zset_lookup (ent->zset, name, strlen (name));
  if (znode)
    *out_score = znode->score;
  else
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }

  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_zquery (Minis *minis, const char *key, double score, const char *name,
	      int64_t offset, int64_t limit, MinisZSetCb cb, void *ctx,
	      uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_ZSET)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  if (limit <= 0)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_OK;
    }

  ZNode *znode = zset_query (ent->zset, score, name, strlen (name));
  znode = znode_offset (znode, offset);

  int64_t count = 0;
  while (znode && count < limit)
    {
      cb (znode->name, znode->len, znode->score, ctx);
      znode = znode_offset (znode, 1);
      count++;
    }

  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}
