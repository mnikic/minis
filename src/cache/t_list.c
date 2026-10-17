#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "cache/minis_private.h"
#include "cache/minis.h"
#include "cache/entry.h"
#include "cache/list.h"
#include "cache/t_list.h"

// Looks up a list. NULL with *out_err set if missing or another type.
static Entry *
list_lookup (Minis *minis, Shard *shard, const char *key, uint64_t now_us,
	     MinisError *out_err)
{
  Entry *ent = entry_lookup (minis, shard, key, now_us);
  if (!ent)
    {
      *out_err = MINIS_ERR_NIL;
      return NULL;
    }
  if (ent->type != T_LIST)
    {
      *out_err = MINIS_ERR_TYPE;
      return NULL;
    }
  *out_err = MINIS_OK;
  return ent;
}

MinisError
minis_push (Minis *minis, const char *key, const char **vals, size_t count,
	    bool front, size_t *out_len, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  MinisError err;
  Entry *ent = list_lookup (minis, shard, key, now_us, &err);
  if (err == MINIS_ERR_TYPE)
    {
      unlock_shard (shard);
      return err;
    }
  if (!ent)
    ent = entry_new_list (minis, key);
  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_OOM;
    }

  err = MINIS_OK;
  for (size_t i = 0; i < count; i++)
    if (!list_push (ent->list, vals[i], front))
      {
	err = MINIS_ERR_OOM;
	break;
      }

  __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
  if (out_len)
    *out_len = list_len (ent->list);

  // Only an OOM on a new list can leave it empty
  if (list_len (ent->list) == 0)
    entry_dispose_atomic (minis, ent);

  unlock_shard (shard);
  return err;
}

MinisError
minis_pop (Minis *minis, const char *key, size_t count, bool front,
	   MinisListCb cb, void *ctx, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  MinisError err;
  Entry *ent = list_lookup (minis, shard, key, now_us, &err);
  if (!ent)
    {
      unlock_shard (shard);
      return err;
    }

  List *list = ent->list;
  size_t len = list_len (list);
  size_t total = count < len ? count : len;

  // Visit first and remove after, so a failed reply loses nothing
  for (size_t i = 0; i < total; i++)
    {
      const char *val = list_at (list, front ? i : len - 1 - i);
      if (!cb (val, i, total, ctx))
	{
	  unlock_shard (shard);
	  return MINIS_ERR_OOM;
	}
    }

  if (total > 0)
    {
      list_drop (list, total, front);
      __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
    }
  if (list_len (list) == 0)
    entry_dispose_atomic (minis, ent);

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_lrange (Minis *minis, const char *key, int64_t start, int64_t stop,
	      MinisListCb cb, void *ctx, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  MinisError err;
  Entry *ent = list_lookup (minis, shard, key, now_us, &err);
  if (!ent)
    {
      unlock_shard (shard);
      return err;
    }

  List *list = ent->list;
  int64_t len = (int64_t) list_len (list);
  if (start < 0)
    start = start < -len ? 0 : start + len;
  if (stop < 0)
    stop += len;
  if (stop >= len)
    stop = len - 1;

  if (start <= stop)
    {
      size_t total = (size_t) (stop - start + 1);
      for (size_t i = 0; i < total; i++)
	if (!cb (list_at (list, (size_t) start + i), i, total, ctx))
	  {
	    err = MINIS_ERR_OOM;
	    break;
	  }
    }

  unlock_shard (shard);
  return err;
}

MinisError
minis_lrem (Minis *minis, const char *key, int64_t count, const char *val,
	    size_t *out_removed, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  MinisError err;
  Entry *ent = list_lookup (minis, shard, key, now_us, &err);
  size_t removed = 0;
  if (ent)
    {
      removed = list_remove (ent->list, val, count);
      if (removed > 0)
	__atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
      if (list_len (ent->list) == 0)
	entry_dispose_atomic (minis, ent);
    }

  *out_removed = removed;
  unlock_shard (shard);
  return err;
}

MinisError
minis_llen (Minis *minis, const char *key, size_t *out_len, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  MinisError err;
  Entry *ent = list_lookup (minis, shard, key, now_us, &err);
  *out_len = ent ? list_len (ent->list) : 0;

  unlock_shard (shard);
  return err;
}
