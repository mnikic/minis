#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "cache/minis_private.h"
#include "cache/minis.h"
#include "cache/entry.h"
#include "cache/hash.h"
#include "cache/hashtable.h"

MinisError
minis_hset (Minis *minis, const char *key, const char *field,
	    const char *value, int *out_added, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    ent = entry_new_hash (minis, key);

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_OOM;
    }
  if (ent->type != T_HASH)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  int res = hash_set (ent->hash, field, value);
  if (res)
    __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);

  if (out_added)
    *out_added = res;

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_hget (Minis *minis, const char *key, const char *field,
	    MinisHashCb func, void *ctx, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_HASH)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  HashEntry *hash_entry = hash_lookup (ent->hash, field);
  if (!hash_entry)
    {
      unlock_shard (shard);
      return MINIS_ERR_NIL;
    }

  bool result = func (hash_entry, ctx);

  unlock_shard (shard);
  return result ? MINIS_OK : MINIS_ERR_OOM;
}

MinisError
minis_hdel (Minis *minis, const char *key, const char **fields,
	    size_t field_count, int *out_deleted, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);
  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      if (out_deleted)
	*out_deleted = 0;
      unlock_shard (shard);
      return MINIS_OK;		// Not an error to delete from missing key
    }

  if (ent->type != T_HASH)
    {
      if (out_deleted)
	*out_deleted = 0;
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  int total = 0;
  for (size_t i = 0; i < field_count; i++)
    if (hash_del (ent->hash, fields[i]))
      total++;

  if (total > 0)
    __atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);

  // If the hash is now empty, remove the entire Entry from the DB.
  // entry_dispose_atomic will handle the hm_pop internally.
  if (hm_size (ent->hash) == 0)
    {
      // Note: We hold the Shard lock here, which entry_dispose_atomic expects
      // (since it accesses the DB but doesn't lock it itself).
      entry_dispose_atomic (minis, ent);
    }

  if (out_deleted)
    *out_deleted = total;

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_hexists (Minis *minis, const char *key, const char *field,
	       int *out_exists, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      *out_exists = 0;
      unlock_shard (shard);
      return MINIS_OK;
    }
  if (ent->type != T_HASH)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  HashEntry *hash_entry = hash_lookup (ent->hash, field);
  *out_exists = hash_entry ? 1 : 0;

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_hlen (Minis *minis, const char *key, size_t *out_count, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_HASH)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  *out_count = hm_size (ent->hash);

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_hgetall (Minis *minis, const char *key, MinisHashCb func, void *ctx,
	       uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key);

  Entry *ent = entry_lookup (minis, shard, key, now_us);

  if (!ent)
    {
      unlock_shard (shard);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_HASH)
    {
      unlock_shard (shard);
      return MINIS_ERR_TYPE;
    }

  HMIter iter;
  hm_iter_init (ent->hash, &iter);
  HNode *node;
  while ((node = hm_iter_next (&iter)))
    {
      HashEntry *hash_entry = fetch_hash_entry (node);
      if (!func (hash_entry, ctx))
	break;
    }

  unlock_shard (shard);
  return MINIS_OK;
}
