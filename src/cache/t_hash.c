#include <stddef.h>
#include <stdint.h>

#include "common/lock.h"
#include "cache/minis.h"
#include "cache/entry.h"
#include "cache/hash.h"
#include "cache/hashtable.h"

MinisError
minis_hset (Minis *minis, const char *key, const char *field,
	    const char *value, int *out_added, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = fetch_or_create (minis, key, now_us);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_OOM;
    }
  if (ent->type != T_HASH)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  int res = hash_set (ent->hash, field, value);
  if (res)
    minis->dirty_count++;
  if (out_added)
    *out_added = res;

  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_hget (Minis *minis, const char *key, const char *field,
	    const char **out_val, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_HASH)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  HashEntry *hash_entry = hash_lookup (ent->hash, field);
  if (!hash_entry)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }

  *out_val = hash_entry->value;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_hdel (Minis *minis, const char *key, const char *field,
	    int *out_deleted, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent || ent->type != T_HASH)
    {
      if (out_deleted)
	*out_deleted = 0;
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_OK;		// Not an error to delete missing
    }

  int val = hash_del (ent->hash, field);
  if (val > 0)
    {
      minis->dirty_count++;
      if (hm_size (ent->hash) == 0)
	entry_dispose_atomic (minis, ent);
    }

  if (out_deleted)
    *out_deleted = val;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_hexists (Minis *minis, const char *key, const char *field,
	       int *out_exists, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      *out_exists = 0;
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_OK;
    }
  if (ent->type != T_HASH)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  HashEntry *hash_entry = hash_lookup (ent->hash, field);
  *out_exists = hash_entry ? 1 : 0;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_hlen (Minis *minis, const char *key, size_t *out_count, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_HASH)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }
  *out_count = hm_size (ent->hash);
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_hgetall (Minis *minis, const char *key, MinisHashCb cb, void *ctx,
	       uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_HASH)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;
    }

  HMIter iter;
  hm_iter_init (ent->hash, &iter);
  HNode *node;
  while ((node = hm_iter_next (&iter)))
    {
      HashEntry *hash_entry = fetch_hash_entry (node);
      cb (hash_entry->field, hash_entry->value, ctx);
    }
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}
