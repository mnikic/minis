#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "cache/minis_private.h"
#include "cache/minis.h"
#include "cache/entry.h"
#include "cache/hash.h"
#include "cache/hashtable.h"

MinisError
minis_hset (Minis *minis, const char *key, size_t key_len,
	    const char **pairs, const size_t *pair_lens, size_t npairs,
	    int64_t *out_added, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

  if (!ent)
    ent = entry_new_hash (minis, key, key_len);

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

  MinisError err = MINIS_OK;
  int64_t added = 0;
  for (size_t i = 0; i < npairs; i++)
    {
      HashSetResult res = hash_set (ent->hash, pairs[2 * i], pair_lens[2 * i],
				    pairs[2 * i + 1], pair_lens[2 * i + 1]);
      if (res == HASH_OOM)
	{
	  err = MINIS_ERR_OOM;
	  break;
	}
      // A new value is a change too, or snapshots would miss it
      if (res != HASH_UNCHANGED)
	__atomic_fetch_add (&shard->dirty_count, 1, __ATOMIC_RELAXED);
      added += res == HASH_ADDED;
    }

  // A hash made for these fields must not stay behind empty
  if (err != MINIS_OK && hm_size (ent->hash) == 0)
    entry_dispose_atomic (minis, ent);

  if (out_added)
    *out_added = added;

  unlock_shard (shard);
  return err;
}

MinisError
minis_hget (Minis *minis, const char *key, size_t key_len, const char *field,
	    size_t field_len, MinisHashCb func, void *ctx, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

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

  HashEntry *hash_entry = hash_lookup (ent->hash, field, field_len);
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
minis_hdel (Minis *minis, const char *key, size_t key_len, const char **fields,
	    const size_t *field_lens, size_t field_count, int *out_deleted,
	    uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);
  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

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
    if (hash_del (ent->hash, fields[i],
		  field_lens ? field_lens[i] : strlen (fields[i])))
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
minis_hexists (Minis *minis, const char *key, size_t key_len, const char *field,
	       size_t field_len, int *out_exists, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

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

  HashEntry *hash_entry = hash_lookup (ent->hash, field, field_len);
  *out_exists = hash_entry ? 1 : 0;

  unlock_shard (shard);
  return MINIS_OK;
}

MinisError
minis_hlen (Minis *minis, const char *key, size_t key_len, size_t *out_count, uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

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

// How many fields ahead of the visit HGETALL prefetches
#define K_HGETALL_AHEAD 8

static inline void
prefetch_hash_entry (HNode *node)
{
  if (node)
    {
      __builtin_prefetch (node);
      __builtin_prefetch ((const char *) node + 64);
    }
}

MinisError
minis_hgetall (Minis *minis, const char *key, size_t key_len, MinisHashCb func, void *ctx,
	       uint64_t now_us)
{
  Shard *shard = lock_shard_for_key (minis, key, key_len);

  Entry *ent = entry_lookup (minis, shard, key, key_len, now_us);

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

  // An iterator runs ahead and prefetches the fields' entries, which hold
  // the field and a short value, two cache lines of each. So their misses
  // overlap with the visits.
  HMIter iter, ahead;
  hm_iter_init (ent->hash, &iter);
  hm_iter_init (ent->hash, &ahead);
  for (int i = 0; i < K_HGETALL_AHEAD; i++)
    prefetch_hash_entry (hm_iter_next (&ahead));
  HNode *node;
  while ((node = hm_iter_next (&iter)))
    {
      prefetch_hash_entry (hm_iter_next (&ahead));
      if (!func (fetch_hash_entry (node), ctx))
	break;
    }

  unlock_shard (shard);
  return MINIS_OK;
}
