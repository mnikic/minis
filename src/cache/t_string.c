#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cache/minis.h"
#include "cache/entry.h"
#include "common/lock.h"
#include "common/macros.h"

static ALWAYS_INLINE const char *
minis_get_internal (Minis *minis, const char *key, uint64_t now_us)
{
  Entry *entry = entry_lookup (minis, key, now_us);
  return entry ? entry->val : NULL;
}

MinisError
minis_get (Minis *minis, const char *key, const char **out_val,
	   uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (!ent)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_NIL;
    }
  if (ent->type != T_STR)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_TYPE;	// or NIL depending on preference, standard is NIL for MGET/GET
    }
  *out_val = ent->val;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

static bool
minis_set_internal (Minis *minis, const char *key, const char *val,
		    uint64_t now_us)
{
  // lookup proactively destroys expired entries!
  Entry *ent = entry_lookup (minis, key, now_us);

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  entry_dispose_atomic (minis, ent);
	  // Create new string entry from scratch
	  if (!entry_new_str (minis, key, val))
	    return false;
	}
      else
	{
	  // Update Existing String entry
	  // Optimization: If value is identical, just clear TTL and return
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
      // New Key
      if (!entry_new_str (minis, key, val))
	return false;
    }

  minis->dirty_count++;
  return true;
}

MinisError
minis_mset (Minis *minis, const char **key_vals, size_t total_num,
	    uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  for (size_t i = 0; i < total_num; i = i + 2)
    if (!minis_set_internal (minis, key_vals[i], key_vals[i + 1], now_us))
      {
	ENGINE_UNLOCK (&minis->lock);
	return MINIS_ERR_OOM;
      }
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_mget (Minis *minis, const char **keys, size_t count,
	    MinisOneValVisitor visitor, void *ctx, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  for (size_t i = 0; i < count; ++i)
    {
      const char *val = minis_get_internal (minis, keys[i], now_us);
      if (!visitor (val, ctx))
	break;
    }

  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_set (Minis *minis, const char *key, const char *val, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  if (!minis_set_internal (minis, key, val, now_us))
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_OOM;
    }
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}

MinisError
minis_incr (Minis *minis, const char *key, int64_t delta, int64_t *out_val,
	    uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  int64_t val = 0;

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  ENGINE_UNLOCK (&minis->lock);
	  return MINIS_ERR_TYPE;
	}
      char *endptr = NULL;
      val = strtoll (ent->val, &endptr, 10);
      if (ent->val[0] == '\0' || *endptr != '\0')
	{
	  ENGINE_UNLOCK (&minis->lock);
	  return MINIS_ERR_ARG;
	}
    }

  if ((delta > 0 && val > INT64_MAX - delta)
      || (delta < 0 && val < INT64_MIN - delta))
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_ARG;
    }

  val += delta;
  char val_str[32];
  snprintf (val_str, sizeof (val_str), "%" PRId64, val);

  // Re-use internal set logic logic manually or simplify:
  // Simplified inline set for INCR:
  if (!ent)
    {
      ent = entry_new_str (minis, key, val_str);
    }
  else
    {
      free (ent->val);
      ent->val = calloc (strlen (val_str) + 1, sizeof (char));
      strcpy (ent->val, val_str);
    }

  if (!ent || !ent->val)
    {
      ENGINE_UNLOCK (&minis->lock);
      return MINIS_ERR_OOM;
    }

  minis->dirty_count++;
  if (out_val)
    *out_val = val;
  ENGINE_UNLOCK (&minis->lock);
  return MINIS_OK;
}
