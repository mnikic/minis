#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cache/minis.h"
#include "cache/entry.h"
#include "common/lock.h"


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

MinisError
minis_set (Minis *minis, const char *key, const char *val, uint64_t now_us)
{
  ENGINE_LOCK (&minis->lock);
  Entry *ent = entry_lookup (minis, key, now_us);
  if (ent)
    {
      if (ent->type != T_STR)
	{
	  entry_dispose_atomic (minis, ent);
	  Entry *new_ent = entry_new_str (minis, key, val);
	  if (!new_ent)
	    {
	      ENGINE_UNLOCK (&minis->lock);
	      return MINIS_ERR_OOM;
	    }
	}
      else
	{
	  if (ent->val)
	    {
	      if (strcmp (ent->val, val) != 0)
		{
		  free (ent->val);
		  ent->val = calloc (strlen (val) + 1, sizeof (char));
		  if (ent->val)
		    strcpy (ent->val, val);
		}
	    }
	}
    }
  else
    {
      if (!entry_new_str (minis, key, val))
	{
	  ENGINE_UNLOCK (&minis->lock);
	  return MINIS_ERR_OOM;
	}
    }
  minis->dirty_count++;
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
