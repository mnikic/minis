#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "io/buffer.h"
#include "heap.h"
#include "cache.h"
#include "zset.h"
#include "io/out.h"
#include "common/common.h"

// the structure for the key
typedef struct entry
{
  HNode node;
  char *key;
  char *val;
  uint32_t type;
  ZSet *zset;
  // for TTLs
  size_t heap_idx;
} Entry;

enum
{
  T_STR = 0, T_ZSET = 1,
};

// Deallocates the Entry and its contents.
static void
entry_destroy (Entry *ent)
{
  if (!ent)
    return;
  switch (ent->type)
    {
    case T_ZSET:
      zset_dispose (ent->zset);
      free (ent->zset);
      break;
    default:
      break;
    }
  if (ent->key)
    free (ent->key);
  if (ent->val)
    free (ent->val);
  free (ent);
}

// Converts an HNode pointer (from the hash map) back to its parent Entry structure.
static Entry *
fetch_entry (HNode *node_to_fetch)
{
// This pragma block is necessary for container_of, which performs type punning.
// By centralizing it here, we remove it from all other usage sites.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  Entry *entry = container_of (node_to_fetch, Entry, node);
#pragma GCC diagnostic pop
  return entry;
}

// Converts a heap index reference (size_t *ref) back to its parent Entry structure.
static Entry *
fetch_entry_from_heap_ref (size_t *ref)
{
// This pragma block is necessary for container_of, which performs type punning.
// By centralizing it here, we remove it from the cache_evict function.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  Entry *ent = container_of (ref, Entry, heap_idx);
#pragma GCC diagnostic pop
  return ent;
}

static bool
cmd_is (const char *word, const char *cmd)
{
  return 0 == strcasecmp (word, cmd);
}

static int
entry_eq (HNode *lhs, HNode *rhs)
{
  Entry *lentry = fetch_entry (lhs);
  Entry *rentry = fetch_entry (rhs);

  return lhs->hcode == rhs->hcode
    && (lentry != NULL && rentry != NULL && lentry->key != NULL
	&& rentry->key != NULL && strcmp (lentry->key, rentry->key) == 0);
}

static void
cb_destroy_entry (HNode *node, void *arg)
{
  (void) arg;			// unused
  Entry *ent = fetch_entry (node);
  // This is a direct iteration over the nodes, so we call the direct destructor
  entry_destroy (ent);
}

static void
cb_scan (HNode *node, void *arg)
{
  Buffer *out = (Buffer *) arg;
  // NOTE: This callback is designed for hm_scan (which is always void)
  // We cannot stop hm_scan if out_str fails, but we continue attempting to write.
  out_str (out, fetch_entry (node)->key);
}

static int
str2dbl (const char *string, double *out)
{
  char *endp = NULL;
  *out = strtod (string, &endp);
  return endp == string + strlen (string) && !isnan (*out);
}

static int
str2int (const char *string, int64_t *out)
{
  char *endp = NULL;
  *out = strtoll (string, &endp, 10);
  return endp == string + strlen (string);
}

// Helper to check for existing ZSet and output Nil/Error on failure.
// Return: bool status of the buffer operation (true means write succeeded or no write needed).
// Output param *is_valid_zset: true if the key was found and is of type T_ZSET.
static bool
expect_zset (Cache *cache, Buffer *out, char *string, Entry **ent,
	     bool *is_valid_zset)
{
  *is_valid_zset = false;

  Entry key;
  key.key = string;
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));
  HNode *hnode = hm_lookup (&cache->db, &key.node, &entry_eq);

  if (!hnode)
    {
      // Not found. Domain logic failed. Buffer write: NIL.
      return out_nil (out);
    }

  *ent = fetch_entry (hnode);
  if ((*ent)->type != T_ZSET)
    {
      // Wrong type. Domain logic failed. Buffer write: ERR.
      return out_err (out, ERR_TYPE, "expect zset");
    }

  // Found and correct type. Domain logic successful. No buffer write needed.
  *is_valid_zset = true;
  return true;
}

static bool			// Signature changed to bool
do_zadd (Cache *cache, char **cmd, Buffer *out)
{
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    {
      // If str2dbl fails, write error to buffer and return its success status
      return out_err (out, ERR_ARG, "expect fp number");
    }

  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));
  HNode *hnode = hm_lookup (&cache->db, &key.node, &entry_eq);

  Entry *ent = NULL;
  if (!hnode)
    {
      ent = malloc (sizeof (Entry));
      if (!ent)
	die ("Out of memory");
      memset (ent, 0, sizeof (Entry));

      ent->key = calloc (strlen (key.key) + 1, sizeof (char));
      if (!ent->key)
	{
	  free (ent);
	  die ("Out of memory");
	}
      strcpy (ent->key, key.key);

      ent->zset = malloc (sizeof (ZSet));
      if (!ent->zset)
	{
	  free (ent->key);
	  free (ent);
	  die ("Couldn't allocate zset");
	}
      memset (ent->zset, 0, sizeof (ZSet));

      ent->node.hcode = key.node.hcode;
      ent->type = T_ZSET;
      ent->heap_idx = (size_t) -1;
      hm_insert (&cache->db, &ent->node);
    }
  else
    {
      ent = fetch_entry (hnode);
      if (ent->type != T_ZSET)
	{
	  // Write error and return its status
	  return out_err (out, ERR_TYPE, "expect zset");
	}
    }

  const char *name = cmd[3];
  int added = zset_add (ent->zset, name, strlen (name), score);
  // Write result and return its success status
  return out_int (out, (int64_t) added);
}

// zrem zset name
static bool			// Signature changed to bool
do_zrem (Cache *cache, char **cmd, Buffer *out)
{
  Entry *ent = NULL;
  bool is_valid_zset;

  // 1. Check if the buffer write was successful (for NIL or ERR cases).
  if (!expect_zset (cache, out, cmd[1], &ent, &is_valid_zset))
    {
      return false;		// Buffer write failure (backpressure)
    }

  // 2. If the buffer write was successful, but the domain validation failed (!is_valid_zset),
  // the response (NIL or ERR) has already been successfully written by expect_zset.
  // We return true to signal the command was handled and the response is ready.
  if (!is_valid_zset)
    {
      return true;
    }

  ZNode *znode = zset_pop (ent->zset, cmd[2], strlen (cmd[2]));
  if (znode)
    {
      znode_del (znode);
    }
  // 3. Write final result and return its success status
  return out_int (out, znode ? 1 : 0);
}

// zscore zset name
static bool			// Signature changed to bool
do_zscore (Cache *cache, char **cmd, Buffer *out)
{
  Entry *ent = NULL;
  bool is_valid_zset;

  // 1. Check if the buffer write was successful (for NIL or ERR cases).
  if (!expect_zset (cache, out, cmd[1], &ent, &is_valid_zset))
    {
      return false;		// Buffer write failure (backpressure)
    }

  // 2. If the buffer write was successful, but the domain validation failed (!is_valid_zset),
  // the response (NIL or ERR) has already been successfully written by expect_zset.
  // We return true to signal the command was handled and the response is ready.
  if (!is_valid_zset)
    {
      return true;
    }

  ZNode *znode = zset_lookup (ent->zset, cmd[2], strlen (cmd[2]));
  if (znode)
    {
      // 3. Write result and return its success status
      return out_dbl (out, znode->score);
    }
  else
    {
      // 3. Write NIL and return its success status
      return out_nil (out);
    }
}

// zquery zset score name offset limit
static bool			// Signature changed to bool
do_zquery (Cache *cache, char **cmd, Buffer *out)
{
// parse args
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    {
      return out_err (out, ERR_ARG, "expect fp number");
    }
  int64_t offset = 0;
  int64_t limit = 0;
  if (!str2int (cmd[4], &offset))
    {
      return out_err (out, ERR_ARG, "expect int");
    }
  if (!str2int (cmd[5], &limit))
    {
      return out_err (out, ERR_ARG, "expect int");
    }

// get the zset
  Entry *ent = NULL;
  bool is_valid_zset;

  // 1. Check if the buffer write was successful (for NIL or ERR cases).
  if (!expect_zset (cache, out, cmd[1], &ent, &is_valid_zset))
    {
      return false;		// Buffer write failure (backpressure)
    }

  // 2. If the buffer write was successful, but the domain validation failed (!is_valid_zset)
  if (!is_valid_zset)
    {
      // If the output buffer contains SER_NIL (key not found), clear it and send empty array instead.
      if (buf_len (out) > 0 && buf_data (out)[0] == SER_NIL)
	{
	  buf_clear (out);
	  // If this write fails, we return false. Otherwise, we return true.
	  return out_arr (out, (uint32_t) 0);
	}
      // If it was an ERR (wrong type), the ERR was already written. Command handled successfully.
      return true;
    }

// look up the tuple
  if (limit <= 0)
    {
      return out_arr (out, (uint32_t) 0);
    }
  ZNode *znode = zset_query (ent->zset, score, cmd[3], strlen (cmd[3]));
  znode = znode_offset (znode, offset);

  // output
  size_t idx = out_arr_begin (out);
  if (idx == 0)
    return false;

  uint32_t num = 0;
  while (znode && (int64_t) num < limit)
    {
      if (!out_str_size (out, znode->name, znode->len))
	return false;
      if (!out_dbl (out, znode->score))
	return false;

      znode = znode_offset (znode, +1);
      num += 2;
    }
  return out_arr_end (out, idx, num);
}

// set or remove the TTL
static void
entry_set_ttl (Cache *cache, Entry *ent, int64_t ttl_ms)
{
  if (ttl_ms < 0 && ent->heap_idx != (size_t) -1)
    {
      (void) heap_remove_idx (&cache->heap, ent->heap_idx);
      ent->heap_idx = (size_t) -1;
    }
  else if (ttl_ms >= 0)
    {
      size_t pos = ent->heap_idx;
      if (pos == (size_t) -1)
	{
	  // add an new item to the heap
	  HeapItem item;
	  item.ref = &ent->heap_idx;
	  item.val = get_monotonic_usec () + (uint64_t) (ttl_ms * 1000);
	  heap_add (&cache->heap, &item);
	}
      else
	{
	  heap_get (&cache->heap, pos)->val =
	    get_monotonic_usec () + (uint64_t) (ttl_ms * 1000);
	  heap_update (&cache->heap, pos);
	}
    }
}

static bool			// Signature changed to bool
do_keys (Cache *cache, char **cmd, Buffer *out)
{
  (void) cmd;
  // Check return of out_arr. If it fails, we cannot write the header.
  if (!out_arr (out, (uint32_t) hm_size (&cache->db)))
    return false;
  // We cannot stop h_scan if cb_scan fails to write to the buffer, 
  // but we rely on the persistent failure state of the buffer to prevent 
  // further successful writes.
  h_scan (&cache->db.ht1, &cb_scan, out);
  h_scan (&cache->db.ht2, &cb_scan, out);

  // If the array header was written successfully, we assume success 
  // as we cannot check for individual failures in h_scan callback.
  // The caller will check the buffer's final state anyway.
  return true;
}

static bool			// Signature changed to bool
do_del (Cache *cache, char **cmd, Buffer *out)
{
  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_pop (&cache->db, &key.node, &entry_eq);
  if (node)
    {
      // Refactored to use fetch_entry
      Entry *entry = fetch_entry (node);
      if (entry->heap_idx != (size_t) -1)
	{
	  heap_remove_idx (&cache->heap, entry->heap_idx);
	}
      entry_destroy (entry);
    }
  // Check return of out_int
  return out_int (out, node ? 1 : 0);
}

static bool			// Signature changed to bool
do_set (Cache *cache, char **cmd, Buffer *out)
{
  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (node)
    {
      // Refactored to use fetch_entry
      Entry *ent = fetch_entry (node);
      if (ent->type != T_STR)
	{
	  // Write error and check return
	  return out_err (out, ERR_TYPE, "key exists with different type");
	}
      if (ent->val)
	{
	  free (ent->val);
	}
      ent->val = calloc (strlen (cmd[2]) + 1, sizeof (char));
      if (!ent->val)
	die ("Out of memory");
      strcpy (ent->val, cmd[2]);
    }
  else
    {
      Entry *ent = malloc (sizeof (Entry));
      if (!ent)
	die ("Out of memory in do_set");

      ent->key = calloc (strlen (cmd[1]) + 1, sizeof (char));
      if (!ent->key)
	{
	  free (ent);
	  die ("Out of memory");
	}
      strcpy (ent->key, cmd[1]);

      ent->val = calloc (strlen (cmd[2]) + 1, sizeof (char));
      if (!ent->val)
	{
	  free (ent->key);
	  free (ent);
	  die ("Out of memory");
	}
      strcpy (ent->val, cmd[2]);

      ent->node.hcode = key.node.hcode;
      ent->heap_idx = (size_t) -1;
      ent->type = T_STR;
      hm_insert (&cache->db, &ent->node);
    }
  // Write NIL and check return
  return out_nil (out);
}

static void
entry_del_async (void *arg)
{
  entry_destroy ((Entry *) arg);
}

static void
entry_del (Cache *cache, Entry *ent)
{
  entry_set_ttl (cache, ent, -1);

  const size_t k_large_container_size = 10000;
  bool too_big = false;
  switch (ent->type)
    {
    case T_ZSET:
      too_big = hm_size (&ent->zset->hmap) > k_large_container_size;
      break;
    default:
      break;
    }

  if (too_big)
    {
      thread_pool_queue (&cache->tp, &entry_del_async, ent);
    }
  else
    {
      entry_destroy (ent);
    }
}

static bool			// Signature changed to bool
do_get (Cache *cache, char **cmd, Buffer *out)
{
  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (!node)
    {
      // Write NIL and check return
      return out_nil (out);
    }

  // Refactored to use fetch_entry
  Entry *ent = fetch_entry (node);
  if (ent->type != T_STR || !ent->val)
    {
      // Write NIL and check return
      return out_nil (out);
    }

  // Write string and check return
  return out_str (out, ent->val);
}

static bool			// Signature changed to bool
do_expire (Cache *cache, char **cmd, Buffer *out)
{
  int64_t ttl_ms = 0;
  if (!str2int (cmd[2], &ttl_ms))
    {
      // Write error and check return
      return out_err (out, ERR_ARG, "expect int64");
    }

  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (node)
    {
      // Refactored to use fetch_entry
      Entry *ent = fetch_entry (node);
      entry_set_ttl (cache, ent, ttl_ms);
    }
  // Write result and check return
  return out_int (out, node ? 1 : 0);
}

static bool			// Signature changed to bool
do_ttl (Cache *cache, char **cmd, Buffer *out)
{
  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (!node)
    {
      // Write -2 and check return
      return out_int (out, -2);
    }

  // Refactored to use fetch_entry
  Entry *ent = fetch_entry (node);
  if (ent->heap_idx == (size_t) -1)
    {
      // Write -1 and check return
      return out_int (out, -1);
    }

  uint64_t expire_at = heap_get (&cache->heap, ent->heap_idx)->val;
  uint64_t now_us = get_monotonic_usec ();
  // Write calculated TTL and check return
  return out_int (out,
		  (int64_t) (expire_at >
			     now_us ? (expire_at - now_us) / 1000 : 0));
}

bool				// Updated signature to return bool
cache_execute (Cache *cache, char **cmd, size_t size, Buffer *out)
{
  if (size == 1 && cmd_is (cmd[0], "keys"))
    {
      return do_keys (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 2 && cmd_is (cmd[0], "get"))
    {
      return do_get (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 3 && cmd_is (cmd[0], "set"))
    {
      return do_set (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 2 && cmd_is (cmd[0], "del"))
    {
      return do_del (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 3 && cmd_is (cmd[0], "pexpire"))
    {
      return do_expire (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 2 && cmd_is (cmd[0], "pttl"))
    {
      return do_ttl (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 4 && cmd_is (cmd[0], "zadd"))
    {
      return do_zadd (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 3 && cmd_is (cmd[0], "zrem"))
    {
      return do_zrem (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 3 && cmd_is (cmd[0], "zscore"))
    {
      return do_zscore (cache, cmd, out);	// Check and propagate status
    }
  else if (size == 6 && cmd_is (cmd[0], "zquery"))
    {
      return do_zquery (cache, cmd, out);	// Check and propagate status
    }
  else
    {
      // For unknown command, write error and return its status
      return out_err (out, ERR_UNKNOWN, "Unknown cmd");
    }
}

static int
hnode_same (HNode *lhs, HNode *rhs)
{
  return lhs == rhs;
}

Cache *
cache_init (void)
{
  Cache *cache = (Cache *) malloc (sizeof (Cache));
  if (!cache)
    die ("Out of memory in cache_init");
  memset (cache, 0, sizeof (Cache));
  hm_init (&cache->db);
  thread_pool_init (&cache->tp, 4);
  heap_init (&cache->heap);
  return cache;
}

void
cache_evict (Cache *cache, uint64_t now_us)
{
  // TTL timers
  const size_t k_max_works = 2000;
  size_t nworks = 0;
  while (!heap_empty (&cache->heap) && heap_top (&cache->heap)->val < now_us)
    {
      // Refactored to use fetch_entry_from_heap_ref
      Entry *ent = fetch_entry_from_heap_ref (heap_top (&cache->heap)->ref);
      HNode *node = hm_pop (&cache->db, &ent->node, &hnode_same);
      assert (node == &ent->node);
      entry_del (cache, ent);
      if (nworks++ >= k_max_works)
	{
	  // don't stall the server if too many keys are expiring at once
	  break;
	}
    }
}

uint64_t
cache_next_expiry (Cache *cache)
{
  // ttl timers
  if (heap_empty (&cache->heap))
    {
      return (uint64_t) - 1;
    }
  return heap_top (&cache->heap)->val;
}

void
cache_free (Cache *cache)
{
  if (!cache)
    return;
  // Destroy all individual Entry objects in the hash map.
  // We iterate over all nodes in the internal hash tables (ht1 and ht2) and destroy the Entry.
  h_scan (&cache->db.ht1, &cb_destroy_entry, NULL);
  h_scan (&cache->db.ht2, &cb_destroy_entry, NULL);

  // Clean up the internal structures of the components.
  thread_pool_destroy (&cache->tp);

  // Clean up the min-heap internal structure (frees the internal array buffer)
  heap_free (&cache->heap);
  // Clean up the hash map internal structure (frees the internal array buffers)
  hm_destroy (&cache->db);

  // Free the Cache structure itself.
  free (cache);
}
