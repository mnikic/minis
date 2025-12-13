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
  uint64_t expire_at_us;
  ZSet *zset;
  // for TTLs
  size_t heap_idx;		// Index in the TTL heap. (size_t)-1 means not in heap.
} Entry;

enum
{
  T_STR = 0, T_ZSET = 1,
};

static int
hnode_same (HNode *lhs, HNode *rhs)
{
  return lhs == rhs;
}

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

// Helper for asynchronous cleanup
static void
entry_del_async (void *arg)
{
  entry_destroy ((Entry *) arg);
}

// set or remove the TTL
static void
entry_set_ttl (Cache *cache, uint64_t now_us, Entry *ent, int64_t ttl_ms)
{
  if (ttl_ms < 0 && ent->heap_idx != (size_t) -1)
    {
      // TTL is being removed or key is being deleted.
      (void) heap_remove_idx (&cache->heap, ent->heap_idx);
      ent->heap_idx = (size_t) -1;
      ent->expire_at_us = 0;
    }
  else if (ttl_ms >= 0)
    {
      uint64_t new_expire_at_us = now_us + (uint64_t) (ttl_ms * 1000);
      ent->expire_at_us = new_expire_at_us;

      size_t pos = ent->heap_idx;
      if (pos == (size_t) -1)
	{
	  // add an new item to the heap
	  HeapItem item;
	  item.ref = &ent->heap_idx;
	  item.val = new_expire_at_us;
	  heap_add (&cache->heap, &item);
	}
      else
	{
	  // Update existing item in the heap
	  heap_get (&cache->heap, pos)->val = new_expire_at_us;
	  heap_update (&cache->heap, pos);
	}
    }
}

static void
entry_del (Cache *cache, Entry *ent, uint64_t now_us)
{
  // Ensure TTL is removed from the heap and expire_at_us is set to 0.
  entry_set_ttl (cache, now_us, ent, -1);

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
      // Defer destruction to the thread pool to avoid stalling the main thread.
      thread_pool_queue (&cache->tp, &entry_del_async, ent);
    }
  else
    {
      // Small object, destroy immediately.
      entry_destroy (ent);
    }
}

// Converts an HNode pointer (from the hash map) back to its parent Entry structure.
static Entry *
fetch_entry (HNode *node_to_fetch)
{
// This pragma block is necessary for container_of, which performs type punning.
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

// Structure to pass to h_scan to track the buffer and the count of successfully written elements.
typedef struct
{
  Buffer *out;
  uint32_t count;
  uint64_t now_us;
} ScanContext;

static void
cb_scan (HNode *node, void *arg)
{
  ScanContext *ctx = (ScanContext *) arg;
  Entry *ent = fetch_entry (node);
  if (ent->expire_at_us != 0)
    {
      if (ent->expire_at_us < ctx->now_us)
	{
	  return;
	}
    }
  // NOTE: out_str returns true on success. If it fails, we should abort, but we cannot. out_arr_end will fail and report
  if (out_str (ctx->out, fetch_entry (node)->key))
    {
      ctx->count++;
    }
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

/**
 * @brief Helper to check for existing ZSet, perform passive eviction, and output Nil/Error on failure.
 * @return bool status of the buffer operation (true means write succeeded or no write needed).
 * @param is_valid_zset: true if the key was found, not expired, and is of type T_ZSET.
 */
static bool
expect_zset (Cache *cache, Buffer *out, char *string, Entry **ent,
	     bool *is_valid_zset, uint64_t now_us)
{
  *is_valid_zset = false;

  Entry key;
  key.key = string;
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));
  HNode *hnode = hm_lookup (&cache->db, &key.node, &entry_eq);

  if (!hnode)
    {
      return out_nil (out);	// Not found. Buffer write: NIL.
    }

  *ent = fetch_entry (hnode);

  if ((*ent)->expire_at_us != 0)
    {
      if ((*ent)->expire_at_us < now_us)
	{
	  // Key is expired. Perform passive eviction.
	  HNode *removed_node =
	    hm_pop (&cache->db, &(*ent)->node, &hnode_same);
	  assert (removed_node == &(*ent)->node);

	  entry_del (cache, *ent, now_us);
	  *ent = NULL;

	  return out_nil (out);
	}
    }

  if ((*ent)->type != T_ZSET)
    {
      // Wrong type. Domain logic failed. Buffer write: ERR.
      return out_err (out, ERR_TYPE, "expect zset");
    }

  // Found and correct type. Domain logic successful. No buffer write needed.
  *is_valid_zset = true;
  return true;
}

static bool
do_zadd (Cache *cache, char **cmd, Buffer *out)
{
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    {
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
      ent->expire_at_us = 0;
      hm_insert (&cache->db, &ent->node, &entry_eq);
    }
  else
    {
      ent = fetch_entry (hnode);
      if (ent->type != T_ZSET)
	{
	  return out_err (out, ERR_TYPE, "expect zset");
	}
    }

  const char *name = cmd[3];
  int added = zset_add (ent->zset, name, strlen (name), score);
  return out_int (out, (int64_t) added);
}

// zrem zset name
static bool
do_zrem (Cache *cache, char **cmd, Buffer *out, uint64_t now_us)
{
  Entry *ent = NULL;
  bool is_valid_zset;

  // expect_zset handles lookup, passive expiration check, and type check
  if (!expect_zset (cache, out, cmd[1], &ent, &is_valid_zset, now_us))
    {
      return false;		// Buffer write failure (backpressure)
    }

  if (!is_valid_zset)
    {
      return true;		// Response (NIL/ERR) already written successfully
    }

  ZNode *znode = zset_pop (ent->zset, cmd[2], strlen (cmd[2]));
  if (znode)
    {
      znode_del (znode);
    }
  return out_int (out, znode ? 1 : 0);
}

// zscore zset name
static bool
do_zscore (Cache *cache, char **cmd, Buffer *out, uint64_t now_us)
{
  Entry *ent = NULL;
  bool is_valid_zset;

  // expect_zset handles lookup, passive expiration check, and type check
  if (!expect_zset (cache, out, cmd[1], &ent, &is_valid_zset, now_us))
    {
      return false;		// Buffer write failure (backpressure)
    }

  if (!is_valid_zset)
    {
      return true;		// Response (NIL/ERR) already written successfully
    }

  ZNode *znode = zset_lookup (ent->zset, cmd[2], strlen (cmd[2]));
  if (znode)
    {
      return out_dbl (out, znode->score);
    }
  return out_nil (out);
}

// zquery zset score name offset limit
static bool
do_zquery (Cache *cache, char **cmd, Buffer *out, uint64_t now_us)
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

  Entry *ent = NULL;
  bool is_valid_zset;

  // expect_zset handles lookup, passive expiration check, and type check
  if (!expect_zset (cache, out, cmd[1], &ent, &is_valid_zset, now_us))
    {
      return false;		// Buffer write failure (backpressure)
    }

  if (!is_valid_zset)
    {
      // If the output buffer contains SER_NIL (key not found or expired), clear it and send empty array instead.
      // If it was an ERR (wrong type), the ERR was already written and we return true below.
      if (buf_len (out) > 0 && buf_data (out)[0] == SER_NIL)
	{
	  buf_clear (out);
	  return out_arr (out, 0);
	}
      return true;
    }

  if (limit <= 0)
    {
      return out_arr (out, 0);
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

static bool
do_keys (Cache *cache, char **cmd, Buffer *out, uint64_t now_us)
{
  (void) cmd;

  // Start the array response (writes a placeholder for the count)
  size_t idx = out_arr_begin (out);
  if (idx == 0)
    return false;

  ScanContext ctx = {.out = out,.count = 0 };
  ctx.now_us = now_us;

  // We cannot stop the scan if a write fails, but the subsequent calls will
  // also fail, and the final count will be accurate.
  hm_scan (&cache->db, &cb_scan, &ctx);

  // Finalize the array output with the actual number of elements written. If the buffer is full return false.
  return out_arr_end (out, idx, ctx.count);
}

static bool
do_del (Cache *cache, char **cmd, Buffer *out, uint64_t now_us)
{
  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_pop (&cache->db, &key.node, &entry_eq);
  if (node)
    {
      Entry *entry = fetch_entry (node);
      // entry_del handles heap removal and asynchronous destruction
      entry_del (cache, entry, now_us);
    }
  return out_int (out, node ? 1 : 0);
}

static bool
do_set (Cache *cache, char **cmd, Buffer *out)
{
  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (node)
    {
      Entry *ent = fetch_entry (node);
      if (ent->type != T_STR)
	{
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
      ent->expire_at_us = 0;
      ent->type = T_STR;
      hm_insert (&cache->db, &ent->node, &entry_eq);
    }
  return out_nil (out);
}

static bool
do_get (Cache *cache, char *key_param, Buffer *out, uint64_t now_us)
{
  Entry key;
  key.key = key_param;
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (!node)
    {
      return out_nil (out);
    }

  Entry *ent = fetch_entry (node);
  if (ent->type != T_STR || !ent->val)
    {
      return out_nil (out);
    }

  // Check against the timestamp stored directly in the entry.
  if (ent->expire_at_us != 0)
    {
      if (ent->expire_at_us < now_us)
	{
	  // Key is expired. Initiate passive eviction (the garbage collector role).
	  HNode *removed_node = hm_pop (&cache->db, &ent->node, &hnode_same);
	  assert (removed_node == &ent->node);

	  entry_del (cache, ent, now_us);	// Handles heap removal and async cleanup
	  return out_nil (out);	// Key is deleted, return NIL.
	}
    }

  // Key is valid and not expired. Return the value.
  return out_str (out, ent->val);
}

/**
 * @brief Handles the MGET command for multiple keys.
 * * This function handles the RESP array framing and iterates over the keys,
 * calling do_get for each one. Crucially, it implements the "fail-fast" 
 * buffer check: if any single do_get call fails to write its result 
 * (value or NIL) because the output buffer is full, the entire operation 
 * stops and returns false.
 *
 * @param cache The database instance.
 * @param cmd The command array (e.g., {"MGET", "key1", "key2", "key3", ...})
 * @param nkeys The number of keys to retrieve (must be cmd_size - 1).
 * @param out The output buffer to serialize the results to.
 * @return bool True if the entire result (array header + all values/NILs) 
 * was successfully written; false otherwise (buffer exhausted).
 */
static bool
do_mget (Cache *cache, char **cmd, size_t nkeys, Buffer *out, uint64_t now_us)
{
  if (!out_arr (out, nkeys))
    {
      // If the header itself cannot be written, fail immediately.
      return false;
    }

  for (size_t i = 0; i < nkeys; ++i)
    {
      // cmd[0] is "mget" we can skip it.
      // Call do_get for the current key.
      // do_get will write either the value (Bulk String) or (NIL) 
      // to the output buffer 'out'.
      // It returns false only if the output buffer fills up during the write.
      if (!do_get (cache, cmd[i + 1], out, now_us))
	{
	  // FAIL-FAST: If writing the result of the current key failed, 
	  // the buffer is exhausted and the response is incomplete. 
	  // Stop processing immediately.
	  return false;
	}
    }

  // All elements (values or NILs) have been successfully written.
  return true;
}

static bool
do_expire (Cache *cache, char **cmd, Buffer *out, uint64_t now_us)
{
  int64_t ttl_ms = 0;
  if (!str2int (cmd[2], &ttl_ms))
    {
      return out_err (out, ERR_ARG, "expect int64");
    }

  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (node)
    {
      Entry *ent = fetch_entry (node);
      entry_set_ttl (cache, now_us, ent, ttl_ms);
    }
  return out_int (out, node ? 1 : 0);
}

static bool
do_ttl (Cache *cache, char **cmd, Buffer *out, uint64_t now_us)
{
  Entry key;
  key.key = cmd[1];
  key.node.hcode = str_hash ((uint8_t *) key.key, strlen (key.key));

  HNode *node = hm_lookup (&cache->db, &key.node, &entry_eq);
  if (!node)
    {
      return out_int (out, -2);	// Key not found
    }

  Entry *ent = fetch_entry (node);

  // Check if TTL is set (0 means no TTL, or already expired/persisted)
  if (ent->expire_at_us == 0)
    {
      return out_int (out, -1);	// TTL not set
    }

  // Check if the key is already expired in-memory (though it should be passively deleted on read)
  if (ent->expire_at_us < now_us)
    {
      return out_int (out, 0);	// Expired, return 0
    }

  // Calculate remaining TTL in milliseconds
  return out_int (out, (int64_t) ((ent->expire_at_us - now_us) / 1000));
}

bool
cache_execute (Cache *cache, char **cmd, size_t size, Buffer *out,
	       uint64_t now_us)
{
  if (size == 1 && cmd_is (cmd[0], "keys"))
    {
      return do_keys (cache, cmd, out, now_us);
    }
  if (size == 2 && cmd_is (cmd[0], "get"))
    {
      return do_get (cache, cmd[1], out, now_us);
    }
  if (cmd_is (cmd[0], "mget") && size > 1)
    {
      return do_mget (cache, cmd, size - 1, out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "set"))
    {
      return do_set (cache, cmd, out);
    }
  if (size == 2 && cmd_is (cmd[0], "del"))
    {
      return do_del (cache, cmd, out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "pexpire"))
    {
      return do_expire (cache, cmd, out, now_us);
    }
  if (size == 2 && cmd_is (cmd[0], "pttl"))
    {
      return do_ttl (cache, cmd, out, now_us);
    }
  if (size == 4 && cmd_is (cmd[0], "zadd"))
    {
      return do_zadd (cache, cmd, out);
    }
  if (size == 3 && cmd_is (cmd[0], "zrem"))
    {
      return do_zrem (cache, cmd, out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "zscore"))
    {
      return do_zscore (cache, cmd, out, now_us);
    }
  if (size == 6 && cmd_is (cmd[0], "zquery"))
    {
      return do_zquery (cache, cmd, out, now_us);
    }

  return out_err (out, ERR_UNKNOWN, "Unknown cmd");
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
  // TTL timers (Active Eviction)
  const size_t k_max_works = 2000;
  size_t nworks = 0;
  while (!heap_empty (&cache->heap) && heap_top (&cache->heap)->val < now_us)
    {
      // Get the entry from the heap top's ref (which points to the Entry's heap_idx field)
      Entry *ent = fetch_entry_from_heap_ref (heap_top (&cache->heap)->ref);
      HNode *node = hm_pop (&cache->db, &ent->node, &hnode_same);
      assert (node == &ent->node);
      // entry_del handles heap removal (via entry_set_ttl) and cleanup
      entry_del (cache, ent, now_us);
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
  hm_scan (&cache->db, &cb_destroy_entry, NULL);

  // Clean up the internal structures of the components.
  thread_pool_destroy (&cache->tp);

  // Clean up the min-heap internal structure (frees the internal array buffer)
  heap_free (&cache->heap);
  // Clean up the hash map internal structure (frees the internal array buffers)
  hm_destroy (&cache->db);

  // Free the Cache structure itself.
  free (cache);
}
