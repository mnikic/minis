#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <math.h>
#include <time.h>

#include "cache/hashtable.h"
#include "cache/heap.h"
#include "cache/cache.h"
#include "cache/zset.h"
#include "cache/thread_pool.h"
#include "io/proto_defs.h"
#include "io/buffer.h"
#include "io/out.h"
#include "common/common.h"
#include "common/glob.h"
#include "common/macros.h"

static int
hnode_same (HNode *lhs, HNode *rhs)
{
  return lhs == rhs;
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

static int
entry_eq (HNode *lhs, HNode *rhs)
{
  Entry *lentry = fetch_entry (lhs);
  Entry *rentry = fetch_entry (rhs);

  return lhs->hcode == rhs->hcode
    && (lentry != NULL && rentry != NULL && lentry->key != NULL
	&& rentry->key != NULL && strcmp (lentry->key, rentry->key) == 0);
}

static HNode *
hm_lookup_by_key (HMap *hmap, const char *key)
{
  Entry entry_key;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
  entry_key.key = (char *) key;
#pragma GCC diagnostic pop
  entry_key.node.hcode =
    str_hash ((uint8_t *) entry_key.key, strlen (entry_key.key));

  return hm_lookup (hmap, &entry_key.node, &entry_eq);
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
    case T_STR:
      // fallsthrough
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

void
entry_set_expiration (Cache *cache, Entry *ent, uint64_t expire_at_us)
{
  ent->expire_at_us = expire_at_us;

  size_t pos = ent->heap_idx;
  if (pos == (size_t) -1)
    {
      // add an new item to the heap
      HeapItem item;
      item.ref = &ent->heap_idx;
      item.val = expire_at_us;
      heap_add (&cache->heap, &item);
    }
  else
    {
      // Update existing item in the heap
      heap_get (&cache->heap, pos)->val = expire_at_us;
      heap_update (&cache->heap, pos);
    }
}

// set or remove the TTL
void
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
      entry_set_expiration (cache, ent, new_expire_at_us);
    }
}

Entry *
entry_new_zset (Cache *cache, const char *key)
{
  Entry *ent = malloc (sizeof (Entry));
  if (!ent)
    die ("Out of memory");
  memset (ent, 0, sizeof (Entry));

  ent->key = calloc (strlen (key) + 1, sizeof (char));
  if (!ent->key)
    {
      free (ent);
      die ("Out of memory");
    }
  strcpy (ent->key, key);

  ent->zset = malloc (sizeof (ZSet));
  if (!ent->zset)
    {
      free (ent->key);
      free (ent);
      die ("Couldn't allocate zset");
    }
  memset (ent->zset, 0, sizeof (ZSet));

  ent->node.hcode = str_hash ((const uint8_t *) key, strlen (key));
  ent->type = T_ZSET;
  ent->heap_idx = (size_t) -1;
  ent->expire_at_us = 0;
  hm_insert (&cache->db, &ent->node, &entry_eq);
  return ent;
}

Entry *
entry_new_str (Cache *cache, const char *key, const char *val)
{
  Entry *ent = malloc (sizeof (Entry));
  if (!ent)
    die ("Out of memory in do_set");

  ent->key = calloc (strlen (key) + 1, sizeof (char));
  if (!ent->key)
    {
      free (ent);
      die ("Out of memory");
    }
  strcpy (ent->key, key);

  ent->val = calloc (strlen (val) + 1, sizeof (char));
  if (!ent->val)
    {
      free (ent->key);
      free (ent);
      die ("Out of memory");
    }
  strcpy (ent->val, val);

  ent->node.hcode = str_hash ((const uint8_t *) key, strlen (key));
  ent->heap_idx = (size_t) -1;
  ent->expire_at_us = 0;
  ent->type = T_STR;
  hm_insert (&cache->db, &ent->node, &entry_eq);
  return ent;
}

static bool
do_ping (Cache *cache, const char **args, size_t arg_count, Buffer *out)
{
  (void) cache;
  // Case 1: "PING" -> "+PONG\r\n" (Simple String)
  if (arg_count == 1)
    {
      if (out->proto == PROTO_RESP)
	{
	  // Manually write Simple String (+PONG)
	  // Note: out_str writes Bulk Strings ($4\r\nPONG), which is also valid 
	  // but +PONG is the standard.
	  return buf_append_cstr (out, "+PONG\r\n");
	}
      // Binary protocol PONG (Maybe just return string "PONG")
      return out_str (out, "PONG");
    }

  // Case 2: "PING message" -> "message" (Bulk String)
  if (arg_count == 2)
    {
      return out_str (out, args[1]);
    }

  return out_err (out, ERR_ARG,
		  "wrong number of arguments for 'ping' command");
}

static bool
do_config (Cache *cache, const char **args, size_t arg_count, Buffer *out)
{
  (void) cache;
  (void) args;
  (void) arg_count;
  // Just return an empty array to make the tool happy
  // It usually asks for "CONFIG GET save"

  // Return *0\r\n
  return out_arr (out, 0);
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
    case T_STR:
      // fallsthrough
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

// Safely remove an entry from the DB and destroy it (sync or async)
static void
entry_dispose_atomic (Cache *cache, Entry *ent)
{
  HNode *removed = hm_pop (&cache->db, &ent->node, &hnode_same);
  if (!removed)
    return;

  assert (removed == &ent->node);

  // We pass -1 or 0 for timestamp because we are destroying it immediately, 
  // so heap position updates don't matter much (it's being removed).
  entry_del (cache, ent, (uint64_t) - 1);
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

static ALWAYS_INLINE bool
cmd_is (const char *word, const char *cmd)
{
  return 0 == strcasecmp (word, cmd);
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
  bool result;
  const char *pattern;
} ScanContext;

static void
cb_scan (HNode *node, void *arg)
{
  ScanContext *ctx = (ScanContext *) arg;
  if (!ctx->result)
    return;
  Entry *ent = fetch_entry (node);
  if (ent->expire_at_us != 0)
    {
      if (ent->expire_at_us < ctx->now_us)
	{
	  return;
	}
    }
  if (!glob_match (ctx->pattern, ent->key))
    return;

  if (out_str (ctx->out, fetch_entry (node)->key))
    {
      ctx->count++;
    }
  else
    ctx->result = false;
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
expect_zset (Cache *cache, Buffer *out, const char *key, Entry **ent,
	     bool *is_valid_zset, uint64_t now_us)
{
  *is_valid_zset = false;

  HNode *hnode = hm_lookup_by_key (&cache->db, key);

  if (!hnode)
    {
      return out_nil (out);	// Not found. Buffer write: NIL.
    }

  *ent = fetch_entry (hnode);

  if ((*ent)->expire_at_us != 0)
    {
      if ((*ent)->expire_at_us < now_us)
	{
	  entry_dispose_atomic (cache, *ent);
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
do_zadd (Cache *cache, const char **cmd, Buffer *out)
{
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    {
      return out_err (out, ERR_ARG, "expect fp number");
    }

  HNode *hnode = hm_lookup_by_key (&cache->db, cmd[1]);

  Entry *ent = NULL;
  if (!hnode)
    {
      ent = entry_new_zset (cache, cmd[1]);
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
do_zrem (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
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
do_zscore (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
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
do_zquery (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
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
do_keys (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  // Start the array response (writes a placeholder for the count)
  size_t idx = out_arr_begin (out);
  if (idx == 0)
    return false;
  ScanContext ctx = {.out = out,.count = 0,.pattern = cmd[1],.result = true };
  ctx.now_us = now_us;

  // We cannot stop the scan if a write fails, but the subsequent calls will
  // also fail, and the final count will be accurate.
  hm_scan (&cache->db, &cb_scan, &ctx);

  if (!ctx.result)
    {
      return false;
    }
  // Finalize the array output with the actual number of elements written. If the buffer is full return false.
  return out_arr_end (out, idx, ctx.count);
}

static int
del (Cache *cache, const char *key)
{
  Entry entry_key;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
  entry_key.key = (char *) key;
#pragma GCC diagnostic pop
  entry_key.node.hcode =
    str_hash ((uint8_t *) entry_key.key, strlen (entry_key.key));

  HNode *node = hm_pop (&cache->db, &entry_key.node, &entry_eq);
  if (node)
    {
      Entry *entry = fetch_entry (node);
      entry_del (cache, entry, (uint64_t) - 1);
      return 1;
    }
  return 0;
}

static bool
do_del (Cache *cache, const char **cmd, Buffer *out)
{
  return out_int (out, del (cache, cmd[1]));
}

static bool
do_mdel (Cache *cache, const char **cmd, size_t nkeys, Buffer *out)
{
  int64_t num = 0;
  for (size_t i = 0; i < nkeys; ++i)
    {
      num += del (cache, cmd[i + 1]);
    }
  return out_int (out, num);
}

static void
entry_set (Cache *cache, const char *key, const char *val)
{
  HNode *node = hm_lookup_by_key (&cache->db, key);
  if (node)
    {
      Entry *ent = fetch_entry (node);
      if (ent->type != T_STR)
	{
	  entry_dispose_atomic (cache, ent);
	  entry_new_str (cache, key, val);
	  return;
	}
      if (ent->val)
	{
	  free (ent->val);
	}
      ent->val = calloc (strlen (val) + 1, sizeof (char));
      if (!ent->val)
	die ("Out of memory");
      strcpy (ent->val, val);
    }
  else
    entry_new_str (cache, key, val);
}

static bool
do_set (Cache *cache, const char **cmd, Buffer *out)
{
  entry_set (cache, cmd[1], cmd[2]);
  if (out->proto == PROTO_RESP)
    return out_ok (out);
  return out_nil (out);
}

static bool
do_mset (Cache *cache, const char **cmd, size_t nkeys, Buffer *out)
{
  for (size_t i = 0; i < nkeys; i = i + 2)
    {
      entry_set (cache, cmd[i + 1], cmd[i + 2]);
    }

  return out_nil (out);
}

static bool
do_get (Cache *cache, const char *key, Buffer *out, uint64_t now_us)
{
  HNode *node = hm_lookup_by_key (&cache->db, key);
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
	  entry_dispose_atomic (cache, ent);
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
do_mget (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	 uint64_t now_us)
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
do_expire (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  int64_t ttl_ms = 0;
  if (!str2int (cmd[2], &ttl_ms))
    {
      return out_err (out, ERR_ARG, "expect int64");
    }

  HNode *node = hm_lookup_by_key (&cache->db, cmd[1]);
  if (node)
    {
      Entry *ent = fetch_entry (node);
      entry_set_ttl (cache, now_us, ent, ttl_ms);
    }
  return out_int (out, node ? 1 : 0);
}

static bool
do_ttl (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  HNode *node = hm_lookup_by_key (&cache->db, cmd[1]);
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
cache_execute (Cache *cache, const char **cmd, size_t size, Buffer *out,
	       uint64_t now_us)
{
  if (size > 0 && cmd_is (cmd[0], "ping"))
    {
      return do_ping (cache, cmd, size, out);
    }
  if (size > 0 && cmd_is (cmd[0], "config"))
    {
      return do_config (cache, cmd, size, out);
    }
  if (size == 2 && cmd_is (cmd[0], "keys"))
    {
      return do_keys (cache, cmd, out, now_us);
    }
  if (size > 2 && size % 2 == 1 && cmd_is (cmd[0], "mset"))
    {
      return do_mset (cache, cmd, size - 1, out);
    }
  if (size == 2 && cmd_is (cmd[0], "get"))
    {
      return do_get (cache, cmd[1], out, now_us);
    }
  if (size > 1 && cmd_is (cmd[0], "mget"))
    {
      return do_mget (cache, cmd, size - 1, out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "set"))
    {
      return do_set (cache, cmd, out);
    }
  if (size == 2 && cmd_is (cmd[0], "del"))
    {
      return do_del (cache, cmd, out);
    }
  if (size > 1 && cmd_is (cmd[0], "mdel"))
    {
      return do_mdel (cache, cmd, size - 1, out);
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
      entry_dispose_atomic (cache, ent);
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
