#include <inttypes.h>
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
#include "cache/hash.h"
#include "cache/thread_pool.h"
#include "io/proto_defs.h"
#include "io/buffer.h"
#include "io/out.h"
#include "common/common.h"
#include "common/glob.h"
#include "common/macros.h"

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

static int
hnode_same (HNode *lhs, HNode *rhs)
{
  return lhs == rhs;
}

bool
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
      return heap_add (&cache->heap, &item);
    }
  // Update existing item in the heap
  heap_get (&cache->heap, pos)->val = expire_at_us;
  heap_update (&cache->heap, pos);
  return true;
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
      break;
    case T_HASH:
      hash_dispose (ent->hash);
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
  if (ent->hash)
    free (ent->hash);
  if (ent->zset)
    free (ent->zset);

  free (ent);
}

// Helper for asynchronous cleanup
static void
entry_del_async (void *arg)
{
  entry_destroy ((Entry *) arg);
}

// set or remove the TTL
bool
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
      if (new_expire_at_us == ent->expire_at_us)
	return false;
      cache->dirty_count++;
      return entry_set_expiration (cache, ent, new_expire_at_us);
    }
  return true;
}

static void
entry_del (Cache *cache, Entry *ent, uint64_t now_us)
{
  // Ensure TTL is removed from the heap and expire_at_us is set to 0.
  bool success = entry_set_ttl (cache, now_us, ent, -1);
  (void) success;

  const size_t k_large_container_size = 10000;
  bool too_big = false;
  switch (ent->type)
    {
    case T_ZSET:
      too_big = hm_size (&ent->zset->hmap) > k_large_container_size;
      break;
    case T_HASH:
      too_big = hm_size (ent->hash) > k_large_container_size;
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

static Entry *
fetch_entry_expiry_aware (Cache *cache, HNode *node_to_fetch, uint64_t now_us)
{
// This pragma block is necessary for container_of, which performs type punning.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  Entry *entry = container_of (node_to_fetch, Entry, node);
#pragma GCC diagnostic pop
  if (entry->expire_at_us != 0)
    {
      if (entry->expire_at_us < now_us)
	{
	  entry_dispose_atomic (cache, entry);
	  entry = NULL;
	  return NULL;
	}
    }

  return entry;
}

static int
entry_eq (HNode *lhs, HNode *rhs)
{
  Entry *lentry = fetch_entry (lhs);	// Standard fetch
  Entry *rentry = fetch_entry (rhs);

  return lhs->hcode == rhs->hcode
    && (lentry != NULL && rentry != NULL && lentry->key != NULL
	&& rentry->key != NULL && strcmp (lentry->key, rentry->key) == 0);
}

static Entry *
entry_lookup_by_key (Cache *cache, const char *key, uint64_t now_us)
{
  Entry entry_key;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
  entry_key.key = (char *) key;
#pragma GCC diagnostic pop
  entry_key.node.hcode =
    str_hash ((uint8_t *) entry_key.key, strlen (entry_key.key));
  HNode *node = hm_lookup (&cache->db, &entry_key.node, &entry_eq);
  if (!node)
    return NULL;
  Entry *entry = fetch_entry_expiry_aware (cache, node, now_us);

  if (!entry)
    return NULL;		// Was expired and deleted
  return entry;
}

static Entry *
entry_new (Cache *cache, const char *key, EntryType type)
{
  Entry *ent = calloc (1, sizeof (Entry));
  if (!ent)
    {
      msg ("Out of memory in entry_new.");
      return NULL;
    }
  ent->key = calloc (strlen (key) + 1, sizeof (char));
  if (!ent->key)
    {
      free (ent);
      msg ("Out of memory in entry_new key allocation.");
      return NULL;
    }
  strcpy (ent->key, key);
  ent->node.hcode = str_hash ((const uint8_t *) key, strlen (key));
  ent->type = type;
  ent->heap_idx = (size_t) -1;
  ent->expire_at_us = 0;
  hm_insert (&cache->db, &ent->node, &entry_eq);
  return ent;
}

Entry *
entry_new_zset (Cache *cache, const char *key)
{
  Entry *ent = entry_new (cache, key, T_ZSET);
  if (!ent)
    {
      return NULL;
    }

  ent->zset = calloc (1, sizeof (ZSet));
  if (!ent->zset)
    {
      entry_dispose_atomic (cache, ent);
      ent = NULL;
      msg ("Out of memory for new zset.");
      return NULL;
    }

  return ent;
}

Entry *
entry_new_str (Cache *cache, const char *key, const char *val)
{
  Entry *ent = entry_new (cache, key, T_STR);
  if (!ent)
    {
      return NULL;
    }
  ent->val = calloc (strlen (val) + 1, sizeof (char));
  if (!ent->val)
    {
      entry_dispose_atomic (cache, ent);
      ent = NULL;
      msg ("Out of memory for new str entity.");
      return NULL;
    }
  strcpy (ent->val, val);
  return ent;
}

Entry *
entry_new_hash (Cache *cache, const char *key)
{
  Entry *ent = entry_new (cache, key, T_HASH);
  if (!ent)
    {
      return NULL;
    }
  ent->hash = calloc (1, sizeof (HMap));
  if (!ent->hash)
    {
      entry_dispose_atomic (cache, ent);
      ent = NULL;
      msg ("Out of memory for new hash.");
      return NULL;
    }
  hm_init (ent->hash);
  return ent;
}

Entry *
fetch_or_create_entry (Cache *cache, const char *key, uint64_t now_us)
{
  Entry *entry = entry_lookup_by_key (cache, key, now_us);
  if (!entry)
    return entry_new_hash (cache, key);

  return entry;
}

static bool
do_ping (Cache *cache, const char **args, size_t arg_count, Buffer *out)
{
  (void) cache;
  if (arg_count == 1)
    {
      return out_simple_str (out, "PONG");
    }
  if (arg_count == 2)
    {
      // Echo the argument back as a Bulk String
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
  bool write;
} ScanContext;

static void
cb_scan (HNode *node, void *arg)
{
  ScanContext *ctx = (ScanContext *) arg;
  if (!ctx->result)
    return;
  // We don't call functins that remove the element if expired automatically here
  // So we have to check expiration here.
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
  if (ctx->write)
    {
      if (out_str (ctx->out, fetch_entry (node)->key))
	{
	  ctx->count++;
	}
      else
	ctx->result = false;
    }
  else
    {
      ctx->count++;
    }
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

  *ent = entry_lookup_by_key (cache, key, now_us);
  if (!(*ent))
    return out_nil (out);

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
do_zadd (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    {
      return out_err (out, ERR_ARG, "expect fp number");
    }

  Entry *ent = entry_lookup_by_key (cache, cmd[1], now_us);
  if (!ent)
    {
      ent = entry_new_zset (cache, cmd[1]);
    }
  if (!ent)
    {
      out_err (out, ERR_TYPE, "Out of memory");
      return false;
    }
  if (ent->type != T_ZSET)
    {
      return out_err (out, ERR_TYPE, "expect zset");
    }

  const char *name = cmd[3];
  int added = zset_add (ent->zset, name, strlen (name), score);
  if (added == 1)
    cache->dirty_count++;
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
      cache->dirty_count++;
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
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    {
      return out_err (out, ERR_ARG, "expect fp number");
    }

  int64_t offset = 0;
  if (!str2int (cmd[4], &offset))
    {
      return out_err (out, ERR_ARG, "expect int");
    }

  int64_t limit = 0;
  if (!str2int (cmd[5], &limit))
    {
      return out_err (out, ERR_ARG, "expect int");
    }

  Entry *ent = NULL;
  bool is_valid_zset;

  // expect_zset handles lookup, passive expiration check, and type check
  if (!expect_zset (cache, out, cmd[1], &ent, &is_valid_zset, now_us))
    {
      return false;
    }

  if (!is_valid_zset)
    {
      // If expect_zset wrote SER_NIL (key not found), clear it and return Empty Array.
      // If it wrote SER_ERR (wrong type), leave it and return true.
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

  // Find Start Node
  // This is common to both paths.
  ZNode *znode = zset_query (ent->zset, score, cmd[3], strlen (cmd[3]));
  znode = znode_offset (znode, offset);

  // BINARY PROTOCOL (Single Pass + Patching)
  if (out->proto == PROTO_BIN)
    {
      size_t idx = out_arr_begin (out);
      if (idx == 0)
	return false;

      uint32_t num = 0;
      // Loop until we hit the limit or the end of the ZSet
      while (znode && (int64_t) (num / 2) < limit)
	{
	  if (!out_str_size (out, znode->name, znode->len))
	    return false;
	  if (!out_dbl (out, znode->score))
	    return false;

	  znode = znode_offset (znode, +1);
	  num += 2;		// Each member is 2 elements (Name + Score)
	}
      return out_arr_end (out, idx, num);
    }

  // RESP PROTOCOL (Double Pass: Count -> Write)
  // We cannot patch RESP arrays, so we must count the available items first.
  ZNode *iter = znode;
  uint32_t actual_count = 0;
  while (iter && (int64_t) actual_count < limit)
    {
      actual_count++;
      iter = znode_offset (iter, +1);
    }

  // Write Header: The array size is count * 2 (Name + Score)
  if (!out_arr (out, (size_t) (actual_count * (size_t) 2)))
    return false;

  // Write Data
  iter = znode;
  uint32_t written = 0;
  while (iter && written < actual_count)
    {
      if (!out_str_size (out, iter->name, iter->len))
	return false;
      // out_dbl handles formatting (binary double vs string "3.14")
      if (!out_dbl (out, iter->score))
	return false;

      iter = znode_offset (iter, +1);
      written++;
    }

  return true;
}

static bool
do_keys (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  // Initialize context with common fields
  ScanContext ctx = {
    .out = out,
    .count = 0,
    .pattern = cmd[1],
    .result = true,
    .now_us = now_us,
    // .write is set below
  };

  // PATH BINARY (Single Pass + Patching)
  if (out->proto == PROTO_BIN)
    {
      size_t idx = out_arr_begin (out);
      if (idx == 0)
	return false;

      ctx.write = true;		// Write immediately
      hm_scan (&cache->db, &cb_scan, &ctx);

      if (!ctx.result)
	return false;
      return out_arr_end (out, idx, ctx.count);
    }

  // PATH RESP (Two Pass: Count -> Header -> Write)
  ctx.write = false;
  hm_scan (&cache->db, &cb_scan, &ctx);

  // Write the Header (*N\r\n)
  // We use the count we just gathered
  if (!out_arr (out, ctx.count))
    return false;

  // Write Data
  ctx.write = true;
  ctx.count = 0;

  // because ctx.write == true this actually writes!
  hm_scan (&cache->db, &cb_scan, &ctx);

  return ctx.result;
}

static int
del (Cache *cache, const char *key, uint64_t now_us)
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
      int ret_val = 1;
      if (entry->expire_at_us != 0 && entry->expire_at_us < now_us)
	{
	  ret_val = 0;		// We pretend we didn't find it
	}
      entry_del (cache, entry, (uint64_t) - 1);
      return ret_val;
    }
  return 0;
}

static bool
do_del (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  int deleted = del (cache, cmd[1], now_us);
  if (deleted == 1)
    cache->dirty_count++;

  return out_int (out, deleted);
}

static bool
do_mdel (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	 uint64_t now_us)
{
  int64_t num = 0;
  for (size_t i = 0; i < nkeys; ++i)
    {
      num += del (cache, cmd[i + 1], now_us);
    }
  cache->dirty_count += (uint64_t) num;
  return out_int (out, num);
}

static bool
entry_set (Cache *cache, const char *key, const char *val, uint64_t now_us)
{
  Entry *ent = entry_lookup_by_key (cache, key, now_us);
  if (ent)
    {
      if (ent->type != T_STR)
	{
	  entry_dispose_atomic (cache, ent);
	  Entry *new = entry_new_str (cache, key, val);
	  return new != NULL;
	}
      if (ent->val)
	{
	  if (strcmp (ent->val, val) == 0)
	    return false;
	  free (ent->val);
	}
      ent->val = calloc (strlen (val) + 1, sizeof (char));
      if (!ent->val)
	{
	  msg ("Out of memory in entry_new_str value allocation.");
	  return false;
	}
      strcpy (ent->val, val);
      return true;
    }
  return entry_new_str (cache, key, val) != NULL;
}

static bool
do_exists (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	   uint64_t now_us)
{
  int64_t hits = 0;
  for (size_t i = 0; i < nkeys; ++i)
    {
      const char *key = cmd[i + 1];
      Entry *ent = entry_lookup_by_key (cache, key, now_us);

      if (ent)
	{
	  hits++;
	}
    }
  return out_int (out, hits);
}

static bool
do_incr (Cache *cache, const char *key, int64_t delta, Buffer *out,
	 uint64_t now_us)
{
  Entry *ent = entry_lookup_by_key (cache, key, now_us);
  int64_t val = 0;

  if (ent)
    {
      if (ent->type != T_STR)
	{
	  return out_err (out, ERR_TYPE,
			  "value is not an integer or out of range");
	}

      char *endptr = NULL;
      val = strtoll (ent->val, &endptr, 10);

      if (ent->val[0] == '\0' || *endptr != '\0')
	{
	  return out_err (out, ERR_ARG,
			  "value is not an integer or out of range");
	}
    }

  // If node was NULL (or expired), val is 0. 
  // We effectively do: 0 + delta.

  // Overflow Check
  if ((delta > 0 && val > INT64_MAX - delta) ||
      (delta < 0 && val < INT64_MIN - delta))
    {
      return out_err (out, ERR_ARG, "increment or decrement would overflow");
    }

  val += delta;

  // Store Back as String
  char val_str[32];
  snprintf (val_str, sizeof (val_str), "%" PRId64, val);

  if (entry_set (cache, key, val_str, now_us))
    {
      cache->dirty_count++;
    }
  else
    {
      return out_err (out, ERR_UNKNOWN, "OOM updating integer");
    }

  return out_int (out, val);
}

static bool
do_hdel (Cache *cache, const char **cmd, size_t argc, Buffer *out,
	 uint64_t now_us)
{
  // cmd[0] is "HDEL", cmd[1] is key, cmd[2...] are fields
  Entry *ent = entry_lookup_by_key (cache, cmd[1], now_us);
  if (!ent || ent->type != T_HASH)
    return out_int (out, 0);

  int deleted_count = 0;
  for (size_t i = 2; i < argc; i++)
    {
      if (hash_del (ent->hash, cmd[i]))
	deleted_count++;
    }

  if (deleted_count > 0)
    {
      cache->dirty_count++;
      if (hm_size (ent->hash) == 0)
	entry_dispose_atomic (cache, ent);
    }

  return out_int (out, deleted_count);
}

static bool
do_hgetall (Cache *cache, const char *key, Buffer *out, uint64_t now_us)
{
  Entry *ent = entry_lookup_by_key (cache, key, now_us);
  if (!ent || ent->type != T_HASH)
    return out_arr (out, 0);	// Empty array

  size_t size = hm_size (ent->hash);
  if (!out_arr (out, size * 2))
    return false;		// *2 because Field + Value

  HMIter iter;
  hm_iter_init (ent->hash, &iter);
  HNode *node;
  while ((node = hm_iter_next (&iter)))
    {
      HashEntry *hash_entry = fetch_hash_entry (node);
      if (!out_str (out, hash_entry->field))
	return false;
      if (!out_str (out, hash_entry->value))
	return false;
    }
  return true;
}

static bool
do_hset (Cache *cache, const char *key, const char *field, const char *value,
	 Buffer *out, uint64_t now_us)
{
  Entry *ent = fetch_or_create_entry (cache, key, now_us);
  if (ent->type != T_HASH)
    return out_err (out, ERR_TYPE, "WRONGTYPE...");

  // 2. Insert into Inner Map
  // We need a helper to create/update HashEntry (similar to entry_set but for fields)
  int result = hash_set (ent->hash, field, value);

  if (result)
    cache->dirty_count++;

  // Redis returns number of fields added (not updated)
  return out_int (out, result);
}

static bool
do_hget (Cache *cache, const char *key, const char *field, Buffer *out,
	 uint64_t now_us)
{
  Entry *ent = entry_lookup_by_key (cache, key, now_us);
  if (!ent)
    return out_nil (out);
  if (ent->type != T_HASH)
    return out_err (out, ERR_TYPE,
		    "WRONGTYPE Operation against a key holding the wrong kind of value");

  HashEntry *hash = hash_lookup (ent->hash, field);
  if (!hash)
    return out_nil (out);

  return out_str (out, hash->value);
}

static bool
do_hexists (Cache *cache, const char *key, const char *field, Buffer *out,
	    uint64_t now_us)
{
  Entry *ent = entry_lookup_by_key (cache, key, now_us);
  if (!ent)
    return out_int (out, 0);
  if (ent->type != T_HASH)
    return out_err (out, ERR_TYPE,
		    "WRONGTYPE Operation against a key holding the wrong kind of value");

  HashEntry *hash = hash_lookup (ent->hash, field);
  return out_int (out, hash ? 1 : 0);
}

static bool
do_set (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  if (entry_set (cache, cmd[1], cmd[2], now_us))
    cache->dirty_count++;

  // Unified: Returns +OK in RESP, "OK" string in Binary
  return out_ok (out);
}

static bool
do_mset (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	 uint64_t now_us)
{
  for (size_t i = 0; i < nkeys; i = i + 2)
    {
      if (entry_set (cache, cmd[i + 1], cmd[i + 2], now_us))
	cache->dirty_count++;
    }

  return out_ok (out);
}

static bool
do_get (Cache *cache, const char *key, Buffer *out, uint64_t now_us)
{
  Entry *ent = entry_lookup_by_key (cache, key, now_us);
  if ((!ent || ent->type != T_STR) || !ent->val)
    {
      return out_nil (out);
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
    return out_err (out, ERR_ARG, "expect int64");

  Entry *ent = entry_lookup_by_key (cache, cmd[1], now_us);
  if (ent)
    {
      entry_set_ttl (cache, now_us, ent, ttl_ms);
      cache->dirty_count++;
    }
  return out_int (out, ent ? 1 : 0);
}

static bool
do_ttl (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  Entry *ent = entry_lookup_by_key (cache, cmd[1], now_us);
  if (!ent)
    return out_int (out, -2);	// Expired, or not there, return 0
  // Check if TTL is set (0 means no TTL, or already expired/persisted)
  if (ent->expire_at_us == 0)
    return out_int (out, -1);	// TTL not set

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
      return do_mset (cache, cmd, size - 1, out, now_us);
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
      return do_set (cache, cmd, out, now_us);
    }
  if (size == 2 && cmd_is (cmd[0], "del"))
    {
      return do_del (cache, cmd, out, now_us);
    }
  if (size > 1 && cmd_is (cmd[0], "mdel"))
    {
      return do_mdel (cache, cmd, size - 1, out, now_us);
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
      return do_zadd (cache, cmd, out, now_us);
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
  if (size == 2 && cmd_is (cmd[0], "incr"))
    {
      return do_incr (cache, cmd[1], 1, out, now_us);
    }
  if (size == 2 && cmd_is (cmd[0], "decr"))
    {
      return do_incr (cache, cmd[1], -1, out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "incrby"))
    {
      int64_t delta;
      if (!str2int (cmd[2], &delta))
	return out_err (out, ERR_ARG, "expect int");
      return do_incr (cache, cmd[1], delta, out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "decrby"))
    {
      int64_t delta;
      if (!str2int (cmd[2], &delta))
	return out_err (out, ERR_ARG, "expect int");
      return do_incr (cache, cmd[1], -delta, out, now_us);
    }
  if (size >= 2 && cmd_is (cmd[0], "exists"))
    {
      return do_exists (cache, cmd, size - 1, out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "hget"))
    {
      return do_hget (cache, cmd[1], cmd[2], out, now_us);
    }
  if (size == 4 && cmd_is (cmd[0], "hset"))
    {
      return do_hset (cache, cmd[1], cmd[2], cmd[3], out, now_us);
    }
  if (size > 2 && cmd_is (cmd[0], "hdel"))
    {
      return do_hdel (cache, cmd, size, out, now_us);
    }
  if (size == 2 && cmd_is (cmd[0], "hgetall"))
    {
      return do_hgetall (cache, cmd[1], out, now_us);
    }
  if (size == 3 && cmd_is (cmd[0], "hexists"))
    {
      return do_hexists (cache, cmd[1], cmd[2], out, now_us);
    }

  return out_err (out, ERR_UNKNOWN, "Unknown cmd");
}

Cache *
cache_init (void)
{
  Cache *cache = calloc (1, sizeof (Cache));
  if (!cache)
    die ("Out of memory in cache_init");
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
