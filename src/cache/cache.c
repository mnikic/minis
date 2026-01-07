#include "cache/cache.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <math.h>
#include <stdint.h>

#include "cache/hash.h"
#include "io/out.h"
#include "io/proto_defs.h"
#include "io/buffer.h"
#include "common/common.h"
#include "common/macros.h"
#include "cache/minis.h"
#include "cache/t_hash.h"
#include "cache/t_string.h"
#include "cache/t_zset.h"

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

static ALWAYS_INLINE bool
cmd_is (const char *word, const char *cmd)
{
  return 0 == strcasecmp (word, cmd);
}

// Maps MinisError to Redis Protocol Errors
static bool
reply_with_error (Buffer *out, MinisError err)
{
  switch (err)
    {
    case MINIS_ERR_NIL:
      return out_nil (out);
    case MINIS_ERR_TYPE:
      return out_err (out, ERR_TYPE,
		      "WRONGTYPE Operation against a key holding the wrong kind of value");
    case MINIS_ERR_ARG:
      return out_err (out, ERR_ARG,
		      "value is not an integer or out of range");
    case MINIS_ERR_OOM:
      return out_err (out, ERR_UNKNOWN, "Out of memory");
    case MINIS_OK:
      return true;
    case MINIS_ERR_UNKNOWN:
      // fallsthrouh
    default:
      return out_err (out, ERR_UNKNOWN, "Unknown error");
    }
}

// Lifecycle Wrappers

Cache *
cache_init (void)
{
  return minis_init ();
}

void
cache_free (Cache *cache)
{
  minis_free (cache);
}

void
cache_evict (Cache *cache, uint64_t now_us)
{
  minis_evict (cache, now_us);
}

uint64_t
cache_next_expiry (Cache *cache)
{
  return minis_next_expiry (cache);
}

// Server Control Commands

static bool
do_ping (Cache *cache, const char **args, size_t arg_count, Buffer *out)
{
  (void) cache;
  if (arg_count == 1)
    return out_simple_str (out, "PONG");
  if (arg_count == 2)
    return out_str (out, args[1]);
  return out_err (out, ERR_ARG,
		  "wrong number of arguments for 'ping' command");
}

static bool
do_config (Cache *cache, const char **args, size_t arg_count, Buffer *out)
{
  (void) cache;
  (void) args;
  (void) arg_count;
  return out_arr (out, 0);
}

// Key Operations

static bool
do_del (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  int deleted = 0;
  minis_del (cache, cmd[1], &deleted, now_us);
  return out_int (out, deleted);
}

static bool
do_mdel (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	 uint64_t now_us)
{
  uint64_t total = 0;
  minis_mdel (cache, cmd, nkeys, &total, now_us);
  return out_int (out, (int64_t) total);
}

static bool
do_exists (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	   uint64_t now_us)
{
  int64_t hits;
  MinisError err = minis_exists (cache, cmd, nkeys, &hits, now_us);
  if (err == MINIS_OK)
    return out_int (out, hits);
  return reply_with_error (out, err);
}

static bool
do_expire (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  int64_t ttl_ms = 0;
  if (!str2int (cmd[2], &ttl_ms))
    return out_err (out, ERR_ARG, "expect int64");

  int set = 0;
  minis_expire (cache, cmd[1], ttl_ms, &set, now_us);
  return out_int (out, set);
}

static bool
do_ttl (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  int64_t ttl_val = 0;
  minis_ttl (cache, cmd[1], &ttl_val, now_us);
  return out_int (out, ttl_val);
}

// Visitor Callbacks for Arrays

typedef struct
{
  Buffer *out;
  bool counting_only;
  size_t count;
  bool result;
} VisitCtx;

static void
cb_keys_visitor (const char *key, void *arg)
{
  VisitCtx *ctx = (VisitCtx *) arg;
  if (!ctx->result)
    return;
  ctx->count++;
  if (!ctx->counting_only)
    {
      if (!out_str (ctx->out, key))
	ctx->result = false;
    }
}

static bool
do_keys (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  VisitCtx ctx = {.out = out,.counting_only = false,.count = 0,.result = true
  };

  // PATH 1: BINARY (Single Pass)
  if (out->proto == PROTO_BIN)
    {
      size_t idx = out_arr_begin (out);
      if (idx == 0)
	return false;

      minis_keys (cache, cmd[1], cb_keys_visitor, &ctx, now_us);
      if (!ctx.result)
	return false;
      return out_arr_end (out, idx, ctx.count);
    }

  // PATH 2: RESP (Double Pass)
  ctx.counting_only = true;
  minis_keys (cache, cmd[1], cb_keys_visitor, &ctx, now_us);

  if (!out_arr (out, ctx.count))
    return false;

  ctx.counting_only = false;
  minis_keys (cache, cmd[1], cb_keys_visitor, &ctx, now_us);

  return ctx.result;
}

typedef struct
{
  Buffer *out;
  bool result;
} GetVstCtx;

static bool
single_get_visitor (const Entry *ent, void *ctx)
{
  GetVstCtx *vctx = (GetVstCtx *) ctx;
  if (ent)
    {
      if (!out_str (vctx->out, ent->val))
	{
	  vctx->result = false;
	}
    }
  else
    vctx->result = out_nil (vctx->out);
  return vctx->result;
}

static bool
do_get (Cache *cache, const char *key, Buffer *out, uint64_t now_us)
{
  GetVstCtx ctx = {.out = out,.result = true };
  MinisError err = minis_get (cache, key, single_get_visitor, &ctx, now_us);
  if (err == MINIS_OK)
    return ctx.result;
  return reply_with_error (out, err);
}

static bool
do_set (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  MinisError err = minis_set (cache, cmd[1], cmd[2], now_us);
  if (err == MINIS_OK)
    return out_ok (out);
  return reply_with_error (out, err);
}

static bool
do_mset (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	 uint64_t now_us)
{
  MinisError err = minis_mset (cache, cmd, nkeys, now_us);
  if (err == MINIS_OK)
    return out_ok (out);
  return reply_with_error (out, err);
}

typedef struct
{
  Buffer *out;
  bool result;
} MgetVisitorCtx;

static bool
mget_visitor (const Entry *ent, void *ctx)
{
  MgetVisitorCtx *mvc = (MgetVisitorCtx *) ctx;
  if (!mvc->result)
    return false;
  if (ent)
    {
      if (!out_str (mvc->out, ent->val))
	mvc->result = false;
    }
  else if (!out_nil (mvc->out))
    mvc->result = false;

  return mvc->result;
}

static bool
do_mget (Cache *cache, const char **cmd, size_t nkeys, Buffer *out,
	 uint64_t now_us)
{
  if (!out_arr (out, nkeys))
    return false;
  MgetVisitorCtx ctx = {.out = out,.result = true };
  minis_mget (cache, cmd, nkeys, mget_visitor, &ctx, now_us);
  return ctx.result;
}

static bool
do_incr (Cache *cache, const char *key, int64_t delta, Buffer *out,
	 uint64_t now_us)
{
  int64_t val = 0;
  MinisError err = minis_incr (cache, key, delta, &val, now_us);
  if (err == MINIS_OK)
    return out_int (out, val);
  return reply_with_error (out, err);
}

// Hash Operations

static bool
do_hset (Cache *cache, const char *key, const char *field, const char *value,
	 Buffer *out, uint64_t now_us)
{
  int added = 0;
  MinisError err = minis_hset (cache, key, field, value, &added, now_us);
  if (err == MINIS_OK)
    return out_int (out, added);
  return reply_with_error (out, err);
}

static bool
hget_visitor (const HashEntry *entry, void *arg)
{
  Buffer *out = (Buffer *) arg;
  return out_str (out, entry->value);
}

static bool
do_hget (Cache *cache, const char *key, const char *field, Buffer *out,
	 uint64_t now_us)
{
  MinisError err = minis_hget (cache, key, field, hget_visitor, out, now_us);
  return reply_with_error (out, err);
}

static bool
do_hdel (Cache *cache, const char **cmd, size_t argc, Buffer *out,
	 uint64_t now_us)
{
  int total = 0;
  MinisError err =
    minis_hdel (cache, cmd[1], cmd + 2, argc - 2, &total, now_us);
  if (err == MINIS_OK)
    return out_int (out, total);
  return reply_with_error (out, err);
}

static bool
do_hexists (Cache *cache, const char *key, const char *field, Buffer *out,
	    uint64_t now_us)
{
  int exists = 0;
  MinisError err = minis_hexists (cache, key, field, &exists, now_us);
  if (err == MINIS_OK)
    return out_int (out, exists);
  return reply_with_error (out, err);
}

typedef struct
{
  Buffer *out;
  bool result;
} HashVisitorCtx;

static bool
cb_hash_visitor (const HashEntry *entry, void *arg)
{
  HashVisitorCtx *ctx = (HashVisitorCtx *) arg;
  if (!ctx->result)
    return false;
  if (!out_str (ctx->out, entry->field))
    {
      ctx->result = false;
      return false;
    }
  if (!out_str (ctx->out, entry->value))
    {
      ctx->result = false;
      return false;
    }
  return true;
}

static bool
do_hgetall (Cache *cache, const char *key, Buffer *out, uint64_t now_us)
{
  size_t count = 0;
  MinisError err = minis_hlen (cache, key, &count, now_us);

  if (err == MINIS_ERR_NIL)
    return out_arr (out, 0);
  if (err != MINIS_OK)
    return reply_with_error (out, err);

  if (!out_arr (out, count * 2))
    return false;
  HashVisitorCtx ctx = {.out = out,.result = true };
  minis_hgetall (cache, key, cb_hash_visitor, &ctx, now_us);
  return ctx.result;
}

static bool
do_zadd (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    return out_err (out, ERR_ARG, "expect fp number");

  int added = 0;
  MinisError err = minis_zadd (cache, cmd[1], score, cmd[3], &added, now_us);
  if (err == MINIS_OK)
    return out_int (out, added);
  return reply_with_error (out, err);
}

static bool
do_zrem (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  int removed = 0;
  MinisError err = minis_zrem (cache, cmd[1], cmd[2], &removed, now_us);
  if (err == MINIS_OK)
    return out_int (out, removed);
  return reply_with_error (out, err);
}

static bool
do_zscore (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  double score = 0;
  MinisError err = minis_zscore (cache, cmd[1], cmd[2], &score, now_us);
  if (err == MINIS_OK)
    return out_dbl (out, score);
  return reply_with_error (out, err);
}

// Context for ZQUERY visitor (counting logic not needed if using logic below)
typedef struct
{
  Buffer *out;
  bool count_only;
  int count;
} ZQueryCtx;

static void
cb_zquery (const char *name, size_t len, double score, void *arg)
{
  ZQueryCtx *ctx = (ZQueryCtx *) arg;
  ctx->count++;
  if (!ctx->count_only)
    {
      out_str_size (ctx->out, name, len);
      out_dbl (ctx->out, score);
    }
}

static bool
do_zquery (Cache *cache, const char **cmd, Buffer *out, uint64_t now_us)
{
  double score = 0;
  if (!str2dbl (cmd[2], &score))
    return out_err (out, ERR_ARG, "expect fp number");
  int64_t offset = 0;
  if (!str2int (cmd[4], &offset))
    return out_err (out, ERR_ARG, "expect int");
  int64_t limit = 0;
  if (!str2int (cmd[5], &limit))
    return out_err (out, ERR_ARG, "expect int");

  // Setup context for the single pass (write immediately)
  ZQueryCtx ctx = {.out = out,.count_only = false,.count = 0 };

  // --- PATH 1: BINARY PROTOCOL (Single Pass + Patching) ---
  if (out->proto == PROTO_BIN)
    {
      size_t patch_idx = out_arr_begin (out);
      if (patch_idx == 0)
	return false;

      // Single call to core logic
      MinisError err =
	minis_zquery (cache, cmd[1], score, cmd[3], offset, limit,
		      cb_zquery, &ctx, now_us);

      if (err != MINIS_OK && err != MINIS_ERR_NIL)
	return reply_with_error (out, err);

      // Patch the header with the actual count we visited
      return out_arr_end (out, patch_idx, (size_t) ctx.count * 2);
    }

  // RESP PROTOCOL (Double Pass: Count -> Write)
  // RESP cannot easily patch headers because they are variable length text (*10\r\n vs *9\r\n)

  // Pass 1: Count Only
  ctx.count_only = true;
  MinisError err = minis_zquery (cache, cmd[1], score, cmd[3], offset, limit,
				 cb_zquery, &ctx, now_us);

  if (err == MINIS_ERR_NIL)
    {
      return out_arr (out, 0);
    }
  if (err != MINIS_OK)
    return reply_with_error (out, err);

  // Write Header
  if (!out_arr (out, (size_t) ctx.count * 2))
    return false;

  // Write Data
  ctx.count_only = false;
  // Reset count if you want, though irrelevant for writing
  minis_zquery (cache, cmd[1], score, cmd[3], offset, limit,
		cb_zquery, &ctx, now_us);

  return true;
}

bool
cache_execute (Cache *cache, const char **cmd, size_t size, Buffer *out,
	       uint64_t now_us)
{
  if (size > 0 && cmd_is (cmd[0], "ping"))
    return do_ping (cache, cmd, size, out);
  if (size > 0 && cmd_is (cmd[0], "config"))
    return do_config (cache, cmd, size, out);
  if (size == 2 && cmd_is (cmd[0], "keys"))
    return do_keys (cache, cmd, out, now_us);
  if (size > 2 && size % 2 == 1 && cmd_is (cmd[0], "mset"))
    return do_mset (cache, &cmd[1], size - 1, out, now_us);
  if (size == 2 && cmd_is (cmd[0], "get"))
    return do_get (cache, cmd[1], out, now_us);
  if (size > 1 && cmd_is (cmd[0], "mget"))
    return do_mget (cache, cmd + 1, size - 1, out, now_us);
  if (size == 3 && cmd_is (cmd[0], "set"))
    return do_set (cache, cmd, out, now_us);
  if (size == 2 && cmd_is (cmd[0], "del"))
    return do_del (cache, cmd, out, now_us);
  if (size > 1 && cmd_is (cmd[0], "mdel"))
    return do_mdel (cache, &cmd[1], size - 1, out, now_us);
  if (size == 3 && cmd_is (cmd[0], "pexpire"))
    return do_expire (cache, cmd, out, now_us);
  if (size == 2 && cmd_is (cmd[0], "pttl"))
    return do_ttl (cache, cmd, out, now_us);
  if (size == 4 && cmd_is (cmd[0], "zadd"))
    return do_zadd (cache, cmd, out, now_us);
  if (size == 3 && cmd_is (cmd[0], "zrem"))
    return do_zrem (cache, cmd, out, now_us);
  if (size == 3 && cmd_is (cmd[0], "zscore"))
    return do_zscore (cache, cmd, out, now_us);
  if (size == 6 && cmd_is (cmd[0], "zquery"))
    return do_zquery (cache, cmd, out, now_us);
  if (size == 2 && cmd_is (cmd[0], "incr"))
    return do_incr (cache, cmd[1], 1, out, now_us);
  if (size == 2 && cmd_is (cmd[0], "decr"))
    return do_incr (cache, cmd[1], -1, out, now_us);
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
    return do_exists (cache, cmd + 1, size - 1, out, now_us);
  if (size == 3 && cmd_is (cmd[0], "hget"))
    return do_hget (cache, cmd[1], cmd[2], out, now_us);
  if (size == 4 && cmd_is (cmd[0], "hset"))
    return do_hset (cache, cmd[1], cmd[2], cmd[3], out, now_us);
  if (size > 2 && cmd_is (cmd[0], "hdel"))
    return do_hdel (cache, cmd, size, out, now_us);
  if (size == 2 && cmd_is (cmd[0], "hgetall"))
    return do_hgetall (cache, cmd[1], out, now_us);
  if (size == 3 && cmd_is (cmd[0], "hexists"))
    return do_hexists (cache, cmd[1], cmd[2], out, now_us);

  return out_err (out, ERR_UNKNOWN, "Unknown cmd");
}
