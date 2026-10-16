#ifndef _T_LIST_H_
#define _T_LIST_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "cache/minis.h"

// LPUSH/RPUSH: pushes vals in order, creating the list if needed
MinisError minis_push (Minis * minis, const char *key, const char **vals,
		       size_t count, bool front, size_t *out_len,
		       uint64_t now_us);

// LPOP/RPOP: visits up to count elements from one end, then removes
// them. If the visitor fails nothing is removed. MINIS_ERR_NIL if the
// key is missing.
MinisError minis_pop (Minis * minis, const char *key, size_t count,
		      bool front, MinisListCb cb, void *ctx,
		      uint64_t now_us);

// LRANGE: visits elements start..stop inclusive. Negative indexes count
// from the end, out of range ones are clamped like in Redis.
MinisError minis_lrange (Minis * minis, const char *key, int64_t start,
			 int64_t stop, MinisListCb cb, void *ctx,
			 uint64_t now_us);

MinisError minis_llen (Minis * minis, const char *key, size_t *out_len,
		       uint64_t now_us);

#endif // _T_LIST_H_
