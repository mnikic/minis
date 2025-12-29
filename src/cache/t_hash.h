#ifndef _T_HASH_H_
#define _T_HASH_H_

#include <stdint.h>

#include "cache/minis.h"


MinisError minis_hset (Minis * minis, const char *key, const char *field,
		       const char *value, int *out_added, uint64_t now_us);
MinisError minis_hget (Minis * minis, const char *key, const char *field,
		       const char **out_val, uint64_t now_us);
MinisError minis_hdel (Minis * minis, const char *key, const char *field,
		       int *out_deleted, uint64_t now_us);
MinisError minis_hexists (Minis * minis, const char *key, const char *field,
			  int *out_exists, uint64_t now_us);
MinisError minis_hgetall (Minis * minis, const char *key, MinisHashCb cb,
			  void *ctx, uint64_t now_us);
MinisError minis_hlen (Minis * minis, const char *key, size_t *out_count,
		       uint64_t now_us);

#endif //  _T_HASH_H_
