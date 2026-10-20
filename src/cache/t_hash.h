#ifndef _T_HASH_H_
#define _T_HASH_H_

#include <stdint.h>

#include "cache/minis.h"


// Fields and values may hold NUL bytes, hence their lengths. pairs holds
// npairs fields, each followed by its value, set under one lock; out_added
// is how many of the fields were new.
MinisError minis_hset (Minis * minis, const char *key, size_t key_len,
		       const char **pairs, const size_t *pair_lens,
		       size_t npairs, int64_t *out_added, uint64_t now_us);
MinisError
minis_hget (Minis * minis, const char *key, size_t key_len, const char *field,
	    size_t field_len, MinisHashCb func, void *ctx, uint64_t now_us);
MinisError
// field_lens: the fields' lengths, NULL if they are C strings
minis_hdel (Minis * minis, const char *key, size_t key_len, const char **fields,
	    const size_t *field_lens, size_t field_count, int *out_deleted,
	    uint64_t now_us);
MinisError minis_hexists (Minis * minis, const char *key, size_t key_len, const char *field,
			  size_t field_len, int *out_exists, uint64_t now_us);
MinisError minis_hgetall (Minis * minis, const char *key, size_t key_len, MinisHashCb cb,
			  void *ctx, uint64_t now_us);
MinisError minis_hlen (Minis * minis, const char *key, size_t key_len, size_t *out_count,
		       uint64_t now_us);

#endif //  _T_HASH_H_
