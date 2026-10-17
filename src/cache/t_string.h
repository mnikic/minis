#ifndef _T_STRING_H_
#define _T_STRING_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "cache/minis.h"

MinisError minis_get (Minis * minis, const char *key, size_t key_len,
		      MinisOneEntryVisitor visitor, void *ctx,
		      uint64_t now_us);
MinisError minis_mget (Minis * minis, const char **keys,
		       const size_t *key_lens, size_t count,
		       MinisOneEntryVisitor visitor, void *ctx,
		       uint64_t now_us);


// Values may hold NUL bytes, hence their lengths
MinisError
minis_set (Minis * minis, const char *key, size_t key_len, const char *val, size_t val_len,
	   uint64_t now_us);
MinisError
minis_set_ex (Minis * minis, const char *key, size_t key_len, const char *val,
	      size_t val_len, int64_t ttl_ms, uint64_t now_us);
// lens[i] is key_vals[i]'s length
MinisError
minis_mset (Minis * minis, const char **key_vals, const size_t *lens,
	    size_t count, uint64_t now_us);


MinisError minis_incr (Minis * minis, const char *key, size_t key_len, int64_t delta,
		       int64_t * out_val, uint64_t now_us);

#endif //  _T_STRING_H_
