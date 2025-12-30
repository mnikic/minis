#ifndef _T_STRING_H_
#define _T_STRING_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "cache/minis.h"

// --- String Operations ---

MinisError
minis_get (Minis * minis, const char *key, const char **out_val,
	   uint64_t now_us);
MinisError
minis_mget (Minis * minis, const char **keys, size_t count,
	    MinisOneValVisitor visitor, void *ctx, uint64_t now_us);


MinisError
minis_set (Minis * minis, const char *key, const char *val, uint64_t now_us);
MinisError
minis_mset (Minis * minis, const char **key_vals, size_t count,
	    uint64_t now_us);


MinisError minis_incr (Minis * minis, const char *key, int64_t delta,
		       int64_t * out_val, uint64_t now_us);

#endif //  _T_STRING_H_
