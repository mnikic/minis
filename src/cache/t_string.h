#ifndef _T_STRING_H_
#define _T_STRING_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "cache/minis.h"

// --- String Operations ---

MinisError minis_get (Minis * minis, const char *key, const char **out_val,
		      uint64_t now_us);
MinisError minis_set (Minis * minis, const char *key, const char *val,
		      uint64_t now_us);
MinisError minis_incr (Minis * minis, const char *key, int64_t delta,
		       int64_t * out_val, uint64_t now_us);

#endif //  _T_STRING_H_
