#ifndef _T_ZSET_H_
#define _T_ZSET_H_

#include <stdint.h>

#include "minis.h"


MinisError minis_zadd (Minis * minis, const char *key, double score,
		       const char *name, int *out_added, uint64_t now_us);
MinisError minis_zrem (Minis * minis, const char *key, const char *name,
		       int *out_removed, uint64_t now_us);
MinisError minis_zscore (Minis * minis, const char *key, const char *name,
			 double *out_score, uint64_t now_us);
MinisError minis_zquery (Minis * minis, const char *key, double score,
			 const char *name, int64_t offset, int64_t limit,
			 MinisZSetCb cb, void *ctx, uint64_t now_us);

#endif // _T_ZSET_H_
