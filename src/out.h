/*
 * out.h
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */

#ifndef OUT_H_
#define OUT_H_

#include <stdint.h>
#include "strings.h"
#include "common.h"

extern void out_nil(String *out);

extern void out_str(String *out, const char *val);

extern void out_int(String *out, int64_t val);

extern void out_dbl(String *out, double val);

extern void out_err(String *out, int32_t code, const char *msg);

extern void out_arr(String *out, uint32_t n);

extern void out_update_arr(String *out, uint32_t n);

extern void out_str_size(String *out, const char *s, size_t size);

#endif /* OUT_H_ */
