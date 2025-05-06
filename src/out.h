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

void out_nil(String *out);

void out_str(String *out, const char *val);

void out_int(String *out, int64_t val);

void out_dbl(String *out, double val);

void out_err(String *out, int32_t code, const char *msg);

void out_arr(String *out, uint32_t n);

size_t out_bgn_arr(String *out);

void out_end_arr(String *out, size_t pos, uint32_t n); 

void out_update_arr(String *out, uint32_t n);

void out_str_size(String *out, const char *s, size_t size);

#endif /* OUT_H_ */
