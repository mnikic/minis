/*
 * out.h
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */
#ifndef OUT_H_
#define OUT_H_

#include <stdint.h>
#include <stdbool.h>
#include "buffer.h"

bool out_nil (Buffer * out);
bool out_str (Buffer * out, const char *val);
bool out_str_size (Buffer * out, const char *string, size_t size);
bool out_int (Buffer * out, int64_t val);
bool out_dbl (Buffer * out, double val);
bool out_err (Buffer * out, int32_t code, const char *msg);
bool out_arr (Buffer * out, size_t num);

// For building arrays dynamically
size_t out_arr_begin (Buffer * out);
bool out_arr_end (Buffer * out, size_t pos, size_t num);

#endif /* OUT_H_ */
