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
#include "common.h"

void out_nil (Buffer * out);
void out_str (Buffer * out, const char *val);
void out_str_size (Buffer * out, const char *s, size_t size);
void out_int (Buffer * out, int64_t val);
void out_dbl (Buffer * out, double val);
void out_err (Buffer * out, int32_t code, const char *msg);
void out_arr (Buffer * out, uint32_t n);

// For building arrays dynamically
size_t out_arr_begin (Buffer * out);
bool out_arr_end (Buffer * out, size_t pos, uint32_t n);

#endif /* OUT_H_ */
