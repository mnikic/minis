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

#include "io/buffer.h"
#include "common/macros.h"

HOT bool out_nil (Buffer * out);
HOT bool out_str (Buffer * out, const char *val);
HOT bool out_str_size (Buffer * out, const char *string, size_t size);
HOT bool out_int (Buffer * out, int64_t val);
HOT bool out_dbl (Buffer * out, double val);
COLD bool out_err (Buffer * out, int32_t code, const char *msg);
HOT bool out_arr (Buffer * out, size_t num);

// For building arrays dynamically
HOT size_t out_arr_begin (Buffer * out);
HOT bool out_arr_end (Buffer * out, size_t pos, size_t num);
HOT bool out_ok (Buffer * out);


#endif /* OUT_H_ */
