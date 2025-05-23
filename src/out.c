/*
 * out.c
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "out.h"
#include "strings.h"

static uint32_t size(const char *string) {
	if (string) {
		return (uint32_t) strlen(string);
	}
	return 0;
}

void out_nil(String *out) {
	str_appendC(out, SER_NIL);
}

void out_str(String *out, const char *val) {
	str_appendC(out, SER_STR);
	uint32_t len = size(val);
	str_append_uint32(out, len);
	str_appendCs(out, val);
}

void out_str_size(String *out, const char *s, size_t size) {
	str_appendC(out, SER_STR);
	uint32_t len = (uint32_t)size;
	str_append_uint32(out, len);
	str_appendCs_size(out, s, len);
}

void out_int(String *out, int64_t val) {
	str_appendC(out, SER_INT);
	str_append_int64_t(out, val);
}

void out_dbl(String *out, double val) {
	str_appendC(out, SER_DBL);
	str_append_double(out, val);
}

void out_err(String *out, int32_t code, const char *msg) {
	str_appendC(out, SER_ERR);
	uint32_t len = size(msg);
	str_append_uint32(out, (uint32_t) code);
	str_append_uint32(out, len);
	str_appendCs(out, msg);
}

void out_arr(String *out, uint32_t n) {
	str_appendC(out, SER_ARR);
	str_append_uint32(out, n);
}

size_t out_bgn_arr(String *out) {
	str_appendC(out, SER_ARR);
	// append 4 0 chars for size that will later be updated
	// and return the pointer to the beggining of them.
	size_t start_pos = out->i;
	str_append_uint32(out, (uint32_t) 0);
	return start_pos;    // the `ctx` arg
}

void out_end_arr(String *out, size_t pos, uint32_t n) {
	assert(out->data[pos - 1] == SER_ARR);
	memcpy(&out->data[pos], &n, 4);
}

void out_update_arr(String *out, uint32_t n) {
	str_append_uint32(out, n);
}
