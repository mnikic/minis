/*
 * out.c
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "out.h"

static uint32_t size(char *string) {
	if (string) {
		return strlen(string);
	}
	return 0;
}

void out_nil(String *out) {
	str_appendC(out, SER_NIL);
}

void out_str(String *out, char *val) {
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

void out_err(String *out, int32_t code, char *msg) {
	str_appendC(out, SER_ERR);
	uint32_t len = size(msg);
	str_append_uint32(out, code);
	str_append_uint32(out, len);
	str_appendCs(out, msg);
}

void out_arr(String *out, uint32_t n) {
	str_appendC(out, SER_ARR);
	str_append_uint32(out, n);
}

void out_update_arr(String *out, uint32_t n) {
    str_append_uint32(out, n);
}
