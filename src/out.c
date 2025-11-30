/*
 * out.c
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */
// out.c
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "out.h"
#include "buffer.h"

void out_nil(Buffer *out) {
    buf_append_byte(out, SER_NIL);
}

void out_str(Buffer *out, const char *val) {
    if (!val) {
        out_nil(out);
        return;
    }
    
    buf_append_byte(out, SER_STR);
    uint32_t len = (uint32_t)strlen(val);
    buf_append_u32_le(out, len);
    buf_append_cstr(out, val);
}

void out_str_size(Buffer *out, const char *s, size_t size) {
    if (!s || size == 0) {
        out_nil(out);
        return;
    }
    
    if (size > UINT32_MAX) {
        // String too large for protocol
        out_err(out, ERR_UNKNOWN, "String too large");
        return;
    }
    
    buf_append_byte(out, SER_STR);
    uint32_t len = (uint32_t)size;
    buf_append_u32_le(out, len);
    buf_append_bytes(out, s, len);
}

void out_int(Buffer *out, int64_t val) {
    buf_append_byte(out, SER_INT);
    buf_append_i64_le(out, val);
}

void out_dbl(Buffer *out, double val) {
    buf_append_byte(out, SER_DBL);
    buf_append_double_le(out, val);
}

void out_err(Buffer *out, int32_t code, const char *msg) {
    if (!msg) {
        msg = "";
    }
    
    buf_append_byte(out, SER_ERR);
    uint32_t len = (uint32_t)strlen(msg);
    buf_append_u32_le(out, (uint32_t)code);
    buf_append_u32_le(out, len);
    buf_append_cstr(out, msg);
}

void out_arr(Buffer *out, uint32_t n) {
    buf_append_byte(out, SER_ARR);
    buf_append_u32_le(out, n);
}

size_t out_arr_begin(Buffer *out) {
    buf_append_byte(out, SER_ARR);
    size_t pos = buf_len(out);
    buf_append_u32_le(out, 0);  // Placeholder for count
    return pos;
}

bool out_arr_end(Buffer *out, size_t pos, uint32_t n) {
    // Validate that pos points to an array type tag
    if (pos == 0 || pos > buf_len(out)) {
        return false;  // Invalid position
    }
    
    const uint8_t *data = buf_data(out);
    if (data[pos - 1] != SER_ARR) {
        return false;  // Not an array at this position
    }
    
    // Patch in the actual count
    // Note: This directly modifies the buffer - needs const_cast
    uint8_t *writable = (uint8_t *)data;
    memcpy(&writable[pos], &n, sizeof(uint32_t));
    return true;
}
