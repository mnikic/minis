/*
 * common.c
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#include <stdint.h>
#include <stddef.h>
#include "common.h"


inline uint64_t str_hash(const uint8_t *data, size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}
