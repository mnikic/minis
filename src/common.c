/*
 * common.c
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#include <stdint.h>
#include <stddef.h>
#include "common.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

inline uint64_t str_hash(const uint8_t *data, size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + (data[i] ? data[i] : 0)) * 0x01000193;
    }
    return h;
}

uint64_t get_monotonic_usec(void) {
	struct timespec tv = { 0, 0 };
	clock_gettime(CLOCK_MONOTONIC, &tv);
	return (uint64_t) (tv.tv_sec * 1000000 + tv.tv_nsec / 1000);
}

inline void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

inline void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}
