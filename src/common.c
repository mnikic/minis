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
#include <string.h>

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

__attribute__((noreturn))
inline void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

/**
 * @brief Prints usage information.
 */
static void usage(const char *prog_name) {
    fprintf(stderr, "Usage: %s [-p <port>]\n", prog_name);
    fprintf(stderr, "  -p <port>  Specify the TCP port to listen on (default: %d)\n", DEFAULT_PORT);
}

uint16_t parse_port (int argc, char* argv[]) {
    int port = DEFAULT_PORT;

    // Simple argument parsing for the port number
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-p") == 0) {
            if (i + 1 < argc) {
                port = (int)strtol(argv[++i], NULL, 10);
                if (port <= 0 || port > 65535) {
                    usage(argv[0]);
                    die("Error: Invalid port number.");
                }
            } else {
                usage(argv[0]);
                die("Error: -p requires a port number argument.");
            }
            goto DONE;
        } 
    }
DONE:
    return (uint16_t) port;
}

