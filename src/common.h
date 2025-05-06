/*
 * common.h
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#ifndef COMMON_H_
#define COMMON_H_

#include <stdint.h>
#include <stddef.h>

#define ERR_UNKNOWN 1
#define ERR_2BIG 2
#define ERR_TYPE 3
#define ERR_ARG 4

#define SER_NIL 0
#define SER_ERR 1    // An error code and message
#define SER_STR 2    // A string
#define SER_INT 3    // A int64
#define SER_DBL 4 	 // A double
#define SER_ARR 5	 // Ar array

#define PORT 1234

#define K_MAX_MSG 4096
#define K_MAX_ARGS 100

#define container_of(ptr, type, member) ({\
	const typeof( ((type *)0)->member ) *__mptr = (ptr);\
	(type *)( (char *)__mptr - offsetof(type,member) );})

uint64_t get_monotonic_usec(void); 

uint64_t str_hash(const uint8_t *data, size_t len);

void msg(const char *msg); 

void die(const char *msg);

#endif /* COMMON_H_ */
