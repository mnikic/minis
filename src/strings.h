/*
 * string.h
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */

#ifndef STRINGS_H_
#define STRINGS_H_

#include <stdint.h>

typedef struct {
	char *data;
	size_t capacity;
	size_t i;
} String;

extern String* str_init(const char *chars);
extern void str_appendS(String *this, String *that);
extern void str_appendCs(String *this, const char *that);
extern void str_appendC(String *this, char that);
extern void str_append_uint32(String *this, uint32_t integer);
extern void str_append_int64_t(String *this, int64_t integer);
extern void str_append_double(String *this, double dbl);
extern void str_free(String *this);
extern int str_size(String *this);
extern char* str_data(String *this);
extern char str_char_at(String *this, int position);
extern void str_appendCs_size(String *this, const char *that, uint32_t size);

#endif /* STRINGS_H_ */
