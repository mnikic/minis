/*
 * string.h
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */

#ifndef STRINGS_H_
#define STRINGS_H_

#include <stdint.h>
#include <stddef.h>

typedef struct {
	char *data;
	size_t capacity;
	size_t i;
} String;

String* str_init(const char *chars);
void str_clear(String *this);
void str_appendS(String *this, String *that);
void str_appendCs(String *this, const char *that);
void str_appendC(String *this, char that);
void str_append_uint32(String *this, uint32_t integer);
void str_append_int64_t(String *this, int64_t integer);
void str_append_double(String *this, double dbl);
void str_free(String *this);
size_t str_size(String *this);
char* str_data(String *this);
char str_char_at(String *this, int position);
void str_appendCs_size(String *this, const char *that, uint32_t size);

#endif /* STRINGS_H_ */
