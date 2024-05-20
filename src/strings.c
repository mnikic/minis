/*
 * out.c
 *
 *  Created on: Jun 15, 2023
 *      Author: loshmi
 */
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "strings.h"

static inline void ensureAdditionalCapacity(String *this, size_t size) {
	if (this->capacity < (size + this->i)) {
		size_t new_capacity = (this->i + size) * 2;
		char* ptr = (char*)realloc(this->data, new_capacity * sizeof(char));
		if (!ptr)
			abort();
		this->data = ptr;
		memset(&this->data[this->i], 0, new_capacity - (size_t) this->i);
		if (!this->data) {
			abort();
		}

		this->capacity = new_capacity;
	}
}

void str_appendCs(String *this, char *that) {
	if (!this || !that)
		return;
	ensureAdditionalCapacity(this, strlen(that));
	while (*that) {
		this->data[this->i++] = *that++;
	}
}

void str_appendCs_size(String *this, const char *that, uint32_t size) {
	if (!this || !that)
		return;
	ensureAdditionalCapacity(this, size);
	while (size) {
		this->data[this->i++] = *that++;
		size--;
	}
}

String* str_init(char *chars) {
	String *this = malloc(sizeof(String));
	this->i = 0;
	this->capacity = 0;
	this->data = malloc(sizeof(char));
	this->data[0] = '\0';
	if (!this->data) {
		abort();
	}

	if (chars) {
		str_appendCs(this, chars);
	}
	return this;
}

void str_appendS(String *this, String *that) {
	if (!this || !that) {
		return;
	}
	str_appendCs(this, that->data);
}

void str_appendC(String *this, char that) {
	if (!this) {
		return;
	}
	ensureAdditionalCapacity(this, 1);
	this->data[this->i++] = that;
}

void str_append_uint32(String *this, uint32_t integer) {
	if (!this) {
		return;
	}
	ensureAdditionalCapacity(this, 4);
	memcpy(&this->data[this->i], &integer, 4);
	this->i += 4;
}

void str_append_int64_t(String *this, int64_t integer) {
	if (!this) {
		return;
	}
	ensureAdditionalCapacity(this, 8);
	memcpy(&this->data[this->i], &integer, 8);
	this->i += 8;
}

void str_append_double(String *this, double dbl) {
	if (!this) {
		return;
	}
	ensureAdditionalCapacity(this, 4);
	memcpy(&this->data[this->i], &dbl, 8);
	this->i += 8;
}

void str_free(String *this) {
	if (!this)
		return;
	if (this->data)
		free(this->data);
	free(this);
}

int str_size(String *this) {
	return this->i;
}

char str_char_at(String *this, int position) {
	return this->data[position];
}

char* str_data(String *this) {
	if (!this || !this->data)
		return NULL;
	if (this->data[this->i] != '\0') {
		ensureAdditionalCapacity(this, 1);
		this->data[this->i++] = '\0';
	}
	return this->data;
}

