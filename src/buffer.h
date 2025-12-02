// buffer.h
#ifndef BUFFER_H_
#define BUFFER_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// A dynamic byte buffer for building protocol messages
typedef struct {
    uint8_t *data;
    size_t capacity;
    size_t length;
} Buffer;

Buffer* buf_new(void);
Buffer* buf_new_with_capacity(size_t capacity);
void buf_clear(Buffer *buf);
void buf_free(Buffer *buf);

void buf_append_bytes(Buffer *buf, const void *data, size_t len);
void buf_append_cstr(Buffer *buf, const char *str);
void buf_append_byte(Buffer *buf, uint8_t byte);

void buf_append_u32_le(Buffer *buf, uint32_t value);
void buf_append_i64_le(Buffer *buf, int64_t value);
void buf_append_double_le(Buffer *buf, double value);

const uint8_t* buf_data(const Buffer *buf);
size_t buf_len(const Buffer *buf);

#endif /* BUFFER_H_ */
