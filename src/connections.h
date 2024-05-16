/*
 * connections.h
 *
 *  Created on: Jun 12, 2023
 *      Author: loshmi
 */

#ifndef CONNECTIONS_H_
#define CONNECTIONS_H_

#include <stdlib.h>
#include <stdint.h>
#include "list.h"

#define K_MAX_MSG 4096

typedef struct {
	int fd;
	uint32_t state;     // either STATE_REQ or STATE_RES
	// buffer for reading
	size_t rbuf_size;
	uint8_t rbuf[4 + K_MAX_MSG];
	// buffer for writing
	size_t wbuf_size;
	size_t wbuf_sent;
	uint8_t wbuf[4 + K_MAX_MSG];
	uint64_t idle_start;
	// timer
	DList idle_list;
} Conn;

typedef struct {
	Conn *value;
	uint32_t ind_in_all;
} Value;

typedef struct {
	Value **conns_by_fd;
	Conn *conns_all;
	uint32_t *presence;
	size_t capacity;
	size_t size;
} Conns;

extern Conns* conns_new(uint32_t capacity);
extern Conn* conns_get(Conns *this, int key);
extern void conns_set(Conns *this, Conn *connection);
extern void conns_iter(Conns *this, Conn *array[], size_t *size);
extern void conns_del(Conns *this, int key);
extern void conns_free(Conns *this);

#endif /* CONNECTIONS_H_ */

