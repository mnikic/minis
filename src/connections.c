/*
 * connections.c
 *
 *  Created on: Jun 12, 2023
 *      Author: loshmi
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "connections.h"
#include "common.h"


static void grow(Conns *this, size_t new_capacity) {
    if (new_capacity <= (2 * (this->capacity))) {
        new_capacity = 2 * this->capacity;
    }
    //this->conns_all = realloc(this->conns_all,
     //       sizeof(Conn) * (new_capacity / 32) + 1);
    this->conns_all = realloc(this->conns_all, sizeof(Conn) * new_capacity);
    this->conns_by_fd = realloc(this->conns_by_fd,
            sizeof(Value*) * new_capacity);
    this->capacity = new_capacity;
}

Conns* conns_new(uint32_t capacity) {
    Conns *this = malloc(sizeof(Conns));
    if (!this) {
        return NULL;
    }
    this->capacity = capacity;
    this->presence = (uint32_t*) calloc((capacity / 32) + 1, sizeof(uint32_t));
    if (!this->presence) {
        free(this);
        return NULL;
    }
    this->conns_all = (Conn*) calloc(capacity, sizeof(Conn));
    if (!this->conns_all) {
        free(this->presence);
        free(this);
        return NULL;
    }
    this->conns_by_fd = (Value**) calloc(capacity, sizeof(Value*));
    if (!this->conns_by_fd) {
        free(this->presence);
        free(this->conns_all);
        free(this);
        return NULL;
    }
    this->size = 0;
    return this;
}

void conns_set(Conns *this, Conn *connection) {
    if (!this || !connection) {
        return;
    }
    assert(connection->fd >= 0);
    if ((size_t) connection->fd >= this->capacity) {
        grow(this, (size_t) connection->fd);
    }
    if (this->presence[connection->fd / 32] & 1 << connection->fd) {
        Value *old_value = this->conns_by_fd[connection->fd];
        old_value->value = connection;
        this->conns_all[old_value->ind_in_all] = *connection;
    } else {
        Value *value = (Value*) malloc(sizeof(Value));
        if (!value)
            die ("Out of memory conns_set");
        value->value = connection;
        value->ind_in_all =(uint32_t) this->size;

        this->conns_by_fd[connection->fd] = value;
        this->conns_all[this->size] = *connection;
        (this->size)++;
        this->presence[connection->fd / 32] |= 1 << connection->fd;
    }
}

void conns_del(Conns *this, int key) {
    assert(key >= 0);
    if (!this
            || !((size_t) key <= ((this->capacity / 32 + 1) * 32)
                    && (this->presence[key / 32] & 1 << key))) {
        return;
    }

    Value *old_value = this->conns_by_fd[key];
    if ((size_t) key != this->size - 1) {
        this->conns_all[old_value->ind_in_all] =
                this->conns_all[this->size - 1];
        this->conns_by_fd[this->conns_all[this->size - 1].fd]->ind_in_all =
                old_value->ind_in_all;
    }
    (this->size)--;
    this->presence[key / 32] &= (uint32_t) ~(1 << key);

    free(old_value);
    this->conns_by_fd[key] = NULL;
}

Conn* conns_get(Conns *this, int key) {
    if (!this || !(this->presence[key / 32] & 1 << key)) {
        return NULL;
    }
    return this->conns_by_fd[key]->value;
}

void conns_free(Conns *this) {
    if (!this || this->capacity == 0) {
        return;
    }
    for (size_t i = 0; i < this->capacity; i++) {
        free(this->conns_all + i);
    }
    free(this->conns_by_fd);
}

void conns_iter(Conns *this, Conn *array[], size_t *size) {
    if (!this) {
        return;
    }
    *size = this->size;
    *array = this->conns_all;
}
