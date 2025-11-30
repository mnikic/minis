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
#include "connections.h"
#include "common.h"

static void connpool_grow(ConnPool *pool, size_t new_capacity) {
    if (new_capacity <= pool->capacity * 2) {
        new_capacity = pool->capacity * 2;
    }
    
    // Grow active connections array
    Conn **new_active = realloc(pool->active, sizeof(Conn*) * new_capacity);
    if (!new_active) {
        die("Out of memory growing active connections");
    }
    pool->active = new_active;
    
    // Grow fd lookup table
    PoolEntry **new_by_fd = realloc(pool->by_fd, sizeof(PoolEntry*) * new_capacity);
    if (!new_by_fd) {
        die("Out of memory growing fd table");
    }
    pool->by_fd = new_by_fd;
    
    // Initialize new slots
    for (size_t i = pool->capacity; i < new_capacity; i++) {
        pool->by_fd[i] = NULL;
    }
    
    // Grow bitmap
    size_t old_words = (pool->capacity / 32) + 1;
    size_t new_words = (new_capacity / 32) + 1;
    uint32_t *new_bitmap = realloc(pool->fd_bitmap, sizeof(uint32_t) * new_words);
    if (!new_bitmap) {
        die("Out of memory growing fd bitmap");
    }
    pool->fd_bitmap = new_bitmap;
    
    // Zero new bitmap words
    for (size_t i = old_words; i < new_words; i++) {
        pool->fd_bitmap[i] = 0;
    }
    
    pool->capacity = new_capacity;
}

ConnPool* connpool_new(uint32_t capacity) {
    ConnPool *pool = malloc(sizeof(ConnPool));
    if (!pool) {
        return NULL;
    }
    
    pool->capacity = capacity;
    pool->active_count = 0;
    
    pool->fd_bitmap = calloc((capacity / 32) + 1, sizeof(uint32_t));
    if (!pool->fd_bitmap) {
        free(pool);
        return NULL;
    }
    
    pool->active = calloc(capacity, sizeof(Conn*));
    if (!pool->active) {
        free(pool->fd_bitmap);
        free(pool);
        return NULL;
    }
    
    pool->by_fd = calloc(capacity, sizeof(PoolEntry*));
    if (!pool->by_fd) {
        free(pool->fd_bitmap);
        free(pool->active);
        free(pool);
        return NULL;
    }
    
    return pool;
}

void connpool_add(ConnPool *pool, Conn *connection) {
    if (!pool || !connection) {
        return;
    }
    assert(connection->fd >= 0);
    
    if ((size_t)connection->fd >= pool->capacity) {
        connpool_grow(pool, (size_t)connection->fd + 1);
    }
    
    uint32_t word_idx = (uint32_t)connection->fd / 32;
    uint32_t bit_idx = (uint32_t)connection->fd % 32;
    
    if (pool->fd_bitmap[word_idx] & (1U << bit_idx)) {
        // Already exists, update it
        PoolEntry *entry = pool->by_fd[connection->fd];
        entry->conn = connection;
        pool->active[entry->index_in_active] = connection;
    } else {
        // New connection
        PoolEntry *entry = malloc(sizeof(PoolEntry));
        if (!entry) {
            die("Out of memory adding connection");
        }
        entry->conn = connection;
        entry->index_in_active = (uint32_t)pool->active_count;
        
        pool->by_fd[connection->fd] = entry;
        pool->active[pool->active_count] = connection;
        pool->active_count++;
        pool->fd_bitmap[word_idx] |= (1U << bit_idx);
    }
}

void connpool_remove(ConnPool *pool, int fd) {
    if (!pool || fd < 0 || (size_t)fd >= pool->capacity) {
        return;
    }
    
    uint32_t word_idx = (uint32_t)fd / 32;
    uint32_t bit_idx = (uint32_t)fd % 32;
    
    if (!(pool->fd_bitmap[word_idx] & (1U << bit_idx))) {
        return;
    }
    
    PoolEntry *entry = pool->by_fd[fd];
    
    // Swap with last active connection if not already last
    if (entry->index_in_active != pool->active_count - 1) {
        pool->active[entry->index_in_active] = pool->active[pool->active_count - 1];
        pool->by_fd[pool->active[pool->active_count - 1]->fd]->index_in_active = 
            entry->index_in_active;
    }
    
    pool->active_count--;
    pool->fd_bitmap[word_idx] &= ~(1U << bit_idx);
    
    free(entry);
    pool->by_fd[fd] = NULL;
}

Conn* connpool_lookup(ConnPool *pool, int fd) {
    if (!pool || fd < 0 || (size_t)fd >= pool->capacity) {
        return NULL;
    }
    
    uint32_t word_idx = (uint32_t)fd / 32;
    uint32_t bit_idx = (uint32_t)fd % 32;
    
    if (!(pool->fd_bitmap[word_idx] & (1U << bit_idx))) {
        return NULL;
    }
    
    return pool->by_fd[fd]->conn;
}

void connpool_free(ConnPool *pool) {
    if (!pool) {
        return;
    }
    
    // Free all pool entries
    for (size_t i = 0; i < pool->capacity; i++) {
        if (pool->by_fd[i]) {
            free(pool->by_fd[i]);
        }
    }
    
    free(pool->active);
    free(pool->by_fd);
    free(pool->fd_bitmap);
    free(pool);
}

void connpool_iter(ConnPool *pool, Conn ***connections, size_t *count) {
    if (!pool) {
        return;
    }
    *count = pool->active_count;
    *connections = pool->active;
}
