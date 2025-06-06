/*
 * heap.h
 *
 *  Created on: Jun 25, 2023
 *      Author: loshmi
 */

#ifndef HEAP_H_
#define HEAP_H_

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef struct {
    uint64_t val;
    size_t *ref;
} HeapItem;

typedef struct {
    HeapItem  *items;
    size_t size;
    size_t capacity;
} Heap;

void heap_init(Heap* heap);
void heap_free(Heap* heap);
bool heap_empty(Heap* heap);
HeapItem* heap_top(Heap* heap);
HeapItem* heap_remove_idx(Heap* heap, size_t pos);
void heap_update(Heap* heap, size_t pos);
void heap_add(Heap* heap, HeapItem* item);
HeapItem* heap_get(Heap* heap, size_t pos);

#endif /* HEAP_H_ */
