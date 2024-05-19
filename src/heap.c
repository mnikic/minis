/*
 * heap.c
 *
 *  Created on: Jun 25, 2023
 *      Author: loshmi
 */
#include <stddef.h>
#include <stdint.h>
#include "heap.h"
#include <stdio.h>
#include <stdlib.h>


void heap_init(Heap* heap) {
	heap->size = 0;
	heap->capacity = 10;
	heap->items = (HeapItem*) malloc(sizeof(HeapItem) * heap->capacity);
}

void heap_free(Heap* heap) {
	if (heap == NULL) return;
	free(heap->items);
}

bool heap_empty(Heap* heap) {
	return heap == NULL || heap->size == 0;
}

HeapItem* heap_top(Heap* heap) {
	return heap_get(heap, 0);
}

void heap_remove_idx(Heap* heap, size_t pos) {
	// erase an item from the heap
	// by replacing it with the last item in the array.
	heap->items[pos] = heap->items[heap->size - 1];
	heap->size--;
	if (pos < heap->size) {
		heap_update(heap, pos);
	}
}

void heap_add(Heap* heap, HeapItem* item) {
	if (heap == NULL) {
		fprintf(stderr, "Cannot add items to a NULL heap.");
		abort();
	}
	if (heap->size + 1 > heap->capacity) {
		fprintf(stderr, "Current cap is: %lu, current size is: %lu, the requested new size is %lu\n", heap->capacity, heap->size, sizeof(HeapItem)* heap -> capacity * 2);
		heap->items = realloc(heap->items, sizeof(HeapItem) * heap->capacity * 2);
		if (heap->items == NULL) {
			fprintf(stderr, "Couldn't not realocate memory to increase the capacity of the heap.");
			abort();
		}
		heap->capacity *= 2;
	}
	heap->items[heap->size++] = *item;
	heap_update(heap, heap->size - 1);
}

HeapItem* heap_get(Heap* heap, size_t pos) {
	if (heap == NULL || heap->size <= pos) return NULL;
	return &heap->items[pos];
}

static size_t heap_parent(size_t i) {
	return (i + 1) / 2 - 1;
}

static size_t heap_left(size_t i) {
	return i * 2 + 1;
}

static size_t heap_right(size_t i) {
	return i * 2 + 2;
}

static void heap_up(Heap *heap, size_t pos) {
	HeapItem t = heap->items[pos];
	while (pos > 0 && heap->items[heap_parent(pos)].val > t.val) {
		// swap with the parent
		heap->items[pos] = heap->items[heap_parent(pos)];
		*heap->items[pos].ref = pos;
		pos = heap_parent(pos);
	}
	heap->items[pos] = t;
	*heap->items[pos].ref = pos;
}

static void heap_down(Heap* heap, size_t pos) {
	HeapItem t = heap->items[pos];
	while (1) {
		// find the smallest one among the parent and their kids
		size_t l = heap_left(pos);
		size_t r = heap_right(pos);
		size_t min_pos = -1;
		size_t min_val = t.val;
		if (l < heap->size && heap->items[l].val < min_val) {
			min_pos = l;
			min_val = heap->items[l].val;
		}
		if (r < heap->size && heap->items[r].val < min_val) {
			min_pos = r;
		}
		if (min_pos == (size_t) -1) {
			break;
		}
		// swap with the kid
		heap->items[pos] = heap->items[min_pos];
		*(heap->items[pos].ref) = pos;
		pos = min_pos;
	}
	heap->items[pos] = t;
	*heap->items[pos].ref = pos;
}

void heap_update(Heap* heap, size_t pos) {
	if (pos > 0 && heap->items[heap_parent(pos)].val > heap->items[pos].val) {
		heap_up(heap, pos);
	} else {
		heap_down(heap, pos);
	}
}

