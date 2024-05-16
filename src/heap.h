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


typedef struct {
    uint64_t val;
    size_t *ref;
} HeapItem;

extern void heap_update(HeapItem *a, size_t pos, size_t len);

#endif /* HEAP_H_ */
