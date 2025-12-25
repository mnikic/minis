/*
 * heap_test.c
 *
 *  Created on: May 17, 2024
 *      Author: loshmi
 */

#include "cache/heap.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>

int
main (void)
{
  Heap heap;
  heap_init (&heap);
  assert (heap_empty (&heap));
  assert (heap.size == 0);
  assert (heap.capacity == 10);
  HeapItem item1 = { 0 };
  size_t a = 26;
  item1.val = 2;
  item1.ref = &a;
  heap_add (&heap, &item1);

  assert (!heap_empty (&heap));
  assert (heap.size == 1);
  assert (heap.capacity == 10);
  assert (heap_top (&heap)->val == 2);
  size_t b = 124;
  HeapItem item2;
  item2.val = 1;
  item2.ref = &b;
  heap_add (&heap, &item2);
  assert (!heap_empty (&heap));
  assert (heap_top (&heap)->val == 1);
  assert (heap.size == 2);
  assert (heap.capacity == 10);
  HeapItem items[20];
  for (uint32_t i = 0; i < 20; i++)
    {
      items[i].val = i + 3;
      items[i].ref = &a;
      heap_add (&heap, &items[i]);
    }
  assert (!heap_empty (&heap));
  assert (heap_top (&heap)->val == 1);
  assert (heap.size == 22);
  assert (heap.capacity == 40);
  size_t size = heap.size;
  for (size_t i = 0; i < size; i++)
    {
      assert (heap_top (&heap)->val == i + 1);
      heap_remove_idx (&heap, 0);
    }
  assert (heap.size == 0);
  assert (heap_empty (&heap));
  heap_free(&heap);
  printf ("Success!\n");
}
