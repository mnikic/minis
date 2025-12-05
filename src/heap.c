/*
 * heap.c
 *
 *  Created on: Jun 25, 2023
 *      Author: loshmi
 */
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "heap.h"
#include "common.h"

void
heap_init (Heap *heap)
{
  if (!heap)
    return;

  heap->size = 0;
  heap->capacity = 10;
  heap->items = malloc (sizeof (HeapItem) * heap->capacity);
  if (!heap->items)
    die ("Failed to allocate memory for heap.");

}

void
heap_free (Heap *heap)
{
  if (!heap)
    return;
  if (heap->items)
    {
      free (heap->items);
      heap->items = NULL;
    }
  heap->size = 0;
  heap->capacity = 0;
}

bool
heap_empty (Heap *heap)
{
  return !heap || heap->size == 0;
}

HeapItem *
heap_top (Heap *heap)
{
  return heap_get (heap, 0);
}

HeapItem *
heap_remove_idx (Heap *heap, size_t pos)
{
  if (!heap || pos >= heap->size)
    return NULL;

  // Copy the item being removed (caller's responsibility to use immediately)
  static HeapItem removed;
  removed = heap->items[pos];

  // Replace with last item and shrink
  heap->items[pos] = heap->items[heap->size - 1];
  if (heap->items[pos].ref)
    {
      *heap->items[pos].ref = pos;
    }
  heap->size--;

  if (pos < heap->size)
    {
      heap_update (heap, pos);
    }

  return &removed;
}

void
heap_add (Heap *heap, HeapItem *item)
{
  if (!heap)
    die ("Cannot add items to a NULL heap.");

  if (!item)
    die ("Cannot add NULL item to heap.");

  if (heap->size + 1 > heap->capacity)
    {
      // Check for overflow
      if (heap->capacity > SIZE_MAX / 2)
	{
	  die ("Heap capacity overflow.");
	  abort ();
	}

      size_t new_capacity = heap->capacity * 2;
      HeapItem *new_items =
	realloc (heap->items, sizeof (HeapItem) * new_capacity);
      if (!new_items)
	{
	  die ("Could not reallocate memory to increase heap capacity.");
	}
      heap->items = new_items;
      heap->capacity = new_capacity;
    }

  heap->items[heap->size] = *item;
  if (heap->items[heap->size].ref)
    {
      *heap->items[heap->size].ref = heap->size;
    }
  heap->size++;
  heap_update (heap, heap->size - 1);
}

HeapItem *
heap_get (Heap *heap, size_t pos)
{
  if (!heap || pos >= heap->size)
    return NULL;
  return &heap->items[pos];
}

static size_t
heap_parent (size_t index)
{
  return ((index + 1) / 2) - 1;
}

static size_t
heap_left (size_t index)
{
  return (index * 2) + 1;
}

static size_t
heap_right (size_t index)
{
  return (index * 2) + 2;
}

static void
heap_up (Heap *heap, size_t pos)
{
  HeapItem item = heap->items[pos];
  while (pos > 0 && heap->items[heap_parent (pos)].val > item.val)
    {
      // swap with the parent
      heap->items[pos] = heap->items[heap_parent (pos)];
      if (heap->items[pos].ref)
	{
	  *heap->items[pos].ref = pos;
	}
      pos = heap_parent (pos);
    }
  heap->items[pos] = item;
  if (heap->items[pos].ref)
    {
      *heap->items[pos].ref = pos;
    }
}

static void
heap_down (Heap *heap, size_t pos)
{
  HeapItem item = heap->items[pos];
  while (1)
    {
      // find the smallest one among the parent and their kids
      size_t left = heap_left (pos);
      size_t right = heap_right (pos);
      size_t min_pos = (size_t) -1;
      uint64_t min_val = item.val;

      if (left < heap->size && heap->items[left].val < min_val)
	{
	  min_pos = left;
	  min_val = heap->items[left].val;
	}
      if (right < heap->size && heap->items[right].val < min_val)
	{
	  min_pos = right;
	}
      if (min_pos == (size_t) -1)
	{
	  break;
	}

      // swap with the kid
      heap->items[pos] = heap->items[min_pos];
      if (heap->items[pos].ref)
	{
	  *heap->items[pos].ref = pos;
	}
      pos = min_pos;
    }
  heap->items[pos] = item;
  if (heap->items[pos].ref)
    {
      *heap->items[pos].ref = pos;
    }
}

void
heap_update (Heap *heap, size_t pos)
{
  if (!heap || pos >= heap->size)
    return;

  if (pos > 0 && heap->items[heap_parent (pos)].val > heap->items[pos].val)
    {
      heap_up (heap, pos);
    }
  else
    {
      heap_down (heap, pos);
    }
}
