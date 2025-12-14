/*
 * list.c
 *
 *  Created on: Jun 24, 2023
 *      Author: loshmi
 */
#include <stdbool.h>

#include "list.h"
#include "common/common.h"

void
dlist_init (DList *node)
{
  node->prev = node->next = node;
}

int
dlist_empty (DList *node)
{
  return node->next == node;
}

bool
dlist_is_linked (DList *node) 
{
  return node != NULL && node->next != node;
}

void
dlist_detach (DList *node)
{
  DList *prev = node->prev;
  DList *next = node->next;
  
  // CRITICAL GUARD: Check if the back-pointer is valid before writing
  // If the neighbor's back-pointer doesn't point to 'node', the list is corrupted 
  // and we should NOT write to the neighbor's memory.
  if (prev->next != node || next->prev != node) {
      // Log or assert the list corruption, and forcefully unlinked this node.
      // This prevents writing to freed memory but leaves the list potentially damaged.
      // Since this is a UAF, we must proceed with caution.
      // If the list is a circular list, prev->next == node must hold true.
      
       die ("List is corrupted");
      // For now, let's assume the issue is a simple Use-After-Free
  }

  // CRASH happens here if 'prev' points to freed memory (fd)
  prev->next = next;
  next->prev = prev;

  // IMPORTANT: Clear pointers to prevent future false positives
  node->prev = node;
  node->next = node;
}

void
dlist_insert_before (DList *target, DList *rookie)
{
  DList *prev = target->prev;
  prev->next = rookie;
  rookie->prev = prev;
  rookie->next = target;
  target->prev = rookie;
}
