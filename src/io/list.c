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

bool
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
  
  if (prev->next != node || next->prev != node) {
    die ("List is corrupted");
  }

  prev->next = next;
  next->prev = prev;

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
