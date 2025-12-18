/*
 * list.h
 *
 *  Created on: Jun 24, 2023
 *      Author: loshmi
 */

#ifndef LIST_H_
#define LIST_H_

#include <stddef.h>
#include <stdbool.h>

typedef struct dlist
{
  struct dlist *prev;
  struct dlist *next;
} DList;

void dlist_init (DList * node);

bool dlist_empty (DList * node);

bool dlist_is_linked (DList * node);

void dlist_detach (DList * node);

void dlist_insert_before (DList * target, DList * rookie);

#endif /* LIST_H_ */
