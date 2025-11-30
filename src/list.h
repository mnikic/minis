/*
 * list.h
 *
 *  Created on: Jun 24, 2023
 *      Author: loshmi
 */

#ifndef LIST_H_
#define LIST_H_

#include <stddef.h>

typedef struct dlist {
	struct dlist *prev;
	struct dlist *next;
} DList;

void dlist_init(DList *node);

int dlist_empty(DList *node);

void dlist_detach(DList *node);

void dlist_insert_before(DList *target, DList *rookie);

#endif /* LIST_H_ */
