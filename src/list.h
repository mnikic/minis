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

extern void dlist_init(DList *node);

extern int dlist_empty(DList *node);

extern void dlist_detach(DList *node);

extern void dlist_insert_before(DList *target, DList *rookie);


#endif /* LIST_H_ */
