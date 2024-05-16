/*
 * list.c
 *
 *  Created on: Jun 24, 2023
 *      Author: loshmi
 */
#include "list.h"

void dlist_init(DList *node) {
    node->prev = node->next = node;
}

int dlist_empty(DList *node) {
    return node->next == node;
}

void dlist_detach(DList *node) {
    DList *prev = node->prev;
    DList *next = node->next;
    prev->next = next;
    next->prev = prev;
}

void dlist_insert_before(DList *target, DList *rookie) {
    DList *prev = target->prev;
    prev->next = rookie;
    rookie->prev = prev;
    rookie->next = target;
    target->prev = rookie;
}
