/*
 * deque.h
 *
 *  Created on: Jun 26, 2023
 *      Author: loshmi
 */

#ifndef DEQUE_H_
#define DEQUE_H_

#include <stdio.h>
#include <stdlib.h>

typedef struct s_deque_node {
	void *content;
	struct s_deque_node *next;
	struct s_deque_node *prev;
} t_deque_node;

typedef struct s_deque {
	struct s_deque_node *first;
	struct s_deque_node *last;
} t_deque;

extern t_deque *dq_init(void);
extern int dq_empty(t_deque *deque);
extern void dq_push_front(t_deque *deque, void *content);
extern void dq_push_back(t_deque *deque, void *content);
extern void *dq_pop_front(t_deque *deque);
extern void *dq_pop_back(t_deque *deque);
extern void *dq_peek_front(t_deque *deque);
extern void *dq_peek_back(t_deque *deque);

#endif /* DEQUE_H_ */
