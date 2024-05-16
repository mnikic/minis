/*
 * deque.c
 *
 *  Created on: Jun 26, 2023
 *      Author: loshmi
 */
#include "deque.h"

t_deque *dq_init(void)
{
	t_deque *deque;

	deque = malloc(sizeof(t_deque));
	deque->first = NULL;
	deque->last = NULL;
	return deque;
}

int dq_empty(t_deque *deque)
{
	return !deque->first || !deque->last;
}

void dq_push_front(t_deque *deque, void *content)
{
	t_deque_node *node = malloc(sizeof(t_deque_node));
	node->content = content;
	node->prev = NULL;
	node->next = deque->first;
	if (dq_empty(deque))
		deque->last = node;
	else
		deque->first->prev = node;
	deque->first = node;
}

void dq_push_back(t_deque *deque, void *content)
{
	t_deque_node *node = malloc(sizeof(t_deque_node));
	node->content = content;
	node->prev = deque->last;
	node->next = NULL;
	if (dq_empty(deque))
		deque->first = node;
	else
		deque->last->next = node;
	deque->last = node;
}

void *dq_pop_front(t_deque *deque)
{
	t_deque_node *node;
	void *content;
	if (dq_empty(deque))
		return NULL;
	node = deque->first;
	deque->first = node->next;
	if (!deque->first)
		deque->last = NULL;
	else
		deque->first->prev = NULL;
	content = node->content;
	free(node);
	return content;
}

void *dq_pop_back(t_deque *deque)
{
	t_deque_node *node;
	void *content;
	if (dq_empty(deque))
		return NULL;
	node = deque->last;
	deque->last = node->prev;
	if (!deque->last)
		deque->first = NULL;
	else
		deque->last->next = NULL;
	content = node->content;
	free(node);
	return content;
}

void *dq_peek_front(t_deque *deque)
{
	if(dq_empty(deque))
		return NULL;
	return deque->first->content;
}

void *dq_peek_back(t_deque *deque)
{
	if(dq_empty(deque))
		return NULL;
	return deque->last->content;
}

