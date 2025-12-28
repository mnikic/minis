/*
 * deque.c
 *
 *  Created on: Jun 26, 2023
 *      Author: loshmi
 */
#include <stdlib.h>

#include "cache/deque.h"
#include "common/common.h"

t_deque *
dq_init (void)
{
  t_deque *deque;

  deque = malloc (sizeof (t_deque));
  if (!deque)
    die ("Out of memory dq_init.");
  deque->first = NULL;
  deque->last = NULL;
  return deque;
}

bool
dq_empty (t_deque *deque)
{
  return !deque->first || !deque->last;
}

bool
dq_push_front (t_deque *deque, void *content)
{
  t_deque_node *node = malloc (sizeof (t_deque_node));
  if (!node)
    {
      msg ("Out of memory dq_push_front");
      return false;
    }
  node->content = content;
  node->prev = NULL;
  node->next = deque->first;
  if (dq_empty (deque))
    deque->last = node;
  else
    deque->first->prev = node;
  deque->first = node;
  return true;
}

bool
dq_push_back (t_deque *deque, void *content)
{
  t_deque_node *node = malloc (sizeof (t_deque_node));
  if (!node)
    {
      msg ("Out of memory dq_push_back");
      return false;
    }
  node->content = content;
  node->prev = deque->last;
  node->next = NULL;
  if (dq_empty (deque))
    deque->first = node;
  else
    deque->last->next = node;
  deque->last = node;
  return true;
}

void *
dq_pop_front (t_deque *deque)
{
  t_deque_node *node;
  void *content;
  if (dq_empty (deque))
    return NULL;
  node = deque->first;
  deque->first = node->next;
  if (!deque->first)
    deque->last = NULL;
  else
    deque->first->prev = NULL;
  content = node->content;
  free (node);
  return content;
}

void *
dq_pop_back (t_deque *deque)
{
  t_deque_node *node;
  void *content;
  if (dq_empty (deque))
    return NULL;
  node = deque->last;
  deque->last = node->prev;
  if (!deque->last)
    deque->first = NULL;
  else
    deque->last->next = NULL;
  content = node->content;
  free (node);
  return content;
}

void *
dq_peek_front (t_deque *deque)
{
  if (dq_empty (deque))
    return NULL;
  return deque->first->content;
}

void *
dq_peek_back (t_deque *deque)
{
  if (dq_empty (deque))
    return NULL;
  return deque->last->content;
}

void
dq_dispose (t_deque *deque)
{
  if (!deque)
    return;
  t_deque_node *current = deque->first;
  t_deque_node *next;

  // Iterate through all nodes and free them
  while (current != NULL)
    {
      next = current->next;
      // NOTE: We do NOT free current->content (the actual work item),
      // as that responsibility belongs to the caller (thread pool/worker).
      free (current);
      current = next;
    }

  // Finally, free the deque structure itself
  free (deque);
}
