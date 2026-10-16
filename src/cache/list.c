#include <stdlib.h>
#include <string.h>

#include "cache/list.h"

#define LIST_MIN_CAP 8

// Slot of element idx
static inline char **
list_slot (List *list, size_t idx)
{
  return &list->items[(list->head + idx) & (list->cap - 1)];
}

void
list_init (List *list)
{
  list->items = NULL;
  list->cap = 0;
  list->head = 0;
  list->len = 0;
}

void
list_dispose (List *list)
{
  for (size_t i = 0; i < list->len; i++)
    free (*list_slot (list, i));
  free (list->items);
  list_init (list);
}

// Moves the elements to a new array of new_cap slots, head at slot 0
static bool
list_resize (List *list, size_t new_cap)
{
  char **items = malloc (new_cap * sizeof (char *));
  if (!items)
    return false;
  for (size_t i = 0; i < list->len; i++)
    items[i] = *list_slot (list, i);
  free (list->items);
  list->items = items;
  list->cap = new_cap;
  list->head = 0;
  return true;
}

bool
list_push (List *list, const char *val, bool front)
{
  if (list->len == list->cap
      && !list_resize (list, list->cap ? list->cap * 2 : LIST_MIN_CAP))
    return false;

  char *copy = strdup (val);
  if (!copy)
    return false;

  if (front)
    {
      list->head = (list->head - 1) & (list->cap - 1);
      list->items[list->head] = copy;
    }
  else
    {
      *list_slot (list, list->len) = copy;
    }
  list->len++;
  return true;
}

void
list_drop (List *list, size_t count, bool front)
{
  for (size_t i = 0; i < count; i++)
    {
      if (front)
	{
	  free (list->items[list->head]);
	  list->head = (list->head + 1) & (list->cap - 1);
	}
      else
	{
	  free (*list_slot (list, list->len - 1));
	}
      list->len--;
    }

  // Give memory back once a queue drains, keeping it at least half
  // empty. Failing to shrink is harmless.
  size_t new_cap = list->cap;
  while (new_cap > LIST_MIN_CAP && list->len <= new_cap / 4)
    new_cap /= 2;
  if (new_cap != list->cap)
    (void) list_resize (list, new_cap);
}
