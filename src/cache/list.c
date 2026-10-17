#include <stdlib.h>
#include <string.h>

#include "cache/list.h"

#define LIST_MIN_CAP 8

// Slot of element idx
static inline ListItem **
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
  ListItem **items = malloc (new_cap * sizeof (ListItem *));
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

static bool
item_equals (const ListItem *item, const char *val, size_t len)
{
  return item->len == len && memcmp (item->data, val, len) == 0;
}

bool
list_push (List *list, const char *val, size_t len, bool front)
{
  if (list->len == list->cap
      && !list_resize (list, list->cap ? list->cap * 2 : LIST_MIN_CAP))
    return false;

  ListItem *copy = malloc (sizeof (ListItem) + len + 1);
  if (!copy)
    return false;
  copy->len = len;
  memcpy (copy->data, val, len);
  copy->data[len] = '\0';

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

// Gives memory back once a queue drains, keeping it at least half
// empty. Failing to shrink is harmless.
static void
list_maybe_shrink (List *list)
{
  size_t new_cap = list->cap;
  while (new_cap > LIST_MIN_CAP && list->len <= new_cap / 4)
    new_cap /= 2;
  if (new_cap != list->cap)
    (void) list_resize (list, new_cap);
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
  list_maybe_shrink (list);
}

size_t
list_remove (List *list, const char *val, size_t len, int64_t count)
{
  size_t matches = 0;
  for (size_t i = 0; i < list->len; i++)
    if (item_equals (*list_slot (list, i), val, len))
      matches++;

  // -INT64_MIN does not fit in int64_t, negate unsigned
  uint64_t want = count < 0 ? 0 - (uint64_t) count : (uint64_t) count;
  size_t limit = (count == 0 || want > matches) ? matches : (size_t) want;
  // Removing from the tail means keeping the first matches
  size_t keep = count < 0 ? matches - limit : 0;

  size_t kept = 0;
  size_t seen = 0;
  for (size_t i = 0; i < list->len; i++)
    {
      ListItem *item = *list_slot (list, i);
      if (item_equals (item, val, len))
	{
	  size_t nth = seen++;
	  if (nth >= keep && nth < keep + limit)
	    {
	      free (item);
	      continue;
	    }
	}
      *list_slot (list, kept++) = item;
    }

  size_t removed = list->len - kept;
  list->len = kept;
  list_maybe_shrink (list);
  return removed;
}
