#ifndef CACHE_LIST_H_
#define CACHE_LIST_H_

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

// A list of strings in a ring buffer: O(1) push/pop at both ends and
// O(1) indexing.
typedef struct
{
  char **items;			// cap slots, NULL when cap == 0
  size_t cap;			// 0 or a power of 2
  size_t head;			// Slot of element 0
  size_t len;
} List;

void list_init (List * list);
void list_dispose (List * list);

static inline size_t
list_len (const List *list)
{
  return list->len;
}

// Element idx, 0 is the head. idx must be < len.
static inline const char *
list_at (const List *list, size_t idx)
{
  return list->items[(list->head + idx) & (list->cap - 1)];
}

// Copies val in. False on OOM.
bool list_push (List * list, const char *val, bool front);

// Removes and frees count elements from one end, count must be <= len
void list_drop (List * list, size_t count, bool front);

// Removes elements equal to val: the first count if count > 0, the last
// -count if count < 0, all if count == 0. Returns how many went.
size_t list_remove (List * list, const char *val, int64_t count);

#endif // CACHE_LIST_H_
