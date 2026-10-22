#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "cache/hash.h"
#include "cache/hashtable.h"
#include "common/macros.h"
#include "common/common.h"

// What hm_lookup compares fields against
typedef struct
{
  const char *field;
  size_t len;
} FieldKey;

HashEntry *
fetch_hash_entry (HNode *node)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node, HashEntry, node);
#pragma GCC diagnostic pop
}

static bool
hash_entry_eq (HNode *node, const void *key)
{
  HashEntry *hent = fetch_hash_entry (node);
  const FieldKey *fkey = (const FieldKey *) key;
  return hent->field_len == fkey->len
    && memcmp (hent->field, fkey->field, fkey->len) == 0;
}

// Values up to this long go in their entry's block
#define K_INLINE_VALUE_MAX 64

static char *
inline_value (HashEntry *ent)
{
  return ent->field + ent->field_len + 1;
}

static bool
value_is_inline (HashEntry *ent)
{
  return ent->value_cap > 0 && ent->value == inline_value (ent);
}

// A value that fits goes in the block, one that does not apart, in place
// of the one it replaces where realloc can
static bool
set_value (HashEntry *ent, const char *value, size_t value_len)
{
  char *old = (ent->value && !value_is_inline (ent)) ? ent->value : NULL;
  char *dst = inline_value (ent);
  if (value_len >= ent->value_cap)
    {
      dst = old ? realloc (old, value_len + 1) : malloc (value_len + 1);
      if (!dst)
	return false;
      old = NULL;
    }
  memcpy (dst, value, value_len);
  dst[value_len] = '\0';
  free (old);
  ent->value = dst;
  ent->value_len = value_len;
  return true;
}

HashEntry *
hash_lookup (HMap *hmap, const char *field, size_t field_len)
{
  if (!hmap)
    return NULL;
  FieldKey key = {.field = field,.len = field_len };
  uint64_t hcode = str_hash ((const uint8_t *) field, field_len);
  HNode *node = hm_lookup (hmap, &key, hcode, &hash_entry_eq);
  if (!node)
    return NULL;
  return fetch_hash_entry (node);
}

HashSetResult
hash_set (HMap *hmap, const char *field, size_t field_len, const char *value,
	  size_t value_len)
{
  HashEntry *ent = hash_lookup (hmap, field, field_len);
  if (ent)
    {
      if (ent->value_len == value_len
	  && memcmp (ent->value, value, value_len) == 0)
	return HASH_UNCHANGED;
      return set_value (ent, value, value_len) ? HASH_UPDATED : HASH_OOM;
    }

  // Room for the value and a little growth
  uint32_t value_cap = 0;
  if (value_len <= K_INLINE_VALUE_MAX)
    value_cap = (uint32_t) ((value_len + 1 + 15) & ~(size_t) 15);
  ent = malloc (offsetof (HashEntry, field) + field_len + 1 + value_cap);
  if (!ent)
    return HASH_OOM;
  memcpy (ent->field, field, field_len);
  ent->field[field_len] = '\0';
  ent->field_len = field_len;
  ent->value_cap = value_cap;
  ent->value = NULL;
  if (!set_value (ent, value, value_len))
    {
      free (ent);
      return HASH_OOM;
    }
  ent->node.hcode = str_hash ((const uint8_t *) field, field_len);
  FieldKey key = {.field = field,.len = field_len };
  hm_insert (hmap, &ent->node, &key, &hash_entry_eq);
  return HASH_ADDED;
}

static void
cb_hash_entry_destroy (HNode *node, void *arg)
{
  (void) arg;
  HashEntry *entry = fetch_hash_entry (node);
  if (!value_is_inline (entry))
    free (entry->value);
  free (entry);
}

int
hash_del (HMap *hmap, const char *field, size_t field_len)
{
  FieldKey key = {.field = field,.len = field_len };
  HNode *popped = hm_pop (hmap, &key, str_hash ((const uint8_t *) field,
						field_len), &hash_entry_eq);
  if (popped)
    {
      cb_hash_entry_destroy (popped, NULL);
      return 1;
    }
  return 0;
}

void
hash_dispose (HMap *hmap)
{
  hm_scan (hmap, &cb_hash_entry_destroy, NULL);
  hm_destroy (hmap);
}
