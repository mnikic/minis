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

// A NUL terminated copy of len bytes
static char *
copy_bytes (const char *bytes, size_t len)
{
  char *copy = malloc (len + 1);
  if (copy)
    {
      memcpy (copy, bytes, len);
      copy[len] = '\0';
    }
  return copy;
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
      char *copy = copy_bytes (value, value_len);
      if (!copy)
	return HASH_OOM;
      free (ent->value);
      ent->value = copy;
      ent->value_len = value_len;
      return HASH_UPDATED;
    }

  ent = malloc (sizeof (HashEntry));
  if (!ent)
    return HASH_OOM;
  ent->field = copy_bytes (field, field_len);
  ent->value = copy_bytes (value, value_len);
  if (!ent->field || !ent->value)
    {
      free (ent->field);
      free (ent->value);
      free (ent);
      return HASH_OOM;
    }
  ent->field_len = field_len;
  ent->value_len = value_len;
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
  free (entry->field);
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
