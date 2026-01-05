#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "cache/hash.h"
#include "cache/hashtable.h"
#include "common/macros.h"
#include "common/common.h"

// Helper to convert HNode back to HashEntry
HashEntry *
fetch_hash_entry (HNode *node)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node, HashEntry, node);
#pragma GCC diagnostic pop
}

// Comparator: HNode vs Raw Key String
bool
hash_entry_eq_str (HNode *node, const void *key)
{
  HashEntry *hent = fetch_hash_entry (node);
  return strcmp (hent->field, (const char *) key) == 0;
}

HashEntry *
hash_lookup (HMap *hmap, const char *field)
{
  if (!hmap)
    return NULL;

  uint64_t hcode = cstr_hash (field);
  // Correctly uses new signature: key + hcode passed separately
  HNode *node = hm_lookup (hmap, field, hcode, &hash_entry_eq_str);

  if (!node)
    return NULL;

  return fetch_hash_entry (node);
}

int
hash_set (HMap *hmap, const char *field, const char *value)
{
  HashEntry *ent = hash_lookup (hmap, field);

  // 1. UPDATE EXISTING
  if (ent)
    {
      if (strcmp (ent->value, value) == 0)
	return 0;

      free (ent->value);
      // Optimization: malloc is faster than calloc when we overwrite immediately
      ent->value = malloc (strlen (value) + 1);
      if (ent->value)
	strcpy (ent->value, value);

      return 0;			// 0 indicates update
    }

  // 2. INSERT NEW
  ent = malloc (sizeof (HashEntry));
  if (!ent)
    return 0;

  ent->field = malloc (strlen (field) + 1);
  ent->value = malloc (strlen (value) + 1);

  if (!ent->field || !ent->value)
    {
      if (ent->field)
	free (ent->field);
      if (ent->value)
	free (ent->value);
      free (ent);
      return 0;
    }

  strcpy (ent->field, field);
  strcpy (ent->value, value);

  ent->node.hcode = cstr_hash (field);

  // Correctly passes 'field' as the lookup key for duplicate checking
  hm_insert (hmap, &ent->node, field, &hash_entry_eq_str);
  return 1;			// 1 indicates new insertion
}

// Helper callback for destroying nodes
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
hash_del (HMap *hmap, const char *field)
{
  // Correctly uses new signature
  HNode *popped = hm_pop (hmap, field, cstr_hash (field), &hash_entry_eq_str);
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
