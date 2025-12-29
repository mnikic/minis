#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "cache/hash.h"
#include "cache/hashtable.h"
#include "common/macros.h"
#include "common/common.h"	// Assuming str_hash is here

// Helper to convert HNode back to HashEntry
HashEntry *
fetch_hash_entry (HNode *node)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node, HashEntry, node);
#pragma GCC diagnostic pop
}

// Equality callback for the hashtable logic
static int
hash_entry_eq (HNode *lhs, HNode *rhs)
{
  HashEntry *lhe = fetch_hash_entry (lhs);
  HashEntry *rhe = fetch_hash_entry (rhs);

  return lhs->hcode == rhs->hcode && strcmp (lhe->field, rhe->field) == 0;
}

HashEntry *
hash_lookup (HMap *hmap, const char *field)
{
  if (!hmap)
    return NULL;

  HashEntry key;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
  key.field = (char *) field;
  key.node.hcode = str_hash ((uint8_t *) field, strlen (field));
#pragma GCC diagnostic pop

  HNode *node = hm_lookup (hmap, &key.node, &hash_entry_eq);
  if (!node)
    return NULL;

  return fetch_hash_entry (node);
}

int
hash_set (HMap *hmap, const char *field, const char *value)
{
  HashEntry *ent = hash_lookup (hmap, field);

  // 1. Update existing field
  if (ent)
    {
      if (strcmp (ent->value, value) == 0)
	return 0;		// Value matches, no update needed

      free (ent->value);
      ent->value = calloc (strlen (value) + 1, sizeof (char));
      if (ent->value)
	strcpy (ent->value, value);

      return 0;			// Updated, not added
    }

  // 2. Insert new field
  ent = calloc (1, sizeof (HashEntry));
  if (!ent)
    return 0;			// OOM check

  ent->field = calloc (strlen (field) + 1, sizeof (char));
  ent->value = calloc (strlen (value) + 1, sizeof (char));

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

  ent->node.hcode = str_hash ((const uint8_t *) field, strlen (field));

  hm_insert (hmap, &ent->node, &hash_entry_eq);
  return 1;
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
  HashEntry query;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
  query.field = (char *) field;
  query.node.hcode = str_hash ((uint8_t *) field, strlen (field));
#pragma GCC diagnostic pop

  // You need to expose a helper that pops from ent->hash using hash_entry_eq
  // similar to how we did zset_pop
  HNode *popped = hm_pop (hmap, &query.node, &hash_entry_eq);

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
