// hashtable.c
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "hashtable.h"
#include "common/common.h"

// Progressive resizing constants
static const size_t k_resizing_work = 128;
static const size_t k_max_load_factor = 8;
static const size_t k_init_size = 4;

// Initialize a hash table with n buckets (n must be power of 2)
static bool
h_init (HTab *htab, size_t n)
{
  assert (n > 0 && ((n - 1) & n) == 0);	// Must be power of 2

  htab->tab = (HNode **) calloc (n, sizeof (HNode *));
  if (!htab->tab)
    {
      return false;
    }
  htab->mask = n - 1;
  htab->size = 0;
  return true;
}

// Insert node into hash table (assumes node not already in table)
static void
h_insert (HTab *htab, HNode *node)
{
  assert (htab->tab != NULL);

  size_t pos = node->hcode & htab->mask;
  node->next = htab->tab[pos];
  htab->tab[pos] = node;
  htab->size++;
}

// Look up a node, returning pointer to the pointer that references it
// This allows deletion without re-traversal
static HNode **
h_lookup (HTab *htab, HNode *key, hnode_cmp_fn cmp)
{
  if (!htab->tab || !cmp)
    {
      return NULL;
    }

  size_t pos = key->hcode & htab->mask;
  HNode **from = &htab->tab[pos];

  while (*from)
    {
      if (cmp (*from, key))
	{
	  return from;
	}
      from = &(*from)->next;
    }
  return NULL;
}

// Remove a node from the chain
static HNode *
h_detach (HTab *htab, HNode **from)
{
  assert (from != NULL && *from != NULL);

  HNode *node = *from;
  *from = node->next;
  htab->size--;
  return node;
}

// Perform incremental resizing work
static void
hm_help_resizing (HMap *hmap)
{
  if (hmap->ht2.tab == NULL)
    {
      return;			// Not currently resizing
    }

  size_t nwork = 0;
  while (nwork < k_resizing_work && hmap->ht2.size > 0)
    {
      // Find next non-empty bucket in ht2
      HNode **from = &hmap->ht2.tab[hmap->resizing_pos];
      if (!*from)
	{
	  hmap->resizing_pos++;
	  continue;
	}

      // Move node from ht2 to ht1
      h_insert (&hmap->ht1, h_detach (&hmap->ht2, from));
      nwork++;
    }

  if (hmap->ht2.size == 0)
    {
      // Resizing complete
      free (hmap->ht2.tab);
      hmap->ht2 = (HTab)
      {
      0};
    }
}

// Start resizing by creating new table and swapping
static void
hm_start_resizing (HMap *hmap)
{
  assert (hmap->ht2.tab == NULL);	// Not already resizing

  // Swap old table to ht2
  hmap->ht2 = hmap->ht1;

  // Create new table with double capacity
  size_t new_size = (hmap->ht2.mask + 1) * 2;
  if (!h_init (&hmap->ht1, new_size))
    {
      // Allocation failed, abort resize and keep using old table
      hmap->ht1 = hmap->ht2;
      hmap->ht2 = (HTab)
      {
      0};
      return;
    }

  hmap->resizing_pos = 0;
}

void
hm_init (HMap *hmap)
{
  if (!hmap)
    return;

  memset (hmap, 0, sizeof (*hmap));
}

HNode *
hm_lookup (HMap *hmap, HNode *key, hnode_cmp_fn cmp)
{
  if (!hmap || !key || !cmp)
    {
      return NULL;
    }

  hm_help_resizing (hmap);

  // Check primary table
  HNode **from = h_lookup (&hmap->ht1, key, cmp);
  if (from)
    {
      return *from;
    }

  // Check secondary table (if resizing)
  from = h_lookup (&hmap->ht2, key, cmp);
  return from ? *from : NULL;
}

void
hm_insert (HMap *hmap, HNode *node)
{
  if (!hmap || !node)
    return;

  // Initialize table if needed
  if (!hmap->ht1.tab)
    {
      if (!h_init (&hmap->ht1, k_init_size))
	{
	  die ("Out of memory initializing hash table");
	}
    }

  h_insert (&hmap->ht1, node);

  // Check if we need to start resizing
  if (!hmap->ht2.tab)
    {
      // Fixed: Use proper comparison to avoid integer division
      size_t capacity = hmap->ht1.mask + 1;
      if (hmap->ht1.size >= k_max_load_factor * capacity)
	{
	  hm_start_resizing (hmap);
	}
    }

  hm_help_resizing (hmap);
}

HNode *
hm_pop (HMap *hmap, HNode *key, hnode_cmp_fn cmp)
{
  if (!hmap || !key || !cmp)
    {
      return NULL;
    }

  hm_help_resizing (hmap);

  // Try primary table
  HNode **from = h_lookup (&hmap->ht1, key, cmp);
  if (from)
    {
      return h_detach (&hmap->ht1, from);
    }

  // Try secondary table
  from = h_lookup (&hmap->ht2, key, cmp);
  if (from)
    {
      return h_detach (&hmap->ht2, from);
    }

  return NULL;
}

size_t
hm_size (HMap *hmap)
{
  if (!hmap)
    return 0;
  return hmap->ht1.size + hmap->ht2.size;
}

void
hm_destroy (HMap *hmap)
{
  if (!hmap)
    return;

  // Nodes must be freed by the caller since this is an intrusive design.
  if (hmap->ht1.tab)
    {
      free (hmap->ht1.tab);
      hmap->ht1.tab = NULL;
    }
  if (hmap->ht2.tab)
    {
      free (hmap->ht2.tab);
      hmap->ht2.tab = NULL;
    }
}

bool
hm_is_resizing (const HMap *hmap)
{
  return hmap && hmap->ht2.tab != NULL;
}

void
h_scan (HTab *tab, void (*func) (HNode *, void *), void *arg)
{
  if (!tab || tab->size == 0)
    {
      return;
    }
  for (size_t i = 0; i < tab->mask + 1; ++i)
    {
      HNode *node = tab->tab[i];
      while (node)
	{
	  HNode *next_node = node->next;
	  func (node, arg);
	  node = next_node;
	}
    }
}
