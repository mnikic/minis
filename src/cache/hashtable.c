#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "cache/hashtable.h"
#include "common/macros.h"

static const size_t k_resizing_work = 128;
static const size_t k_init_size = 8;

static ALWAYS_INLINE size_t
next_pow2 (size_t n)
{
  if (n < k_init_size)
    return k_init_size;
  size_t pow = k_init_size;
  while (pow < n)
    pow *= 2;
  return pow;
}

static inline size_t
probe_distance (uint64_t hcode, size_t pos, size_t mask)
{
  size_t ideal_pos = hcode & mask;
  if (pos >= ideal_pos)
    return pos - ideal_pos;
  return (mask + 1) - ideal_pos + pos;
}

static bool
h_init (HTab *htab, size_t n)
{
  assert (n > 0 && ((n - 1) & n) == 0);
  htab->tab = (HTabEntry *) calloc (n, sizeof (HTabEntry));
  if (!htab->tab)
    return false;
  htab->mask = n - 1;
  htab->size = 0;
  return true;
}

bool
hm_init (HMap *hmap, size_t initial_cap)
{
  memset (hmap, 0, sizeof (*hmap));
  size_t cap = next_pow2 (initial_cap);
  return h_init (&hmap->ht1, cap);
}

static void
hm_help_resizing (HMap *hmap)
{
  if (hmap->ht2.tab == NULL)
    return;

  size_t nwork = 0;
  while (nwork < k_resizing_work && hmap->ht2.size > 0)
    {
      HTabEntry *old_entry = &hmap->ht2.tab[hmap->resizing_pos];
      HNode *node = old_entry->node;

      if (node == NULL)
	{
	  hmap->resizing_pos++;
	  if (hmap->resizing_pos > hmap->ht2.mask)
	    {
	      hmap->resizing_pos = 0;
	      if (hmap->ht2.size == 0)
		break;
	    }
	  continue;
	}

      old_entry->node = NULL;
      old_entry->hcode_cached = 0;
      hmap->ht2.size--;

      size_t n_pos = node->hcode & hmap->ht1.mask;
      size_t n_dist = 0;
      uint64_t n_hcode = node->hcode;

      for (;;)
	{
	  HTabEntry *n_entry = &hmap->ht1.tab[n_pos];
	  HNode *n_exist = n_entry->node;

	  if (n_exist == NULL)
	    {
	      n_entry->node = node;
	      n_entry->hcode_cached = n_hcode;
	      hmap->ht1.size++;
	      break;
	    }

	  size_t exist_dist =
	    probe_distance (n_entry->hcode_cached, n_pos, hmap->ht1.mask);

	  if (exist_dist < n_dist)
	    {
	      HNode *temp_node = n_entry->node;
	      uint64_t temp_hcode = n_entry->hcode_cached;

	      n_entry->node = node;
	      n_entry->hcode_cached = n_hcode;

	      node = temp_node;
	      n_hcode = temp_hcode;
	      n_dist = exist_dist;
	    }
	  n_pos = (n_pos + 1) & hmap->ht1.mask;
	  n_dist++;
	}
      nwork++;
    }

  if (hmap->ht2.size == 0)
    {
      free (hmap->ht2.tab);
      hmap->ht2 = (HTab)
      {
      0};
    }
}

static void
hm_start_resizing (HMap *hmap)
{
  assert (hmap->ht2.tab == NULL);
  hmap->ht2 = hmap->ht1;

  size_t new_cap = (hmap->ht2.mask + 1) * 2;
  if (!h_init (&hmap->ht1, new_cap))
    {
      hmap->ht1 = hmap->ht2;
      hmap->ht2 = (HTab)
      {
      0};
      return;
    }
  hmap->resizing_pos = 0;
}

static inline bool
needs_resize (const HTab *htab)
{
  size_t capacity = htab->mask + 1;
  return htab->size * 100 >= capacity * 85;
}

static HTabEntry *
h_lookup_entry (HTab *htab, const void *key, uint64_t hcode, h_cmp_fn cmp)
{
  if (!htab->tab)
    return NULL;

  size_t pos = hcode & htab->mask;
  size_t dist = 0;

  for (;;)
    {
      HTabEntry *entry = &htab->tab[pos];
      HNode *existing = entry->node;

      if (existing == NULL)
	return NULL;

      uint64_t existing_hcode = entry->hcode_cached;
      size_t existing_dist = probe_distance (existing_hcode, pos, htab->mask);
      if (dist > existing_dist)
	return NULL;

      if (existing_hcode == hcode && cmp (existing, key))
	return entry;

      pos = (pos + 1) & htab->mask;
      dist++;
    }
}

HNode *
hm_lookup (HMap *hmap, const void *key, uint64_t hcode, h_cmp_fn cmp)
{
  hm_help_resizing (hmap);

  HTabEntry *entry = h_lookup_entry (&hmap->ht1, key, hcode, cmp);
  if (entry)
    return entry->node;

  entry = h_lookup_entry (&hmap->ht2, key, hcode, cmp);
  if (entry)
    return entry->node;

  return NULL;
}

static HNode *
h_insert (HTab *htab, HNode *node, const void *key, h_cmp_fn cmp)
{
  assert (htab->tab != NULL);
  assert (node != NULL);

  size_t pos = node->hcode & htab->mask;
  size_t dist = 0;
  HNode *to_insert = node;
  uint64_t hcode_to_insert = node->hcode;

  for (;;)
    {
      HTabEntry *entry = &htab->tab[pos];
      HNode *existing = entry->node;

      if (existing == NULL)
	{
	  entry->node = to_insert;
	  entry->hcode_cached = hcode_to_insert;
	  htab->size++;
	  return NULL;
	}

      uint64_t existing_hcode = entry->hcode_cached;

      if (existing_hcode == hcode_to_insert)
	{
	  if (to_insert == node && cmp (existing, key))
	    {
	      entry->node = to_insert;
	      return existing;
	    }
	}

      size_t existing_dist = probe_distance (existing_hcode, pos, htab->mask);

      if (existing_dist < dist)
	{
	  entry->node = to_insert;
	  entry->hcode_cached = hcode_to_insert;

	  to_insert = existing;
	  hcode_to_insert = existing_hcode;
	  dist = existing_dist;
	}

      pos = (pos + 1) & htab->mask;
      dist++;
    }
}

// Helper to remove a specific slot (used by overwrite/move)
static HNode *
h_delete (HTab *htab, HTabEntry *slot)
{
  assert (slot && slot->node);

  HNode *removed_node = slot->node;
  htab->size--;

  size_t pos = (size_t) (slot - htab->tab);

  for (;;)
    {
      size_t next_pos = (pos + 1) & htab->mask;
      HTabEntry *next_entry = &htab->tab[next_pos];
      HNode *next_node = next_entry->node;

      if (next_node == NULL)
	{
	  slot->node = NULL;
	  slot->hcode_cached = 0;
	  break;
	}

      size_t dist =
	probe_distance (next_entry->hcode_cached, next_pos, htab->mask);

      if (dist == 0)
	{
	  slot->node = NULL;
	  slot->hcode_cached = 0;
	  break;
	}

      *slot = *next_entry;
      pos = next_pos;
      slot = next_entry;
    }

  return removed_node;
}

void
hm_insert (HMap *hmap, HNode *node, const void *key, h_cmp_fn cmp)
{
  if (!hmap->ht1.tab)
    h_init (&hmap->ht1, k_init_size);

  if (hmap->ht2.tab)
    {
      HTabEntry *old_entry =
	h_lookup_entry (&hmap->ht2, key, node->hcode, cmp);
      if (old_entry)
	h_delete (&hmap->ht2, old_entry);
    }

  h_insert (&hmap->ht1, node, key, cmp);

  if (!hmap->ht2.tab && needs_resize (&hmap->ht1))
    hm_start_resizing (hmap);

  hm_help_resizing (hmap);
}

HNode *
hm_pop (HMap *hmap, const void *key, uint64_t hcode, h_cmp_fn cmp)
{
  hm_help_resizing (hmap);

  HTabEntry *entry = h_lookup_entry (&hmap->ht1, key, hcode, cmp);
  if (entry)
    return h_delete (&hmap->ht1, entry);

  entry = h_lookup_entry (&hmap->ht2, key, hcode, cmp);
  if (entry)
    return h_delete (&hmap->ht2, entry);

  return NULL;
}

void
hm_destroy (HMap *hmap)
{
  if (hmap->ht1.tab)
    free (hmap->ht1.tab);
  if (hmap->ht2.tab)
    free (hmap->ht2.tab);
  memset (hmap, 0, sizeof (*hmap));
}

static void
h_scan (HTab *tab, void (*func) (HNode *, void *), void *arg)
{
  if (!tab || tab->size == 0)
    return;

  size_t capacity = tab->mask + 1;
  for (size_t i = 0; i < capacity; ++i)
    {
      HNode *node = tab->tab[i].node;
      if (node != NULL)
	func (node, arg);
    }
}

void
hm_scan (HMap *hmap, void (*func) (HNode *, void *), void *arg)
{
  if (!hmap)
    return;
  h_scan (&hmap->ht1, func, arg);
  h_scan (&hmap->ht2, func, arg);
}

void
hm_iter_init (const HMap *hmap, HMIter *iter)
{
  iter->map = hmap;
  iter->pos = 0;
  iter->table_idx = 0;
}

HNode *
hm_iter_next (HMIter *iter)
{
  const HMap *hmap = iter->map;
  if (!hmap)
    return NULL;

  while (iter->table_idx <= 1)
    {
      const HTab *tab = (iter->table_idx == 0) ? &hmap->ht1 : &hmap->ht2;

      if (tab->tab == NULL || tab->size == 0)
	{
	  iter->table_idx++;
	  iter->pos = 0;
	  continue;
	}

      while (iter->pos <= tab->mask)
	{
	  HNode *node = tab->tab[iter->pos].node;
	  iter->pos++;

	  if (node)
	    return node;
	}

      iter->table_idx++;
      iter->pos = 0;
    }

  return NULL;
}

size_t
hm_size (const HMap *hmap)
{
  return hmap->ht1.size + hmap->ht2.size;
}
