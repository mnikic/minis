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

// --- Map Logic (Incremental Resizing) ---

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
	  // ... (boundary check remains the same) ...
	  if (hmap->resizing_pos > hmap->ht2.mask)
	    {
	      hmap->resizing_pos = 0;
	      if (hmap->ht2.size == 0)
		break;
	    }
	  continue;
	}

      // Raw move: clear from old, insert to new
      // Clear the old slot manually (no need for h_delete overhead during linear sweep)
      old_entry->node = NULL;
      old_entry->hcode_cached = 0;
      hmap->ht2.size--;

      // Specialized raw insert for migration:
      size_t n_pos = node->hcode & hmap->ht1.mask;
      size_t n_dist = 0;
      uint64_t n_hcode = node->hcode;

      for (;;)
	{
	  HTabEntry *n_entry = &hmap->ht1.tab[n_pos];
	  HNode *n_exist = n_entry->node;

	  if (n_exist == NULL)
	    {
	      // Insert here
	      n_entry->node = node;
	      n_entry->hcode_cached = n_hcode;
	      hmap->ht1.size++;
	      break;
	    }

	  // Use cached hash for probe distance
	  size_t exist_dist =
	    probe_distance (n_entry->hcode_cached, n_pos, hmap->ht1.mask);

	  if (exist_dist < n_dist)
	    {
	      // Swap: current entry gets the new node
	      HNode *temp_node = n_entry->node;
	      uint64_t temp_hcode = n_entry->hcode_cached;

	      n_entry->node = node;
	      n_entry->hcode_cached = n_hcode;

	      // Continue inserting the displaced node
	      node = temp_node;
	      n_hcode = temp_hcode;
	      n_dist = exist_dist;
	    }
	  n_pos = (n_pos + 1) & hmap->ht1.mask;
	  n_dist++;
	}

      nwork++;
    }

  // Done resizing?
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

  // ht1 becomes ht2
  hmap->ht2 = hmap->ht1;

  // Init new ht1
  size_t new_cap = (hmap->ht2.mask + 1) * 2;
  if (!h_init (&hmap->ht1, new_cap))
    {
      // Rollback
      hmap->ht1 = hmap->ht2;
      hmap->ht2 = (HTab)
      {
      0};
      return;
    }
  hmap->resizing_pos = 0;
}

// Load factor 0.85 is safe for Robin Hood
static inline bool
needs_resize (const HTab *htab)
{
  size_t capacity = htab->mask + 1;
  return htab->size * 100 >= capacity * 85;
}

// --- Core Hash Table Logic (Robin Hood) ---

static HNode *
h_insert (HTab *htab, HNode *node, hnode_cmp_fn cmp)
{
  assert (htab->tab != NULL);
  assert (node != NULL);
  assert (cmp != NULL);

  size_t pos = node->hcode & htab->mask;
  size_t dist = 0;
  HNode *to_insert = node;
  uint64_t hcode_to_insert = node->hcode;

  for (;;)
    {
      // **CHANGE 2:** Access the HTabEntry struct
      HTabEntry *entry = &htab->tab[pos];
      HNode *existing = entry->node;

      // 1. Empty slot: Found our spot.
      if (existing == NULL)
	{
	  entry->node = to_insert;
	  entry->hcode_cached = hcode_to_insert;	// Cache hash
	  htab->size++;
	  return NULL;
	}

      // Use the cached hash for fast comparison (Cache Line A hit)
      uint64_t existing_hcode = entry->hcode_cached;

      // 2. Hash Match: Check if it's actually the same key.
      if (existing_hcode == hcode_to_insert)
	{
	  // Now we must dereference the pointer for the full cmp
	  if (cmp (existing, to_insert))
	    {
	      // Exact key match: Update (Overwrite)
	      entry->node = to_insert;
	      // entry->hcode_cached is already correct
	      return existing;
	    }
	  // Hash collision (different keys): Fall through to swap logic
	}

      // 3. Robin Hood Swap: if current item is "richer" (closer to home), swap.
      // Use the cached hash
      size_t existing_dist = probe_distance (existing_hcode, pos, htab->mask);

      if (existing_dist < dist)
	{
	  // Swap out the rich node, steal its spot
	  // Swap all fields: node pointer and cached hash
	  entry->node = to_insert;
	  entry->hcode_cached = hcode_to_insert;

	  // Continue with the displaced node
	  to_insert = existing;
	  hcode_to_insert = existing_hcode;
	  dist = existing_dist;
	}

      // 4. Continue probing
      pos = (pos + 1) & htab->mask;
      dist++;
      assert (dist <= htab->mask + 1);
    }
}

static HTabEntry *
h_lookup_entry (HTab *htab, HNode *key, hnode_cmp_fn cmp)
{
  if (!htab->tab)
    return NULL;

  size_t pos = key->hcode & htab->mask;
  size_t dist = 0;
  uint64_t key_hcode = key->hcode;

  for (;;)
    {
      // **CHANGE 3:** Access HTabEntry
      HTabEntry *entry = &htab->tab[pos];
      HNode *existing = entry->node;

      if (existing == NULL)
	{
	  return NULL;
	}

      // Use cached hash for probing (AVOID DEREFERENCE)
      uint64_t existing_hcode = entry->hcode_cached;

      // Optimization: Stop if existing element is closer to home than our probe count.
      size_t existing_dist = probe_distance (existing_hcode, pos, htab->mask);
      if (dist > existing_dist)
	{
	  return NULL;
	}

      // Full comparison (requires pointer dereference, but only if we haven't stopped)
      if (existing_hcode == key_hcode && cmp (existing, key))
	{
	  return entry;		// Return the entry pointer
	}

      pos = (pos + 1) & htab->mask;
      dist++;
    }
}

// Public-facing lookup wraps the internal lookup
HNode *
hm_lookup (HMap *hmap, HNode *key, hnode_cmp_fn cmp)
{
  hm_help_resizing (hmap);

  HTabEntry *entry = h_lookup_entry (&hmap->ht1, key, cmp);
  if (entry)
    return entry->node;

  entry = h_lookup_entry (&hmap->ht2, key, cmp);
  return entry ? entry->node : NULL;
}


// Backward Shift Deletion (No Tombstones)
static HNode *
h_delete (HTab *htab, HTabEntry *slot)
{
  assert (slot && slot->node);

  HNode *removed_node = slot->node;
  htab->size--;

  // 2. Calculate the index of the slot we just emptied
  // Use the correct array type for pointer subtraction
  size_t pos = (size_t) (slot - htab->tab);

  // 3. Backward Shift Loop
  for (;;)
    {
      size_t next_pos = (pos + 1) & htab->mask;
      HTabEntry *next_entry = &htab->tab[next_pos];
      HNode *next_node = next_entry->node;

      // Stop if next slot is empty
      if (next_node == NULL)
	{
	  slot->node = NULL;	// Clear current slot
	  slot->hcode_cached = 0;
	  break;
	}

      // Use cached hash for fast probe distance check
      size_t dist =
	probe_distance (next_entry->hcode_cached, next_pos, htab->mask);

      if (dist == 0)
	{
	  slot->node = NULL;	// Clear current slot
	  slot->hcode_cached = 0;
	  break;
	}

      // Shift next_entry back into the empty slot (move the whole struct)
      *slot = *next_entry;
      pos = next_pos;		// The hole moves to next_pos
      slot = next_entry;	// Update slot pointer to the new hole
    }

  return removed_node;
}

// --- Public API ---

void
hm_destroy (HMap *hmap)
{
  // **CHANGE 5:** Free the array of HTabEntry structs
  if (hmap->ht1.tab)
    free (hmap->ht1.tab);
  if (hmap->ht2.tab)
    free (hmap->ht2.tab);
  memset (hmap, 0, sizeof (*hmap));
}

void
hm_insert (HMap *hmap, HNode *node, hnode_cmp_fn cmp)
{
  if (!hmap->ht1.tab)
    {
      h_init (&hmap->ht1, k_init_size);
    }

  // 1. Prevent Resurrection: Check ht2 first!
  if (hmap->ht2.tab)
    {
      // **CHANGE 6:** Use the new lookup to get the entry pointer
      HTabEntry *old_entry = h_lookup_entry (&hmap->ht2, node, cmp);
      if (old_entry)
	{
	  // Pass the entry to h_delete
	  h_delete (&hmap->ht2, old_entry);
	}
    }

  // 2. Perform Insert into ht1
  h_insert (&hmap->ht1, node, cmp);

  // 3. Trigger Resize if needed
  if (!hmap->ht2.tab && needs_resize (&hmap->ht1))
    {
      hm_start_resizing (hmap);
    }

  hm_help_resizing (hmap);
}

HNode *
hm_pop (HMap *hmap, HNode *key, hnode_cmp_fn cmp)
{
  hm_help_resizing (hmap);

  // **CHANGE 7:** Use the new lookup to get the entry pointer
  HTabEntry *entry = h_lookup_entry (&hmap->ht1, key, cmp);
  if (entry)
    {
      return h_delete (&hmap->ht1, entry);
    }

  entry = h_lookup_entry (&hmap->ht2, key, cmp);
  if (entry)
    {
      return h_delete (&hmap->ht2, entry);
    }

  return NULL;
}

// Scanning functions
static void
h_scan (HTab *tab, void (*func) (HNode *, void *), void *arg)
{
  if (!tab || tab->size == 0)
    {
      return;
    }

  size_t capacity = tab->mask + 1;
  for (size_t i = 0; i < capacity; ++i)
    {
      HNode *node = tab->tab[i].node;	// Access node via HTabEntry

      if (node != NULL)
	{
	  func (node, arg);
	}
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

// cache/hashtable.c

void
hm_iter_init (const HMap *hmap, HMIter *iter)
{
  iter->map = hmap;
  iter->pos = 0;
  iter->table_idx = 0;		// Start with ht1
}

HNode *
hm_iter_next (HMIter *iter)
{
  const HMap *hmap = iter->map;
  if (!hmap)
    return NULL;

  while (iter->table_idx <= 1)
    {
      // Select current table (ht1 or ht2)
      const HTab *tab = (iter->table_idx == 0) ? &hmap->ht1 : &hmap->ht2;

      // If table is empty or unallocated, move to next table
      if (tab->tab == NULL || tab->size == 0)
	{
	  iter->table_idx++;
	  iter->pos = 0;
	  continue;
	}

      // Scan current table
      while (iter->pos <= tab->mask)
	{
	  HNode *node = tab->tab[iter->pos].node;
	  iter->pos++;		// Advance for next call

	  if (node)
	    {
	      return node;	// Found one!
	    }
	}

      // End of this table, switch to next
      iter->table_idx++;
      iter->pos = 0;
    }

  return NULL;			// Done
}

size_t
hm_size (const HMap *hmap)
{
  return hmap->ht1.size + hmap->ht2.size;
}
