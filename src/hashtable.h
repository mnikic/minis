// hashtable.h
#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

// Hashtable node, should be embedded into the payload
typedef struct hnode
{
  uint64_t hcode;
  struct hnode *next;
} HNode;

// A simple fixed-sized hashtable
typedef struct
{
  HNode **tab;
  size_t mask;			// Always capacity - 1 (capacity is power of 2)
  size_t size;			// Number of entries
} HTab;

// The real hashtable interface using 2 tables for progressive resizing
typedef struct
{
  HTab ht1;			// Primary table (new table during resize)
  HTab ht2;			// Secondary table (old table being drained)
  size_t resizing_pos;		// Position in ht2 for progressive migration
} HMap;

// Callback for comparing nodes (return non-zero if equal)
typedef int (*hnode_cmp_fn) (HNode *, HNode *);

void hm_init (HMap * hmap);
HNode *hm_lookup (HMap * hmap, HNode * key, hnode_cmp_fn cmp);
void hm_insert (HMap * hmap, HNode * node);
HNode *hm_pop (HMap * hmap, HNode * key, hnode_cmp_fn cmp);
void
h_scan (HTab *tab, void (*func) (HNode *, void *), void *arg);
size_t hm_size (HMap * hmap);
void hm_destroy (HMap * hmap);

// Helper to check if currently resizing
bool hm_is_resizing (const HMap * hmap);

#endif /* HASHTABLE_H */
