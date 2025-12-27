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
  // next field removed - not needed for open addressing
} HNode;

// Define the actual element stored in the hash table array
typedef struct
{
  uint64_t hcode_cached;	// The cached hash code (8 bytes)
  HNode *node;			// Pointer to the actual HNode payload (8 bytes on 64-bit)
} HTabEntry;

// A simple fixed-sized hashtable
typedef struct
{
  HTabEntry *tab;		// Array of pointers to nodes (NULL = empty, TOMBSTONE = deleted)
  size_t mask;			// Always capacity - 1 (capacity is power of 2)
  size_t size;			// Number of entries (not including tombstones)
} HTab;

// The real hashtable interface using 2 tables for progressive resizing
typedef struct
{
  HTab ht1;			// Primary table (new table during resize)
  HTab ht2;			// Secondary table (old table being drained)
  size_t resizing_pos;		// Position in ht2 for progressive migration
} HMap;

typedef struct {
    const HMap *map;         // The map we are iterating
    size_t pos;        // Current index in the current table
    int table_idx;     // 0 = ht1, 1 = ht2
} HMIter;

// Initialize iterator
void hm_iter_init(const HMap *hmap, HMIter *iter);

// Get next node, or NULL if finished
HNode *hm_iter_next(HMIter *iter);

// Callback for comparing nodes (return non-zero if equal)
typedef int (*hnode_cmp_fn) (HNode *, HNode *);

void hm_init (HMap * hmap);
HNode *hm_lookup (HMap * hmap, HNode * key, hnode_cmp_fn cmp);
void hm_insert (HMap * hmap, HNode * node, hnode_cmp_fn cmp);
HNode *hm_pop (HMap * hmap, HNode * key, hnode_cmp_fn cmp);
void hm_scan (HMap * hmap, void (*func) (HNode *, void *), void *arg);
size_t hm_size (HMap * hmap);
void hm_destroy (HMap * hmap);

// Helper to check if currently resizing
bool hm_is_resizing (const HMap * hmap);

#endif /* HASHTABLE_H */
