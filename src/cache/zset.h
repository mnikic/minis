/*
 * zset.h
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#ifndef ZSET_H_
#define ZSET_H_

#include "cache/avl.h"
#include "cache/hashtable.h"

typedef struct
{
  AVLNode *tree;
  HMap hmap;
} ZSet;

typedef struct
{
  AVLNode tree;
  HNode hmap;
  double score;
  size_t len;
  char name[];
} ZNode;

// Initialize a zset structure
void zset_init (ZSet * zset);

// Add or update: returns 1 if added, 0 if updated, -1 on error
int zset_add (ZSet * zset, const char *name, size_t len, double score);

// Lookup by name
ZNode *zset_lookup (ZSet * zset, const char *name, size_t len);

// Delete and return by name
ZNode *zset_pop (ZSet * zset, const char *name, size_t len);

// Query by score and name (finds >= match)
ZNode *zset_query (ZSet * zset, double score, const char *name, size_t len);

// Navigate by offset from a node
ZNode *znode_offset (ZNode * node, int64_t offset);

// Cleanup functions
void zset_dispose (ZSet * zset);
void znode_del (ZNode * node);

#endif /* ZSET_H_ */
