/*
 * zset.c
 *
 */

#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include "common/macros.h"
#include "common/common.h"
#include "cache/hashtable.h"
#include "cache/zset.h"
#include "cache/avl.h"

#define EPSILON 1e-9

typedef struct
{
  const char *name;
  size_t len;
} ZSetLookupKey;

void
zset_init (ZSet *zset)
{
  if (!zset)
    return;
  zset->tree = NULL;
  hm_init (&zset->hmap, 0);
}

static ZNode *
load_znode_from_hnode (HNode *node)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node, ZNode, hmap);
#pragma GCC diagnostic pop
}

static ZNode *
load_znode_from_tree (AVLNode *tree)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (tree, ZNode, tree);
#pragma GCC diagnostic pop
}

static bool
znode_eq (HNode *node, const void *key_ptr)
{
  ZNode *znode = load_znode_from_hnode (node);
  const ZSetLookupKey *lookup = (const ZSetLookupKey *) key_ptr;

  if (znode->len != lookup->len)
    {
      return false;
    }
  return memcmp (znode->name, lookup->name, znode->len) == 0;
}

static ZNode *
znode_new (const char *name, size_t len, double score)
{
  ZNode *node = malloc (sizeof (ZNode) + len);
  if (!node)
    {
      return NULL;
    }
  memset (node, 0, sizeof (ZNode) + len);
  avl_init (&node->tree);

  node->hmap.hcode = str_hash ((const uint8_t *) name, len);
  node->score = score;
  node->len = len;
  memcpy (&node->name[0], name, len);
  return node;
}

static size_t
min (size_t lhs, size_t rhs)
{
  return lhs < rhs ? lhs : rhs;
}

// compare by the (score, name) tuple
static bool
zless1 (AVLNode *lhs, double score, const char *name, size_t len)
{
  ZNode *z_node = load_znode_from_tree (lhs);
  if (fabs (z_node->score - score) > EPSILON)
    {
      return z_node->score < score;
    }
  // Scores are equal (within epsilon), compare by name
  int err = memcmp (z_node->name, name, min (z_node->len, len));
  if (err != 0)
    {
      return err < 0;
    }
  return z_node->len < len;
}

static bool
zless (AVLNode *lhs, AVLNode *rhs)
{
  ZNode *zrght = load_znode_from_tree (rhs);
  return zless1 (lhs, zrght->score, zrght->name, zrght->len);
}

static void
tree_add (ZSet *zset, ZNode *node)
{
  if (!zset->tree)
    {
      zset->tree = &node->tree;
      return;
    }

  AVLNode *cur = zset->tree;
  while (true)
    {
      AVLNode **from = zless (&node->tree, cur) ? &cur->left : &cur->right;
      if (!*from)
	{
	  *from = &node->tree;
	  node->tree.parent = cur;
	  zset->tree = avl_fix (&node->tree);
	  break;
	}
      cur = *from;
    }
}

static void
zset_update (ZSet *zset, ZNode *node, double score)
{
  if (fabs (node->score - score) < EPSILON)
    {
      return;
    }
  zset->tree = avl_del (&node->tree);
  node->score = score;
  avl_init (&node->tree);
  tree_add (zset, node);
}

// Returns: 1 = added new, 0 = updated existing, -1 = error
int
zset_add (ZSet *zset, const char *name, size_t len, double score)
{
  if (!zset || !name || len == 0)
    {
      return -1;
    }

  ZNode *node = zset_lookup (zset, name, len);
  if (node)
    {
      zset_update (zset, node, score);
      return 0;
    }

  node = znode_new (name, len, score);
  if (!node)
    {
      return -1;
    }

  ZSetLookupKey lookup_key = {.name = name,.len = len };
  hm_insert (&zset->hmap, &node->hmap, &lookup_key, &znode_eq);

  tree_add (zset, node);
  return 1;
}

ZNode *
zset_lookup (ZSet *zset, const char *name, size_t len)
{
  if (!zset || !name || !zset->tree)
    {
      return NULL;
    }

  // Use stack-allocated lookup key to avoid malloc
  ZSetLookupKey lookup_key = {.name = name,.len = len };
  uint64_t hcode = str_hash ((const uint8_t *) name, len);

  HNode *found = hm_lookup (&zset->hmap, &lookup_key, hcode, &znode_eq);
  if (!found)
    {
      return NULL;
    }
  return load_znode_from_hnode (found);
}

ZNode *
zset_pop (ZSet *zset, const char *name, size_t len)
{
  if (!zset || !name || !zset->tree)
    {
      return NULL;
    }

  ZSetLookupKey lookup_key = {.name = name,.len = len };
  uint64_t hcode = str_hash ((const uint8_t *) name, len);

  HNode *found = hm_pop (&zset->hmap, &lookup_key, hcode, &znode_eq);
  if (!found)
    {
      return NULL;
    }
  ZNode *node = load_znode_from_hnode (found);
  zset->tree = avl_del (&node->tree);
  return node;
}

ZNode *
zset_query (ZSet *zset, double score, const char *name, size_t len)
{
  if (!zset || !name)
    {
      return NULL;
    }

  AVLNode *found = NULL;
  AVLNode *cur = zset->tree;
  while (cur)
    {
      if (zless1 (cur, score, name, len))
	{
	  cur = cur->right;
	}
      else
	{
	  found = cur;
	  cur = cur->left;
	}
    }
  return found ? load_znode_from_tree (found) : NULL;
}

ZNode *
znode_offset (ZNode *node, int64_t offset)
{
  AVLNode *tnode = node ? avl_offset (&node->tree, offset) : NULL;
  return tnode ? load_znode_from_tree (tnode) : NULL;
}

void
znode_del (ZNode *node)
{
  free (node);
}

static void
tree_dispose_iterative (AVLNode *root)
{
  if (!root)
    return;

  size_t total_nodes = root->cnt;
  if (total_nodes == 0)
    return;

  AVLNode **stack = (AVLNode **) malloc (total_nodes * sizeof (AVLNode *));
  if (!stack)
    return;
  size_t top = 0;
  AVLNode *cur = root;
  stack[top++] = root;

  while (top > 0)
    {
      cur = stack[--top];

      if (cur->right)
	stack[top++] = cur->right;
      if (cur->left)
	stack[top++] = cur->left;

      znode_del (load_znode_from_tree (cur));
    }

  free (stack);
}

void
zset_dispose (ZSet *zset)
{
  if (!zset)
    return;

  tree_dispose_iterative (zset->tree);
  hm_destroy (&zset->hmap);
}
