/*
 * zset.c
 *
 * Created on: Jun 19, 2023
 * Author: loshmi
 */

#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "zset.h"
#include "common/common.h"

#define EPSILON 1e-9

// Initialize a zset
void
zset_init (ZSet *zset)
{
  if (!zset)
    return;
  zset->tree = NULL;
  hm_init (&zset->hmap);
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
  node->hmap.next = NULL;
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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  ZNode *z_node = container_of (lhs, ZNode, tree);
#pragma GCC diagnostic pop
  if (fabs (z_node->score - score) > EPSILON)	// Not equal within epsilon
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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  ZNode *zrght = container_of (rhs, ZNode, tree);
#pragma GCC diagnostic pop
  return zless1 (lhs, zrght->score, zrght->name, zrght->len);
}

// insert into the AVL tree
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

// update the score of an existing node (AVL tree reinsertion)
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

// add a new (score, name) tuple, or update the score of the existing tuple
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
      return 0;			// updated
    }

  node = znode_new (name, len, score);
  if (!node)
    {
      return -1;		// allocation failed
    }
  hm_insert (&zset->hmap, &node->hmap);
  tree_add (zset, node);
  return 1;			// added new

}

// a helper structure for the hashtable lookup
typedef struct
{
  HNode node;
  const char *name;
  size_t len;
} HKey;

static int
hcmp (HNode *node, HNode *key)
{
  if (node->hcode != key->hcode)
    {
      return false;
    }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  ZNode *znode = container_of (node, ZNode, hmap);
  HKey *hkey = container_of (key, HKey, node);
#pragma GCC diagnostic pop

  if (znode->len != hkey->len)
    {
      return false;
    }
  return 0 == memcmp (znode->name, hkey->name, znode->len);
}

// lookup by name
ZNode *
zset_lookup (ZSet *zset, const char *name, size_t len)
{
  if (!zset || !name || !zset->tree)
    {
      return NULL;
    }

  HKey key;
  key.node.hcode = str_hash ((const uint8_t *) name, len);
  key.name = name;
  key.len = len;
  HNode *found = hm_lookup (&zset->hmap, &key.node, &hcmp);
  if (!found)
    {
      return NULL;
    }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (found, ZNode, hmap);
#pragma GCC diagnostic pop
}

// deletion by name
ZNode *
zset_pop (ZSet *zset, const char *name, size_t len)
{
  if (!zset || !name || !zset->tree)
    {
      return NULL;
    }

  HKey key;
  key.node.hcode = str_hash ((const uint8_t *) name, len);
  key.name = name;
  key.len = len;
  HNode *found = hm_pop (&zset->hmap, &key.node, &hcmp);
  if (!found)
    {
      return NULL;
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  ZNode *node = container_of (found, ZNode, hmap);
#pragma GCC diagnostic pop

  zset->tree = avl_del (&node->tree);
  return node;
}

// find the (score, name) tuple that is greater or equal to the argument.
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
	  found = cur;		// candidate
	  cur = cur->left;
	}
    }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return found ? container_of (found, ZNode, tree) : NULL;
#pragma GCC diagnostic pop
}

// offset into the succeeding or preceding node.
ZNode *
znode_offset (ZNode *node, int64_t offset)
{
  AVLNode *tnode = node ? avl_offset (&node->tree, offset) : NULL;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return tnode ? container_of (tnode, ZNode, tree) : NULL;
#pragma GCC diagnostic pop
}

void
znode_del (ZNode *node)
{
  free (node);
}

// Helper to iteratively dispose of the AVL tree nodes using a stack.
// This replaces the recursive tree_dispose.
static void
tree_dispose_iterative (AVLNode *root)
{
  if (!root)
    {
      return;
    }

  size_t total_nodes = root->cnt;
  if (total_nodes == 0)
    {
      // This should not happen if root is not NULL, but serves as a safeguard.
      return;
    }

  // Allocate stack on the heap. We store AVLNode pointers.
  AVLNode **stack = (AVLNode **) malloc (total_nodes * sizeof (AVLNode *));
  if (!stack)
    {
      // Handle allocation failure if necessary (e.g., error log or exit)
      return;
    }
  size_t top = 0;		// Stack pointer

  AVLNode *cur = root;

  stack[top++] = root;		// Push root

  while (top > 0)
    {
      cur = stack[--top];	// Pop

      // Push children onto the stack before deleting the current node
      if (cur->right)
	{
	  stack[top++] = cur->right;
	}
      if (cur->left)
	{
	  stack[top++] = cur->left;
	}

      // Now delete the node
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      znode_del (container_of (cur, ZNode, tree));
#pragma GCC diagnostic pop
    }

  free (stack);
}

// destroy the zset
void
zset_dispose (ZSet *zset)
{
  if (!zset)
    {
      return;
    }

  tree_dispose_iterative (zset->tree);
  hm_destroy (&zset->hmap);
}
