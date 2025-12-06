/*
 * avl.c
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#include "avl.h"
#include <stddef.h>
#include <stdlib.h>

static uint32_t
avl_depth (AVLNode *node)
{
  return node ? node->depth : 0;
}

static uint32_t
avl_cnt (AVLNode *node)
{
  return node ? node->cnt : 0;
}

static uint32_t
max (uint32_t lhs, uint32_t rhs)
{
  return lhs < rhs ? rhs : lhs;
}

// maintaining the depth and cnt field
static void
avl_update (AVLNode *node)
{
  if (!node)
    return;
  node->depth = 1 + max (avl_depth (node->left), avl_depth (node->right));
  node->cnt = 1 + avl_cnt (node->left) + avl_cnt (node->right);
}

static AVLNode *
rot_left (AVLNode *node)
{
  if (!node || !node->right)
    return node;

  AVLNode *new_node = node->right;
  if (new_node->left)
    {
      new_node->left->parent = node;
    }
  node->right = new_node->left;
  new_node->left = node;
  new_node->parent = node->parent;
  node->parent = new_node;
  avl_update (node);
  avl_update (new_node);
  return new_node;
}

static AVLNode *
rot_right (AVLNode *node)
{
  if (!node || !node->left)
    return node;

  AVLNode *new_node = node->left;
  if (new_node->right)
    {
      new_node->right->parent = node;
    }
  node->left = new_node->right;
  new_node->right = node;
  new_node->parent = node->parent;
  node->parent = new_node;
  avl_update (node);
  avl_update (new_node);
  return new_node;
}

// the left subtree is too deep
static AVLNode *
avl_fix_left (AVLNode *root)
{
  if (!root || !root->left)
    return root;

  if (avl_depth (root->left->left) < avl_depth (root->left->right))
    {
      root->left = rot_left (root->left);
    }
  return rot_right (root);
}

// the right subtree is too deep
static AVLNode *
avl_fix_right (AVLNode *root)
{
  if (!root || !root->right)
    return root;

  if (avl_depth (root->right->right) < avl_depth (root->right->left))
    {
      root->right = rot_right (root->right);
    }
  return rot_left (root);
}

// fix imbalanced nodes and maintain invariants until the root is reached
AVLNode *
avl_fix (AVLNode *node)
{
  if (!node)
    return NULL;

  while (1)
    {
      avl_update (node);
      uint32_t left = avl_depth (node->left);
      uint32_t right = avl_depth (node->right);
      AVLNode **from = NULL;

      if (node->parent)
	{
	  from = (node->parent->left == node)
	    ? &node->parent->left : &node->parent->right;
	}

      if (left == right + 2)
	{
	  node = avl_fix_left (node);
	}
      else if (left + 2 == right)
	{
	  node = avl_fix_right (node);
	}

      if (!from)
	{
	  return node;
	}
      *from = node;
      node = node->parent;
    }
}

// detach a node and returns the new root of the tree
AVLNode *
avl_del (AVLNode *node)
{
  if (!node)
    return NULL;

  AVLNode *parent = node->parent;
  AVLNode *child = NULL;

  // Node has 0 or 1 child (excluding the two-child case handled later)
  if (node->left == NULL || node->right == NULL)
    {
      // Determine the replacement child (could be NULL)
      child = node->left ? node->left : node->right;

      if (child)
	{
	  // Update child's parent pointer
	  child->parent = parent;
	}

      if (parent)
	{
	  // Link parent to the child (the replacement)
	  if (parent->left == node)
	    {
	      parent->left = child;
	    }
	  else
	    {
	      parent->right = child;
	    }
	  // Freeing 'node' is left to the caller.
	  return avl_fix (parent);
	}

      // If 'node' was the root of the entire tree
      // Freeing 'node' is left to the caller.
      return child;		// The new root (may be NULL)
    }

  // Case 2: Node has two children.
  // Replace 'node' content with its successor (leftmost node in right subtree).
  AVLNode *victim = node->right;
  while (victim->left)
    {
      victim = victim->left;
    }

  // The successor is the 'victim' (it has at most one right child).

  // Find the node that loses a child (where rebalancing must start) ---
  AVLNode *victim_parent = victim->parent;
  AVLNode *fix_start = victim_parent;	// The node where the structural change begins

  // Remove the victim from its current spot (iteratively) ---
  child = victim->right;	// Victim has no left child, only a right child (or NULL)

  if (child)
    {
      child->parent = victim_parent;
    }

  // Update the victim's parent to point to the victim's child
  if (victim_parent->left == victim)
    {
      victim_parent->left = child;
    }
  else
    {
      // This handles victim_parent->right == victim
      victim_parent->right = child;
    }

  // Replace the node being deleted with th

  // Victim takes the place of the node-to-be-deleted
  victim->left = node->left;
  victim->right = node->right;
  victim->parent = node->parent;

  // Update the children of the deleted node to point to the new replacement (victim)
  if (victim->left)
    {
      victim->left->parent = victim;
    }
  if (victim->right)
    {
      victim->right->parent = victim;
    }

  // Update the parent of the deleted node to point to the replacement (victim)
  if (parent)
    {
      if (parent->left == node)
	{
	  parent->left = victim;
	}
      else
	{
	  parent->right = victim;
	}
    }

  // Fix invariants on the replacement node and re-balance 
  if (fix_start == node)
    {
      fix_start = victim;
    }

  // IMPORTANT: Update the victim's depth and count now that it has its new children/pointers.
  avl_update (victim);

  // Freeing 'node' is left to the caller.
  return avl_fix (fix_start);
}

// offset into the succeeding or preceding node.
// note: the worst-case is O(log(n)) regardless of how long the offset is.
AVLNode *
avl_offset (AVLNode *node, int64_t offset)
{
  if (!node)
    return NULL;

  int64_t pos = 0;		// relative to the starting node
  while (offset != pos)
    {
      if (pos < offset && node->right
	  && pos + avl_cnt (node->right) >= offset)
	{
	  // the target is inside the right subtree
	  node = node->right;
	  pos += avl_cnt (node->left) + 1;
	}
      else if (pos > offset && node->left
	       && pos - avl_cnt (node->left) <= offset)
	{
	  // the target is inside the left subtree
	  node = node->left;
	  pos -= avl_cnt (node->right) + 1;
	}
      else
	{
	  // go to the parent
	  AVLNode *parent = node->parent;
	  if (!parent)
	    {
	      return NULL;
	    }
	  if (parent->right == node)
	    {
	      pos -= avl_cnt (node->left) + 1;
	    }
	  else
	    {
	      pos += avl_cnt (node->right) + 1;
	    }
	  node = parent;
	}
    }
  return node;
}

void
avl_init (AVLNode *node)
{
  if (!node)
    return;

  node->depth = 1;
  node->cnt = 1;
  node->left = node->right = node->parent = NULL;
}
