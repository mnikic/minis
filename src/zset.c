/*
 * zset.c
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "zset.h"
#include "common.h"

static ZNode* znode_new(const char *name, size_t len, double score) {
	ZNode *node = (ZNode*) malloc(sizeof(ZNode) + len);
	assert(node);   // not a good idea in real projects
	memset(node, 0, sizeof(ZNode) + len);
	avl_init(&node->tree);
	node->hmap.next = NULL;
	node->hmap.hcode = str_hash((uint8_t*) name, len);
	node->score = score;
	node->len = len;
	memcpy(&node->name[0], name, len);
	return node;
}

static uint32_t min(size_t lhs, size_t rhs) {
	return (uint32_t) (lhs < rhs ? lhs : rhs);
}

// compare by the (score, name) tuple
static bool zless1(AVLNode *lhs, double score, const char *name, size_t len) {
	ZNode *zl = container_of(lhs, ZNode, tree);
	if (zl->score != score) {
		return zl->score < score;
	}
	int rv = memcmp(zl->name, name, min(zl->len, len));
	if (rv != 0) {
		return rv < 0;
	}
	return zl->len < len;
}

static bool zless(AVLNode *lhs, AVLNode *rhs) {
	ZNode *zr = container_of(rhs, ZNode, tree);
	return zless1(lhs, zr->score, zr->name, zr->len);
}

// insert into the AVL tree
static void tree_add(ZSet *zset, ZNode *node) {
	if (!zset->tree) {
		zset->tree = &node->tree;
		return;
	}

	AVLNode *cur = zset->tree;
	while (true) {
		AVLNode **from = zless(&node->tree, cur) ? &cur->left : &cur->right;
		if (!*from) {
			*from = &node->tree;
			node->tree.parent = cur;
			zset->tree = avl_fix(&node->tree);
			break;
		}
		cur = *from;
	}
}

// update the score of an existing node (AVL tree reinsertion)
static void zset_update(ZSet *zset, ZNode *node, double score) {
	if (node->score == score) {
		return;
	}
	zset->tree = avl_del(&node->tree);
	node->score = score;
	avl_init(&node->tree);
	tree_add(zset, node);
}

// add a new (score, name) tuple, or update the score of the existing tuple
int zset_add(ZSet *zset, const char *name, size_t len, double score) {
	ZNode *node = zset_lookup(zset, name, len);
	if (node) {
		zset_update(zset, node, score);
		return false;
	} else {
		node = znode_new(name, len, score);
		hm_insert(&zset->hmap, &node->hmap);
		tree_add(zset, node);
		return true;
	}
}

// a helper structure for the hashtable lookup
typedef struct {
	HNode node;
	const char *name;
	size_t len;
} HKey;

static int hcmp(HNode *node, HNode *key) {
	if (node->hcode != key->hcode) {
		return false;
	}
	ZNode *znode = container_of(node, ZNode, hmap);
	HKey *hkey = container_of(key, HKey, node);
	if (znode->len != hkey->len) {
		return false;
	}
	return 0 == memcmp(znode->name, hkey->name, znode->len);
}

// lookup by name
ZNode* zset_lookup(ZSet *zset, const char *name, size_t len) {
	if (!zset->tree) {
		return NULL;
	}

	HKey key;
	key.node.hcode = str_hash((uint8_t*) name, len);
	key.name = name;
	key.len = len;
	HNode *found = hm_lookup(&zset->hmap, &key.node, &hcmp);
	if (!found) {
		return NULL;
	}

	return container_of(found, ZNode, hmap);
}

// deletion by name
ZNode* zset_pop(ZSet *zset, const char *name, size_t len) {
	if (!zset->tree) {
		return NULL;
	}

	HKey key;
	key.node.hcode = str_hash((uint8_t*) name, len);
	key.name = name;
	key.len = len;
	HNode *found = hm_pop(&zset->hmap, &key.node, &hcmp);
	if (!found) {
		return NULL;
	}

	ZNode *node = container_of(found, ZNode, hmap);
	zset->tree = avl_del(&node->tree);
	return node;
}

// find the (score, name) tuple that is greater or equal to the argument.
ZNode *zset_query(ZSet *zset, double score, const char *name, size_t len) {
    AVLNode *found = NULL;
    AVLNode *cur = zset->tree;
    while (cur) {
        if (zless1(cur, score, name, len)) {
            cur = cur->right;
        } else {
            found = cur;    // candidate
            cur = cur->left;
        }
    }
    return found ? container_of(found, ZNode, tree) : NULL;
}

// offset into the succeeding or preceding node.
ZNode *znode_offset(ZNode *node, int64_t offset) {
    AVLNode *tnode = node ? avl_offset(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, ZNode, tree) : NULL;
}

void znode_del(ZNode *node) {
	free(node);
}

static void tree_dispose(AVLNode *node) {
	if (!node) {
		return;
	}
	tree_dispose(node->left);
	tree_dispose(node->right);
	znode_del(container_of(node, ZNode, tree));
}

// destroy the zset
void zset_dispose(ZSet *zset) {
	tree_dispose(zset->tree);
	hm_destroy(&zset->hmap);
}
