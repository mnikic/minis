/*
 * zset.h
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#ifndef ZSET_H_
#define ZSET_H_

#include "avl.h"
#include "hashtable.h"

typedef struct {
	AVLNode *tree;
	HMap hmap;
} ZSet;

typedef struct {
	AVLNode tree;
	HNode hmap;
	double score;
	size_t len;
	char name[];
} ZNode;

int zset_add(ZSet *zset, const char *name, size_t len, double score);
ZNode* zset_lookup(ZSet *zset, const char *name, size_t len);
ZNode* zset_pop(ZSet *zset, const char *name, size_t len);
ZNode *zset_query(ZSet *zset, double score, const char *name, size_t len);
ZNode *znode_offset(ZNode *node, int64_t offset);
void zset_dispose(ZSet *zset);
void znode_del(ZNode *node);

#endif /* ZSET_H_ */
