/*
 * avl.h
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#ifndef AVL_H_
#define AVL_H_

#include <stddef.h>
#include <stdint.h>


typedef struct avlnode {
    uint32_t depth;
    uint32_t cnt;
    struct avlnode *left;
    struct avlnode *right;
    struct avlnode *parent;
} AVLNode;

extern void avl_init(AVLNode *node);
extern AVLNode *avl_fix(AVLNode *node);
extern AVLNode *avl_del(AVLNode *node);
extern AVLNode *avl_offset(AVLNode *node, int64_t offset);


#endif /* AVL_H_ */
