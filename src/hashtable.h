#include <stddef.h>
#include <stdint.h>


// hashtable node, should be embedded into the payload
typedef struct hnode {
    uint64_t hcode;
    struct hnode *next;
} HNode;

// a simple fixed-sized hashtable
typedef struct {
    HNode **tab;
    size_t mask;
    size_t size;
} HTab;

// the real hashtable interface.
// it uses 2 hashtables for progressive resizing.
typedef struct {
    HTab ht1;
    HTab ht2;
    size_t resizing_pos;
} HMap;

extern void hm_init(HMap *hmap);
extern HNode *hm_lookup(HMap *hmap, HNode *key, int (*cmp)(HNode *, HNode *));
extern void hm_insert(HMap *hmap, HNode *node);
extern HNode *hm_pop(HMap *hmap, HNode *key, int (*cmp)(HNode *, HNode *));
extern size_t hm_size(HMap *hmap);
extern void hm_destroy(HMap *hmap);
