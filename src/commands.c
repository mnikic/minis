#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "strings.h"
#include "commands.h"
#include "heap.h"
#include "zset.h"
#include "out.h"
#include "common.h"

// the structure for the key
typedef struct entry {
	HNode node;
	char *key;
	char *val;
	uint32_t type;
	ZSet *zset;
	// for TTLs
	size_t heap_idx;
} Entry;

enum {
	T_STR = 0, T_ZSET = 1,
};

enum {
	ERR_UNKNOWN = 1, ERR_2BIG = 2, ERR_TYPE = 3, ERR_ARG = 4,
};


static int cmd_is(const char *word, const char *cmd) {
	return 0 == strcasecmp(word, cmd);
}

static int entry_eq(HNode *lhs, HNode *rhs) {
	Entry *le = container_of(lhs, Entry, node);
	Entry *re = container_of(rhs, Entry, node);
	return lhs->hcode == rhs->hcode
			&& (le != NULL && re != NULL && le->key != NULL && re->key != NULL
					&& strcmp(le->key, re->key) == 0);
}

static void h_scan(HTab *tab, void (*f)(HNode*, void*), void *arg) {
	if (tab->size == 0) {
		return;
	}
	for (size_t i = 0; i < tab->mask + 1; ++i) {
		HNode *node = tab->tab[i];
		while (node) {
			f(node, arg);
			node = node->next;
		}
	}
}

static void cb_scan(HNode *node, void *arg) {
	String *out = (String*) arg;
	out_str(out, container_of(node, Entry, node)->key);
}

static int str2dbl(const char *s, double *out) {
	char *endp = NULL;
	*out = strtod(s, &endp);
	return endp == s + strlen(s) && !isnan(*out);
}

static int str2int(const char *s, int64_t *out) {
	char *endp = NULL;
	*out = strtoll(s, &endp, 10);
	return endp == s + strlen(s);
}

// zadd zset score name
static void do_zadd(Storage* str, char **cmd, String *out) {
	double score = 0;
	if (!str2dbl(cmd[2], &score)) {
		return out_err(out, ERR_ARG, "expect fp number");
	}

	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);
	HNode *hnode = hm_lookup(&str->db, &key.node, &entry_eq);

	Entry *ent = NULL;
	if (!hnode) {
		ent = malloc(sizeof(Entry));
		if (!ent) {
			abort();
		}
		ent->key = calloc(strlen(key.key) + 1, sizeof(char));
		if (!ent->key) {
			abort();
		}
		strcpy(ent->key, key.key);
		ent->node.hcode = key.node.hcode;
		ent->type = T_ZSET;
		ent->zset = malloc(sizeof(ZSet));
		ent->heap_idx = -1;
		hm_insert(&str->db, &ent->node);
	} else {
		ent = container_of(hnode, Entry, node);
		if (ent->type != T_ZSET) {
			return out_err(out, ERR_TYPE, "expect zset");
		}
	}

// add or update the tuple
	const char *name = cmd[3];
	int added = zset_add(ent->zset, name, strlen(name), score);
	return out_int(out, (int64_t) added);
}

static int expect_zset(Storage* str, String *out, char *s, Entry **ent) {
	Entry key;
	key.key = s;
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);
	HNode *hnode = hm_lookup(&str->db, &key.node, &entry_eq);
	if (!hnode) {
		out_nil(out);
		return FALSE;
	}

	*ent = container_of(hnode, Entry, node);
	if ((*ent)->type != T_ZSET) {
		out_err(out, ERR_TYPE, "expect zset");
		return FALSE;
	}
	return TRUE;
}

// zrem zset name
static void do_zrem(Storage* str, char **cmd, String *out) {
	Entry *ent = NULL;
	if (!expect_zset(str, out, cmd[1], &ent)) {
		return;
	}

	ZNode *znode = zset_pop(ent->zset, cmd[2], strlen(cmd[2]) - 1);
	if (znode) {
		znode_del(znode);
	}
	return out_int(out, znode ? 1 : 0);
}

// zscore zset name
static void do_zscore(Storage* str, char **cmd, String *out) {
	Entry *ent = NULL;
	if (!expect_zset(str, out, cmd[1], &ent)) {
		return;
	}

	ZNode *znode = zset_lookup(ent->zset, cmd[2], strlen(cmd[2]) - 1);
	return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(Storage* str, char **cmd, String *out) {
// parse args
	double score = 0;
	if (!str2dbl(cmd[2], &score)) {
		return out_err(out, ERR_ARG, "expect fp number");
	}
	int64_t offset = 0;
	int64_t limit = 0;
	if (!str2int(cmd[4], &offset)) {
		return out_err(out, ERR_ARG, "expect int");
	}
	if (!str2int(cmd[5], &limit)) {
		return out_err(out, ERR_ARG, "expect int");
	}

// get the zset
	Entry *ent = NULL;
	if (!expect_zset(str, out, cmd[1], &ent)) {
		if (str_char_at(out, 0) == SER_NIL) {
			str_free(out);
			out = str_init(NULL);
			out_arr(out, 0);
		}
		return;
	}

// look up the tuple
	if (limit <= 0) {
		return out_arr(out, 0);
	}
	ZNode *znode = zset_query(ent->zset, score, cmd[3], strlen(cmd[3]) - 1,
			offset);

// output
	out_arr(out, 0);    // the array length will be updated later
	uint32_t n = 0;
	while (znode && (int64_t) n < limit) {
		out_str_size(out, znode->name, znode->len);
		out_dbl(out, znode->score);
		znode = container_of(avl_offset(&znode->tree, +1), ZNode, tree);
		n += 2;
	}
	return out_update_arr(out, n);
}

// set or remove the TTL
static void entry_set_ttl(Storage* str, Entry *ent, int64_t ttl_ms) {
	if (ttl_ms < 0 && ent->heap_idx != (size_t) -1) {
		(void) heap_remove_idx(&str->heap, ent->heap_idx);
		ent->heap_idx = -1;
	} else if (ttl_ms >= 0) {
		size_t pos = ent->heap_idx;
		if (pos == (size_t) -1) {
			// add an new item to the heap
			HeapItem item;
			item.ref = &ent->heap_idx;
			item.val = get_monotonic_usec() + (uint64_t) ttl_ms * 1000;
			heap_add(&str->heap, &item);
		} else {
			heap_get(&str->heap, pos)->val = get_monotonic_usec() + (uint64_t) ttl_ms * 1000; 
			heap_update(&str->heap, pos);
		}
	}
}

static void do_keys(Storage* str, char **cmd, String *out) {
	(void) cmd;
	out_arr(out, (uint32_t) hm_size(&str->db));
	h_scan(&str->db.ht1, &cb_scan, out);
	h_scan(&str->db.ht2, &cb_scan, out);
}

static void do_del(Storage* str, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_pop(&str->db, &key.node, &entry_eq);
	if (node) {
		free(container_of(node, Entry, node));
	}
	out_int(out, node ? 1 : 0);
}

static void do_set(Storage* str, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&str->db, &key.node, &entry_eq);
	if (node) {
		Entry *ent = container_of(node, Entry, node);
		ent->val = calloc(strlen(cmd[2]) + 1, sizeof(char));
		strcpy(ent->val, cmd[2]);
	} else {
		Entry *ent = malloc(sizeof(Entry));
		ent->key = calloc(strlen(cmd[1]) + 1, sizeof(char));
		strcpy(ent->key, cmd[1]);
		ent->node.hcode = key.node.hcode;
		ent->val = calloc(strlen(cmd[2]) + 1, sizeof(char));
		strcpy(ent->val, cmd[2]);
		ent->heap_idx = -1;
		ent->type = T_STR;
		hm_insert(&str->db, &ent->node);
	}
	out_nil(out);
}

// deallocate the key immediately
static void entry_destroy(Entry *ent) {
    switch (ent->type) {
    case T_ZSET:
        zset_dispose(ent->zset);
        free(ent->zset);
        break;
    }
    if (ent->key)
	free(ent->key);
    if (ent->val)
	free(ent->val);
    free(ent);
}

static void entry_del_async(void *arg) {
    entry_destroy((Entry *)arg);
}

static void entry_del(Storage* str, Entry *ent) {
	entry_set_ttl(str, ent, -1);

	const size_t k_large_container_size = 10000;
	int too_big = FALSE;
	switch (ent->type) {
	case T_ZSET:
		too_big = hm_size(&ent->zset->hmap) > k_large_container_size;
		break;
	}

	if (too_big) {
		thread_pool_queue(&str->tp, &entry_del_async, ent);
	} else {
		entry_destroy(ent);
	}
}

static void do_get(Storage* str, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&str->db, &key.node, &entry_eq);
	if (!node) {
		out_nil(out);
		return;
	}

	char *val = container_of(node, Entry, node)->val;
	int val_size = sizeof val - 1;
	assert(val_size <= K_MAX_MSG);
	out_str(out, val);
}

static void do_expire(Storage* str, char **cmd, String *out) {
	int64_t ttl_ms = 0;
	if (!str2int(cmd[2], &ttl_ms)) {
		return out_err(out, ERR_ARG, "expect int64");
	}

	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&str->db, &key.node, &entry_eq);
	if (node) {
		Entry *ent = container_of(node, Entry, node);
		entry_set_ttl(str, ent, ttl_ms);
	}
	return out_int(out, node ? 1 : 0);
}

static void do_ttl(Storage* str, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&str->db, &key.node, &entry_eq);
	if (!node) {
		return out_int(out, -2);
	}

	Entry *ent = container_of(node, Entry, node);
	if (ent->heap_idx == (size_t) -1) {
		return out_int(out, -1);
	}

	uint64_t expire_at = heap_get(&str->heap, ent->heap_idx)->val;
	uint64_t now_us = get_monotonic_usec();
	return out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
}

void commands_execute(Storage* str, char **cmd, size_t size, String *out) {
	if (size == 1 && cmd_is(cmd[0], "keys")) {
		do_keys(str, cmd, out);
	} else if (size == 2 && cmd_is(cmd[0], "get")) {
		do_get(str, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "set")) {
		do_set(str, cmd, out);
	} else if (size == 2 && cmd_is(cmd[0], "del")) {
		do_del(str, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "pexpire")) {
		do_expire(str, cmd, out);
	} else if (size == 2 && cmd_is(cmd[0], "pttl")) {
		do_ttl(str, cmd, out);
	} else if (size == 4 && cmd_is(cmd[0], "zadd")) {
		do_zadd(str, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "zrem")) {
		do_zrem(str, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "zscore")) {
		do_zscore(str, cmd, out);
	} else if (size == 6 && cmd_is(cmd[0], "zquery")) {
		do_zquery(str, cmd, out);
	} else {
		out_err(out, ERR_UNKNOWN, "Unknown cmd");
	}
}

int hnode_same(HNode *lhs, HNode *rhs) {
	return lhs == rhs;
}

Storage* commands_init(void) {
	Storage* storage = (Storage*) malloc(sizeof(Storage));
	memset(storage, 0, sizeof(Storage));
	hm_init(&storage->db);
	thread_pool_init(&storage->tp, 4);
	heap_init(&storage->heap);
	return storage;
}

void commands_evict(Storage *storage, uint64_t now_us) {
	// TTL timers
	const size_t k_max_works = 2000;
	size_t nworks = 0;
	while (!heap_empty(&storage->heap) && heap_top(&storage->heap)->val < now_us) {
		Entry *ent = container_of(heap_top(&storage->heap)->ref, Entry, heap_idx);
		HNode *node = hm_pop(&storage->db, &ent->node, &hnode_same);
		assert(node == &ent->node);
		entry_del(storage, ent);
		if (nworks++ >= k_max_works) {
			// don't stall the server if too many keys are expiring at once
			break;
		}
	}
}

uint64_t commands_next_expiry(Storage* storage) {
	// ttl timers
	if (heap_empty(&storage->heap)) {
		return -1;
	}
	return heap_top(&storage->heap)->val; 
}
