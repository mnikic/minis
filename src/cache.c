#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "strings.h"
#include "heap.h"
#include "cache.h"
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
static void do_zadd(Cache *cache, char **cmd, String *out) {
	double score = 0;
	if (!str2dbl(cmd[2], &score)) {
		out_err(out, ERR_ARG, "expect fp number");
		return;
	}

	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, strlen(key.key));
	HNode *hnode = hm_lookup(&cache->db, &key.node, &entry_eq);

	Entry *ent = NULL;
	if (!hnode) {
		ent = malloc(sizeof(Entry));
		memset(ent, 0, sizeof(Entry));
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
		if (!ent->zset)
			die("Couldnt allocate zset");
		memset(ent->zset, 0, sizeof(ZSet));
		ent->heap_idx = -1;
		hm_insert(&cache->db, &ent->node);
	} else {
		ent = container_of(hnode, Entry, node);
		if (ent->type != T_ZSET) {
			out_err(out, ERR_TYPE, "expect zset");
		}
	}

// add or update the tuple
	const char *name = cmd[3];
	int added = zset_add(ent->zset, name, strlen(name), score);
	out_int(out, (int64_t) added);
}

static int expect_zset(Cache *cache, String *out, char *s, Entry **ent) {
	Entry key;
	key.key = s;
	key.node.hcode = str_hash((uint8_t*) key.key, strlen(key.key));
	HNode *hnode = hm_lookup(&cache->db, &key.node, &entry_eq);
	if (!hnode) {
		out_nil(out);
		return false;
	}

	*ent = container_of(hnode, Entry, node);
	if ((*ent)->type != T_ZSET) {
		out_err(out, ERR_TYPE, "expect zset");
		return false;
	}
	return true;
}

// zrem zset name
static void do_zrem(Cache *cache, char **cmd, String *out) {
	Entry *ent = NULL;
	if (!expect_zset(cache, out, cmd[1], &ent)) {
		return;
	}

	ZNode *znode = zset_pop(ent->zset, cmd[2], strlen(cmd[2]));
	if (znode) {
		znode_del(znode);
	}
	out_int(out, znode ? 1 : 0);
}

// zscore zset name
static void do_zscore(Cache *cache, char **cmd, String *out) {
	Entry *ent = NULL;
	if (!expect_zset(cache, out, cmd[1], &ent)) {
		return;
	}

	ZNode *znode = zset_lookup(ent->zset, cmd[2], strlen(cmd[2]));
	if (znode) {
		out_dbl(out, znode->score);
	} else {
		out_nil(out);
	}
}

// zquery zset score name offset limit
static void do_zquery(Cache *cache, char **cmd, String *out) {
// parse args
	double score = 0;
	if (!str2dbl(cmd[2], &score)) {
		out_err(out, ERR_ARG, "expect fp number");
		return;
	}
	int64_t offset = 0;
	int64_t limit = 0;
	if (!str2int(cmd[4], &offset)) {
		out_err(out, ERR_ARG, "expect int");
		return;
	}
	if (!str2int(cmd[5], &limit)) {
		out_err(out, ERR_ARG, "expect int");
		return;
	}

// get the zset
	Entry *ent = NULL;
	if (!expect_zset(cache, out, cmd[1], &ent)) {
		if (str_char_at(out, 0) == SER_NIL) {
			str_clear(out);
			out_arr(out, (uint32_t) 0);
		}
		return;
	}

// look up the tuple
	if (limit <= 0) {
		out_arr(out, (uint32_t) 0);
		return;
	}
	ZNode *znode = zset_query(ent->zset, score, cmd[3], strlen(cmd[3]));
	znode = znode_offset(znode, offset);

	// output
	size_t idx = out_bgn_arr(out);
	uint32_t n = 0;
	while (znode && (int64_t) n < limit) {
		out_str_size(out, znode->name, znode->len);
		out_dbl(out, znode->score);
		znode = znode_offset(znode, +1);
		n += 2;
	}
	out_end_arr(out,idx, n);
}

// set or remove the TTL
static void entry_set_ttl(Cache *cache, Entry *ent, int64_t ttl_ms) {
	if (ttl_ms < 0 && ent->heap_idx != (size_t) -1) {
		(void) heap_remove_idx(&cache->heap, ent->heap_idx);
		ent->heap_idx = -1;
	} else if (ttl_ms >= 0) {
		size_t pos = ent->heap_idx;
		if (pos == (size_t) -1) {
			// add an new item to the heap
			HeapItem item;
			item.ref = &ent->heap_idx;
			item.val = get_monotonic_usec() + (uint64_t) ttl_ms * 1000;
			heap_add(&cache->heap, &item);
		} else {
			heap_get(&cache->heap, pos)->val = get_monotonic_usec() + (uint64_t) ttl_ms * 1000; 
			heap_update(&cache->heap, pos);
		}
	}
}

static void do_keys(Cache *cache, char **cmd, String *out) {
	(void) cmd;
	out_arr(out, (uint32_t) hm_size(&cache->db));
	h_scan(&cache->db.ht1, &cb_scan, out);
	h_scan(&cache->db.ht2, &cb_scan, out);
}

static void do_del(Cache *cache, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, strlen(key.key));

	HNode *node = hm_pop(&cache->db, &key.node, &entry_eq);
	if (node) {
		Entry *entry = container_of(node, Entry, node);
		if (entry->heap_idx != (size_t) -1) {
			heap_remove_idx(&cache->heap, entry->heap_idx);
		}
		entry_destroy(entry);
	}
	out_int(out, node ? 1 : 0);
}

static void do_set(Cache *cache, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, strlen(key.key));

	HNode *node = hm_lookup(&cache->db, &key.node, &entry_eq);
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
		hm_insert(&cache->db, &ent->node);
	}
	out_nil(out);
}

static void entry_del_async(void *arg) {
    entry_destroy((Entry *)arg);
}

static void entry_del(Cache *cache, Entry *ent) {
	entry_set_ttl(cache, ent, -1);

	const size_t k_large_container_size = 10000;
	bool too_big = false;
	switch (ent->type) {
	case T_ZSET:
		too_big = hm_size(&ent->zset->hmap) > k_large_container_size;
		break;
	}

	if (too_big) {
		thread_pool_queue(&cache->tp, &entry_del_async, ent);
	} else {
		entry_destroy(ent);
	}
}

static void do_get(Cache *cache, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, strlen(key.key));

	HNode *node = hm_lookup(&cache->db, &key.node, &entry_eq);
	if (!node) {
		out_nil(out);
		return;
	}

	char *val = container_of(node, Entry, node)->val;
	int val_size = sizeof val - 1;
	assert(val_size <= K_MAX_MSG);
	out_str(out, val);
}

static void do_expire(Cache *cache, char **cmd, String *out) {
	int64_t ttl_ms = 0;
	if (!str2int(cmd[2], &ttl_ms)) {
		out_err(out, ERR_ARG, "expect int64");
		return;
	}

	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, strlen(key.key));

	HNode *node = hm_lookup(&cache->db, &key.node, &entry_eq);
	if (node) {
		Entry *ent = container_of(node, Entry, node);
		entry_set_ttl(cache, ent, ttl_ms);
	}
	out_int(out, node ? 1 : 0);
}

static void do_ttl(Cache *cache, char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, strlen(key.key));

	HNode *node = hm_lookup(&cache->db, &key.node, &entry_eq);
	if (!node) {
		out_int(out, -2);
		return;
	}

	Entry *ent = container_of(node, Entry, node);
	if (ent->heap_idx == (size_t) -1) {
		out_int(out, -1);
		return;
	}

	uint64_t expire_at = heap_get(&cache->heap, ent->heap_idx)->val;
	uint64_t now_us = get_monotonic_usec();
	out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
}

void cache_execute(Cache *cache, char **cmd, size_t size, String *out) {
	if (size == 1 && cmd_is(cmd[0], "keys")) {
		do_keys(cache, cmd, out);
	} else if (size == 2 && cmd_is(cmd[0], "get")) {
		do_get(cache, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "set")) {
		do_set(cache, cmd, out);
	} else if (size == 2 && cmd_is(cmd[0], "del")) {
		do_del(cache, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "pexpire")) {
		do_expire(cache, cmd, out);
	} else if (size == 2 && cmd_is(cmd[0], "pttl")) {
		do_ttl(cache, cmd, out);
	} else if (size == 4 && cmd_is(cmd[0], "zadd")) {
		do_zadd(cache, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "zrem")) {
		do_zrem(cache, cmd, out);
	} else if (size == 3 && cmd_is(cmd[0], "zscore")) {
		do_zscore(cache, cmd, out);
	} else if (size == 6 && cmd_is(cmd[0], "zquery")) {
		do_zquery(cache, cmd, out);
	} else {
		out_err(out, ERR_UNKNOWN, "Unknown cmd");
	}
}

int hnode_same(HNode *lhs, HNode *rhs) {
	return lhs == rhs;
}

Cache* cache_init(void) {
	Cache* cache = (Cache*) malloc(sizeof(Cache));
	memset(cache, 0, sizeof(Cache));
	hm_init(&cache->db);
	thread_pool_init(&cache->tp, 4);
	heap_init(&cache->heap);
	return cache;
}

void cache_evict(Cache *cache, uint64_t now_us) {
	// TTL timers
	const size_t k_max_works = 2000;
	size_t nworks = 0;
	while (!heap_empty(&cache->heap) && heap_top(&cache->heap)->val < now_us) {
		Entry *ent = container_of(heap_top(&cache->heap)->ref, Entry, heap_idx);
		HNode *node = hm_pop(&cache->db, &ent->node, &hnode_same);
		assert(node == &ent->node);
		entry_del(cache, ent);
		if (nworks++ >= k_max_works) {
			// don't stall the server if too many keys are expiring at once
			break;
		}
	}
}

uint64_t cache_next_expiry(Cache* cache) {
	// ttl timers
	if (heap_empty(&cache->heap)) {
		return -1;
	}
	return heap_top(&cache->heap)->val; 
}
