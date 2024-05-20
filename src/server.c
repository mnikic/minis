/*
 ============================================================================
 Name        : 06_c.c
 Author      : Milos
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <time.h>
#include <netinet/ip.h>
#include <sys/epoll.h>
#include <math.h>
#include "connections.h"
#include "strings.h"
#include "out.h"
#include "avl.h"
#include "common.h"
#include "zset.h"
#include "heap.h"
#include "thread_pool.h"

#define MAX_EVENTS 10000
#define K_MAX_MSG 4096

#define container_of(ptr, type, member) ({\
	const typeof( ((type *)0)->member ) *__mptr = (ptr);\
	(type *)( (char *)__mptr - offsetof(type,member) );})

enum {
	RES_OK = 0, RES_ERR = 1, RES_NX = 2,
};

enum {
	ERR_UNKNOWN = 1, ERR_2BIG = 2, ERR_TYPE = 3, ERR_ARG = 4,
};

static int cmd_is(const char *word, const char *cmd) {
	return 0 == strcasecmp(word, cmd);
}

static void fd_set_nb(int fd) {
	errno = 0;
	int flags = fcntl(fd, F_GETFL, 0);
	if (errno) {
		die("fcntl error");
		return;
	}

	flags |= O_NONBLOCK;

	errno = 0;
	(void) fcntl(fd, F_SETFL, flags);
	if (errno) {
		die("fcntl error");
	}
}

static uint64_t get_monotonic_usec() {
	struct timespec tv = { 0, 0 };
	clock_gettime(CLOCK_MONOTONIC, &tv);
	return (uint64_t) tv.tv_sec * 1000000 + tv.tv_nsec / 1000;
}

enum {
	STATE_REQ = 0, STATE_RES = 1, STATE_END = 2, // mark the connection for deletion
};

// The data structure for the key space.
static struct {
	HMap db;

	// a map of all client connections, keyed by fd
	Conns *fd2conn;
	// timers for idle connections
	DList idle_list;

	// timers for TTLs
	Heap heap;

	// the thread pool
	TheadPool tp;
} g_data;

static int32_t accept_new_conn(int fd) {
// accept
	struct sockaddr_in client_addr = { };
	socklen_t socklen = sizeof(client_addr);
	int connfd = accept(fd, (struct sockaddr*) &client_addr, &socklen);
	if (connfd < 0) {
		msg("accept() error");
		return -1;  // error
	}

// set the new connection fd to nonblocking mode
	fd_set_nb(connfd);
// creating the struct Conn
	Conn *conn = (Conn*) malloc(sizeof(Conn));
	if (!conn) {
		close(connfd);
		return -1;
	}
	conn->fd = connfd;
	conn->state = STATE_REQ;
	conn->rbuf_size = 0;
	conn->wbuf_size = 0;
	conn->wbuf_sent = 0;
	conn->idle_start = get_monotonic_usec();
	dlist_insert_before(&g_data.idle_list, &conn->idle_list);
	conns_set(g_data.fd2conn, conn);
	return connfd;
}

static void conn_done(Conn *conn) {
	conns_del(g_data.fd2conn, conn->fd);
	(void) close(conn->fd);
	dlist_detach(&conn->idle_list);
	free(conn);
}

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
static void do_zadd(char **cmd, String *out) {
	double score = 0;
	if (!str2dbl(cmd[2], &score)) {
		return out_err(out, ERR_ARG, "expect fp number");
	}

	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);
	HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

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
		hm_insert(&g_data.db, &ent->node);
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

static int expect_zset(String *out, char *s, Entry **ent) {
	Entry key;
	key.key = s;
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);
	HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
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
static void do_zrem(char **cmd, String *out) {
	Entry *ent = NULL;
	if (!expect_zset(out, cmd[1], &ent)) {
		return;
	}

	ZNode *znode = zset_pop(ent->zset, cmd[2], strlen(cmd[2]) - 1);
	if (znode) {
		znode_del(znode);
	}
	return out_int(out, znode ? 1 : 0);
}

// zscore zset name
static void do_zscore(char **cmd, String *out) {
	Entry *ent = NULL;
	if (!expect_zset(out, cmd[1], &ent)) {
		return;
	}

	ZNode *znode = zset_lookup(ent->zset, cmd[2], strlen(cmd[2]) - 1);
	return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(char **cmd, String *out) {
// parse args
	double score = 0;
	if (!str2dbl(cmd[2], &score)) {
		return out_err(out, ERR_ARG, "expect fp number");
	}
	int64_t offset = 0;
	int64_t limit = 0;
	if (!str2int(cmd[4], &offset)) {
		char *message = "expect int";
		return out_err(out, ERR_ARG, message);
	}
	if (!str2int(cmd[5], &limit)) {
		return out_err(out, ERR_ARG, "expect int");
	}

// get the zset
	Entry *ent = NULL;
	if (!expect_zset(out, cmd[1], &ent)) {
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
static void entry_set_ttl(Entry *ent, int64_t ttl_ms) {
	if (ttl_ms < 0 && ent->heap_idx != (size_t) -1) {
		(void) heap_remove_idx(&g_data.heap, ent->heap_idx);
		ent->heap_idx = -1;
	} else if (ttl_ms >= 0) {
		size_t pos = ent->heap_idx;
		if (pos == (size_t) -1) {
			// add an new item to the heap
			HeapItem item;
			item.ref = &ent->heap_idx;
			item.val = get_monotonic_usec() + (uint64_t) ttl_ms * 1000;
			heap_add(&g_data.heap, &item);
		} else {
			heap_get(&g_data.heap, pos)->val = get_monotonic_usec() + (uint64_t) ttl_ms * 1000; 
			heap_update(&g_data.heap, pos);
		}
	}
}

static void do_keys(char **cmd, String *out) {
	(void) cmd;
	out_arr(out, (uint32_t) hm_size(&g_data.db));
	h_scan(&g_data.db.ht1, &cb_scan, out);
	h_scan(&g_data.db.ht2, &cb_scan, out);
}

static void do_del(char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_pop(&g_data.db, &key.node, &entry_eq);
	if (node) {
		free(container_of(node, Entry, node));
	}
	out_int(out, node ? 1 : 0);
}

static void do_set(char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
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
		hm_insert(&g_data.db, &ent->node);
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
    free(ent);
}

static void entry_del_async(void *arg) {
    entry_destroy((Entry *)arg);
}

static void entry_del(Entry *ent) {
	entry_set_ttl(ent, -1);

	const size_t k_large_container_size = 10000;
	int too_big = FALSE;
	switch (ent->type) {
	case T_ZSET:
		too_big = hm_size(&ent->zset->hmap) > k_large_container_size;
		break;
	}

	if (too_big) {
		thread_pool_queue(&g_data.tp, &entry_del_async, ent);
	} else {
		entry_destroy(ent);
	}
}

static void do_get(char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
	if (!node) {
		out_nil(out);
		return;
	}

	char *val = container_of(node, Entry, node)->val;
	int val_size = sizeof val - 1;
	assert(val_size <= K_MAX_MSG);
	out_str(out, val);
}

static void do_expire(char **cmd, String *out) {
	int64_t ttl_ms = 0;
	if (!str2int(cmd[2], &ttl_ms)) {
		return out_err(out, ERR_ARG, "expect int64");
	}

	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
	if (node) {
		Entry *ent = container_of(node, Entry, node);
		entry_set_ttl(ent, ttl_ms);
	}
	return out_int(out, node ? 1 : 0);
}

static void do_ttl(char **cmd, String *out) {
	Entry key;
	key.key = cmd[1];
	key.node.hcode = str_hash((uint8_t*) key.key, sizeof key.key - 1);

	HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
	if (!node) {
		return out_int(out, -2);
	}

	Entry *ent = container_of(node, Entry, node);
	if (ent->heap_idx == (size_t) -1) {
		return out_int(out, -1);
	}

	uint64_t expire_at = heap_get(&g_data.heap, ent->heap_idx)->val;
	uint64_t now_us = get_monotonic_usec();
	return out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
}

static void state_req(Conn *conn);
static void state_res(Conn *conn);

static int32_t do_request(const uint8_t *req, uint32_t reqlen, String *out) {
	char *cmd[4];
	int cmd_size = 0;
	int return_value = 0;

	if (reqlen < 4) {
		return -1;
	}
	uint32_t n = 0;
	memcpy(&n, &req[0], 4);
	if (n > K_MAX_MSG) {
		return -1;
	}
	if (n < 1) {
		return -1;
	}

	size_t pos = 4;
	while (n--) {
		if (pos + 4 > reqlen) {
			return_value = -1;
			goto CLEANUP;
		}
		uint32_t sz = 0;
		memcpy(&sz, &req[pos], 4);
		if (pos + 4 + sz > reqlen) {
			return -1;
		}
		cmd[cmd_size] = (char*) (calloc(sz, sizeof(char*)));
		memcpy(cmd[cmd_size], &req[pos + 4], sz);
		cmd_size++;
		pos += 4 + sz;
	}

	if (pos != reqlen) {
		return_value = -1;  // trailing garbage
		goto CLEANUP;

	}

	if (cmd_size == 1 && cmd_is(cmd[0], "keys")) {
		do_keys(cmd, out);
	} else if (cmd_size == 2 && cmd_is(cmd[0], "get")) {
		do_get(cmd, out);
	} else if (cmd_size == 3 && cmd_is(cmd[0], "set")) {
		do_set(cmd, out);
	} else if (cmd_size == 2 && cmd_is(cmd[0], "del")) {
		do_del(cmd, out);
	} else if (cmd_size == 3 && cmd_is(cmd[0], "pexpire")) {
		do_expire(cmd, out);
	} else if (cmd_size == 2 && cmd_is(cmd[0], "pttl")) {
		do_ttl(cmd, out);
	} else if (cmd_size == 4 && cmd_is(cmd[0], "zadd")) {
		do_zadd(cmd, out);
	} else if (cmd_size == 3 && cmd_is(cmd[0], "zrem")) {
		do_zrem(cmd, out);
	} else if (cmd_size == 3 && cmd_is(cmd[0], "zscore")) {
		do_zscore(cmd, out);
	} else if (cmd_size == 6 && cmd_is(cmd[0], "zquery")) {
		do_zquery(cmd, out);
	} else {
		out_err(out, ERR_UNKNOWN, "Unknown cmd");
	}

	CLEANUP: for (int i = 0; i < cmd_size; i++) {
		if (cmd[i]) {
			free(cmd[i]);
		}
	}
	return return_value;
}

static int32_t try_one_request(Conn *conn, uint32_t *start_index) {
// try to parse a request from the buffer
	if (conn->rbuf_size < *start_index + 4) {
		// not enough data in the buffer. Will retry in the next iteration
		return FALSE;
	}
	uint32_t len = 0;
	memcpy(&len, &conn->rbuf[*start_index], 4);
	if (len > K_MAX_MSG) {
		msg("too long");
		conn->state = STATE_END;
		return FALSE;
	}
	if (4 + len + *start_index > conn->rbuf_size) {
		// not enough data in the buffer. Will retry in the next iteration
		return FALSE;
	}

	String *out = str_init(NULL);
	int32_t err = do_request(&conn->rbuf[4], len, out);
	if (err) {
		msg("bad req");
		conn->state = STATE_END;
		return FALSE;
	}
	uint32_t wlen = str_size(out);

	if ((conn->wbuf_size + wlen) > K_MAX_MSG) {
		// cannot append to the write buffer the current message (too long), need to write!
		conn->state = STATE_RES;
		state_res(conn);
	}

// generating echoing response
	memcpy(&conn->wbuf[conn->wbuf_size], &wlen, 4);
	memcpy(&conn->wbuf[conn->wbuf_size + 4], str_data(out), wlen);
	conn->wbuf_size += 4 + wlen;
	*start_index += 4 + len;

	if (*start_index >= conn->rbuf_size) {
		// we read it all, try to send!
		conn->state = STATE_RES;
		state_res(conn);
	}
	return (conn->state == STATE_REQ);
}

static int32_t try_fill_buffer(Conn *conn) {
// try to fill the buffer
	assert(conn->rbuf_size < sizeof(conn->rbuf));
	ssize_t rv = 0;
	do {
		size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
		rv = read(conn->fd, &conn->rbuf[conn->rbuf_size], cap);
	} while (rv < 0 && errno == EINTR);
	if (rv < 0 && errno == EAGAIN) {
		// got EAGAIN, stop.
		return FALSE;
	}
	if (rv < 0) {
		msg("read() error");
		conn->state = STATE_END;
		return FALSE;
	}
	if (rv == 0) {
		if (conn->rbuf_size > 0) {
			msg("unexpected EOF");
		} else {
			//msg("EOF");
		}
		conn->state = STATE_END;
		return FALSE;
	}

	uint32_t start_index = conn->rbuf_size;
	conn->rbuf_size += (size_t) rv;
	assert(conn->rbuf_size <= sizeof(conn->rbuf));

// Try to process requests one by one.
// Why is there a loop? Please read the explanation of "pipelining".
	while (try_one_request(conn, &start_index)) {
	}

	size_t remain = conn->rbuf_size - start_index;
	if (remain) {
		memmove(conn->rbuf, &conn->rbuf[start_index], remain);
	}
	conn->rbuf_size = remain;
	return (conn->state == STATE_REQ);
}

static void state_req(Conn *conn) {
	while (try_fill_buffer(conn)) {
	}
}

static int32_t try_flush_buffer(Conn *conn) {
	ssize_t rv = 0;
	do {
		size_t remain = conn->wbuf_size - conn->wbuf_sent;
		rv = write(conn->fd, &conn->wbuf[conn->wbuf_sent], remain);
	} while (rv < 0 && errno == EINTR);
	if (rv < 0 && errno == EAGAIN) {
		// got EAGAIN, stop.
		return FALSE;
	}
	if (rv < 0) {
		msg("write() error");
		conn->state = STATE_END;
		return FALSE;
	}
	conn->wbuf_sent += (size_t) rv;
	assert(conn->wbuf_sent <= conn->wbuf_size);
	if (conn->wbuf_sent == conn->wbuf_size) {
		// response was fully sent, change state back
		conn->state = STATE_REQ;
		conn->wbuf_sent = 0;
		conn->wbuf_size = 0;
		return FALSE;
	}
// still got some data in wbuf, could try to write again
	return TRUE;
}

static void state_res(Conn *conn) {
	while (try_flush_buffer(conn)) {
	}
}

const uint64_t k_idle_timeout_ms = 5 * 1000;

static int hnode_same(HNode *lhs, HNode *rhs) {
	return lhs == rhs;
}

static void process_timers() {
	// the extra 1000us is for the ms resolution of poll()
	uint64_t now_us = get_monotonic_usec() + 1000;

	// idle timers
	/*while (!dlist_empty(&g_data.idle_list)) {
		// code omitted...
	}*/

	while (!dlist_empty(&g_data.idle_list)) {
		Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
		uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
		if (next_us >= now_us + 1000) {
			// not ready, the extra 1000us is for the ms resolution of poll()
			break;
		}

		printf("removing idle connection: %d\n", next->fd);
		conn_done(next);
	}

	// TTL timers
	const size_t k_max_works = 2000;
	size_t nworks = 0;
	while (!heap_empty(&g_data.heap) && heap_top(&g_data.heap)->val < now_us) {
		Entry *ent = container_of(heap_top(&g_data.heap)->ref, Entry, heap_idx);
		HNode *node = hm_pop(&g_data.db, &ent->node, &hnode_same);
		assert(node == &ent->node);
		entry_del(ent);
		if (nworks++ >= k_max_works) {
			// don't stall the server if too many keys are expiring at once
			break;
		}
	}
}

static void connection_io(Conn *conn) {
// waked up by poll, update the idle timer
// by moving conn to the end of the list.
	conn->idle_start = get_monotonic_usec();
	dlist_detach(&conn->idle_list);
	dlist_insert_before(&g_data.idle_list, &conn->idle_list);

	if (conn->state == STATE_REQ) {
		state_req(conn);
	} else if (conn->state == STATE_RES) {
		state_res(conn);
	} else {
		assert(0);  // not expected
	}
}

static uint32_t next_timer_ms() {
	uint64_t now_us = get_monotonic_usec();
	uint64_t next_us = (uint64_t) -1;
	// idle timers
	if (!dlist_empty(&g_data.idle_list)) {
		Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
		next_us = next->idle_start + k_idle_timeout_ms * 1000;
	}

	// ttl timers
	if (!heap_empty(&g_data.heap) && heap_top(&g_data.heap)->val < next_us) {
		next_us = heap_top(&g_data.heap)->val;
	}

	if (next_us == (uint64_t) -1) {
		return 10000;   // no timer, the value doesn't matter
	}

	if (next_us <= now_us) {
		// missed?
		return 0;
	}
	return (uint32_t) ((next_us - now_us) / 1000);
}

int main() {
	struct epoll_event event, events[MAX_EVENTS];
	struct sockaddr_in addr = { };
	int epfd, fd, rv, val = 1;

	dlist_init(&g_data.idle_list);
	thread_pool_init(&g_data.tp, 4);
	heap_init(&g_data.heap);
	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		die("socket()");
	}

	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

	addr.sin_family = AF_INET;
	addr.sin_port = ntohs(PORT);
	addr.sin_addr.s_addr = ntohl(0);    // wildcard address 0.0.0.0
	rv = bind(fd, (const struct sockaddr*) &addr, sizeof(addr));
	if (rv) {
		die("bind()");
	}

// listen
	rv = listen(fd, SOMAXCONN);
	if (rv) {
		die("listen()");
	}
	printf("The server is listening on port %i.\n", PORT);

// a hash table of all client connections, keyed by fd
	g_data.fd2conn = conns_new(10);

// set the listen fd to nonblocking mode
	fd_set_nb(fd);

	epfd = epoll_create1(0);
	if (epfd == -1) {
		die("epoll_create1");
	}

	event.events = EPOLLIN | EPOLLET;
	event.data.fd = fd;
	if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event) == -1) {
		die("epoll ctl: listen_sock!");
	}
	while (TRUE) {
		int timeout_ms = (int) next_timer_ms();
		// poll for active fds
		int enfd_count = epoll_wait(epfd, events, MAX_EVENTS, timeout_ms);
		if (enfd_count < 0) {
			die("epoll_wait");
		}

		// process active connections
		for (size_t i = 0; i < enfd_count; ++i) {
			if (events[i].data.fd == fd) {
				int conn_fd = accept_new_conn(fd);
				if (conn_fd > -1) {
					struct epoll_event ev;
					ev.data.fd = conn_fd;
					ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
					epoll_ctl(epfd, EPOLL_CTL_ADD, ev.data.fd, &ev);
				}
			} else if (events[i].events & EPOLLIN) {
				Conn *conn = conns_get(g_data.fd2conn, events[i].data.fd);
				connection_io(conn);
				if (conn->state == STATE_END) {
					// client closed normally, or something bad happened.
					// destroy this connection
					conn_done(conn);
				}
			}
			if (events[i].events & (EPOLLHUP | EPOLLHUP | EPOLLERR)) {
				Conn *conn = conns_get(g_data.fd2conn, events[i].data.fd);
				if (conn) {
					conn_done(conn);
				}
			}
		}
		process_timers();
	}

	return 0;
}

