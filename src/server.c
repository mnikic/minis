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
#include "cache.h"
#include "connections.h"
#include "strings.h"
#include "common.h"

#define MAX_EVENTS 10000

enum {
	RES_OK = 0, RES_ERR = 1, RES_NX = 2,
};

enum {
	ERR_UNKNOWN = 1, ERR_2BIG = 2, ERR_TYPE = 3, ERR_ARG = 4,
};

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

enum {
	STATE_REQ = 0, STATE_RES = 1, STATE_END = 2, // mark the connection for deletion
};

// The data structure for the key space.
static struct {
	// a map of all client connections, keyed by fd
	Conns *fd2conn;
	// timers for idle connections
	DList idle_list;
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

static void state_req(Cache* cache, Conn *conn);
static void state_res(Conn *conn);

static int32_t do_request(Cache* cache, const uint8_t *req, uint32_t reqlen, String *out) {
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
	cache_execute(cache, cmd, cmd_size, out);

CLEANUP: for (int i = 0; i < cmd_size; i++) {
		if (cmd[i]) {
			free(cmd[i]);
		}
	}
	return return_value;
}

static int32_t try_one_request(Cache* cache, Conn *conn, uint32_t *start_index) {
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
	int32_t err = do_request(cache, &conn->rbuf[4], len, out);
	if (err) {
		msg("bad req");
		conn->state = STATE_END;
		str_free(out);
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
	str_free(out);
	return (conn->state == STATE_REQ);
}

static int32_t try_fill_buffer(Cache* cache, Conn *conn) {
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
	while (try_one_request(cache, conn, &start_index)) {
	}

	size_t remain = conn->rbuf_size - start_index;
	if (remain) {
		memmove(conn->rbuf, &conn->rbuf[start_index], remain);
	}
	conn->rbuf_size = remain;
	return (conn->state == STATE_REQ);
}

static void state_req(Cache* cache, Conn *conn) {
	while (try_fill_buffer(cache, conn)) {
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

static void process_timers(Cache* cache) {
	// the extra 1000us is for the ms resolution of poll()
	uint64_t now_us = get_monotonic_usec() + 1000;


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

	cache_evict(cache, now_us);
}

static void connection_io(Cache* cache, Conn *conn) {
// waked up by poll, update the idle timer
// by moving conn to the end of the list.
	conn->idle_start = get_monotonic_usec();
	dlist_detach(&conn->idle_list);
	dlist_insert_before(&g_data.idle_list, &conn->idle_list);

	if (conn->state == STATE_REQ) {
		state_req(cache, conn);
	} else if (conn->state == STATE_RES) {
		state_res(conn);
	} else {
		assert(0);  // not expected
	}
}

static uint32_t next_timer_ms(Cache* cache) {
	uint64_t now_us = get_monotonic_usec();
	uint64_t next_us = (uint64_t) -1;
	// idle timers
	if (!dlist_empty(&g_data.idle_list)) {
		Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
		next_us = next->idle_start + k_idle_timeout_ms * 1000;
	}

	uint64_t from_cache = cache_next_expiry(cache);
	if (from_cache != (uint64_t) -1 && from_cache < next_us) {
		next_us = from_cache;
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

int main(void) {
	struct epoll_event event, events[MAX_EVENTS];
	struct sockaddr_in addr = { };
	int epfd, fd, rv, val = 1;

	dlist_init(&g_data.idle_list);
	Cache* cache = cache_init();
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
		int timeout_ms = (int) next_timer_ms(cache);
		// poll for active fds
		int enfd_count = epoll_wait(epfd, events, MAX_EVENTS, timeout_ms);
		if (enfd_count < 0) {
			die("epoll_wait");
		}

		// process active connections
		for (int i = 0; i < enfd_count; ++i) {
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
				connection_io(cache, conn);
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
		process_timers(cache);
	}

	return 0;
}

