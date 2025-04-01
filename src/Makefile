CC=gcc
CFLAGS=-O3 -Wformat -Wformat=2 -Wconversion -Wimplicit-fallthrough -Werror=format-security -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=3 -D_GLIBCXX_ASSERTIONS -fstrict-flex-arrays=3 -fstack-clash-protection -fstack-protector-strong -Wl,-z,nodlopen -Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now -Wl,--as-needed -Wl,--no-copy-dt-needed-entries -Wall -Werror -Wno-unused-parameter -Wwrite-strings -Wstrict-prototypes -Wold-style-definition -Wshadow -Wredundant-decls -Wnested-externs -Wmissing-include-dirs -g -Wextra
ifeq ($(CC),gcc)
	CFLAGS += -Wjump-misses-init -Wlogical-op
endif

all: server client

server:  server.o connections.o list.o out.o hashtable.o zset.o strings.o common.o avl.o heap.o thread_pool.o deque.o cache.o
	$(CC) $(CFLAGS) -o server server.o connections.o list.o out.c hashtable.o zset.c strings.o common.o avl.o heap.o thread_pool.o deque.o cache.o

server.o: server.c
	$(CC) $(CFLAGS) -c server.c

connections.o:
	$(CC) $(CFLAGS) -c connections.c

list.o: list.c list.h
	$(CC) $(CFLAGS) -c list.c

out.o: out.c out.h
	$(CC) $(CFLAGS) -c out.c

hashtable.o: hashtable.c hashtable.h
	$(CC) $(CFLAGS) -c hashtable.c

zset.o: zset.c zset.h
	$(CC) $(CFLAGS) -c zset.c

strings.o: strings.c strings.h
	$(CC) $(CFLAGS) -c strings.c

common.o: common.c common.h
	$(CC) $(CFLAGS) -c common.c

avl.o: avl.c avl.h
	$(CC) $(CFLAGS) -c avl.c

heap.o: heap.c heap.h
	$(CC) $(CFLAGS) -c heap.c

thread_pool.o: thread_pool.c thread_pool.h
	$(CC) $(CFLAGS) -c thread_pool.c

deque.o: deque.c deque.h
	$(CC) $(CFLAGS) -c deque.c

client.o: client.c
	$(CC) $(CFLAGS) -c client.c

cache.o: cache.c cache.h
	$(CC) $(CFLAGS) -c cache.c

client: client.o common.o
	$(CC) $(CFLAGS) -o client client.o common.o

clean:
	rm -f *.o client server
