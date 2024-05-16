CC=gcc
CFLAGS= -Wall -Werror

all: server client

server:  server.o connections.o list.o out.o hashtable.o zset.o strings.o common.o avl.o heap.o thread_pool.o deque.o 
	$(CC) $(CFLAGS) -o server server.o connections.o list.o out.c hashtable.o zset.c strings.o common.o avl.o heap.o thread_pool.o deque.o 

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

client: client.o common.o
	$(CC) $(CFLAGS) -o client client.o common.o

clean:
	rm -f *.o client server
