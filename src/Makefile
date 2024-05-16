CC=gcc
CFLAGS= -Wall -Werror

all: server client

server:  server.o connections.o list.o out.o hashtable.o zset.o strings.o common.o avl.o heap.o thread_pool.o deque.o 
	$(CC) $(CFLAGS) -o server server.o connections.o list.o out.c hashtable.o zset.c strings.o common.o avl.o heap.o thread_pool.o deque.o 

server.o: server.c
	gcc -c server.c

connections.o:
	gcc -c connections.c

list.o: list.c list.h
	gcc -c list.c

out.o: out.c out.h
	gcc -c out.c

hashtable.o: hashtable.c hashtable.h
	gcc -c hashtable.c

zset.o: zset.c zset.h
	gcc -c zset.c

strings.o: strings.c strings.h
	gcc -c strings.c

common.o: common.c common.h
	gcc -c common.c

avl.o: avl.c avl.h
	gcc -c avl.c

heap.o: heap.c heap.h
	gcc -c heap.c

thread_pool.o: thread_pool.c thread_pool.h
	gcc -c thread_pool.c

deque.o: deque.c deque.h
	gcc -c deque.c

client: client.cpp
	g++ $(CFLAGS) -o client client.cpp  

clean:
	rm -f *.o client server
