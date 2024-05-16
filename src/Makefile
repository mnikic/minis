CC=gcc
CFLAGS= -Wall -Werror

all: server client

server:
	$(CC) $(CFLAGS) -o server server.c connections.c list.c out.c hashtable.c zset.c strings.c common.c avl.c heap.c thread_pool.c deque.c 

client:
	g++ $(CFLAGS) -o client client.cpp  
