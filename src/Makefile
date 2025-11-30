CC=gcc
CFLAGS=-std=gnu11 -O2 -g -pthread \
       -Wall -Wextra -Werror -Wno-unused-parameter \
       -Wformat=2 -Werror=format-security \
       -Wconversion -Wimplicit-fallthrough \
       -Wwrite-strings -Wstrict-prototypes -Wold-style-definition \
       -Wshadow -Wredundant-decls -Wnested-externs -Wmissing-include-dirs \
       -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=3 \
       -fstack-protector-strong -fstack-clash-protection \
       -fstrict-flex-arrays=3

LDFLAGS=-pthread \
        -Wl,-z,nodlopen -Wl,-z,noexecstack \
        -Wl,-z,relro,-z,now \
        -Wl,--as-needed -Wl,--no-copy-dt-needed-entries

ifeq ($(CC),gcc)
	CFLAGS += -Wjump-misses-init -Wlogical-op
endif

SERVER_SRC = server.c connections.c list.c out.c hashtable.c zset.c buffer.c \
             common.c avl.c heap.c thread_pool.c deque.c cache.c
SERVER_OBJ = $(SERVER_SRC:.c=.o)

CLIENT_SRC = client.c common.c
CLIENT_OBJ = $(CLIENT_SRC:.c=.o)

#analyze:
#	$(CC) $(CFLAGS) -fanalyzer $(SERVER_SRC) $(CLIENT_SRC) -c

all: server client

server: $(SERVER_OBJ)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

client: $(CLIENT_OBJ)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o server client

.PHONY: all clean #analyze
