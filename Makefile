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

# --- Project structure ---
SRC_DIR := src
OBJ_DIR := build/obj
BIN_DIR := build/bin

# --- Sources ---
SERVER_SRC = server.c connections.c list.c out.c hashtable.c zset.c buffer.c \
             common.c avl.c heap.c thread_pool.c deque.c cache.c

CLIENT_SRC = client.c common.c

SERVER_OBJ = $(addprefix $(OBJ_DIR)/,$(SERVER_SRC:.c=.o))
CLIENT_OBJ = $(addprefix $(OBJ_DIR)/,$(CLIENT_SRC:.c=.o))

# --- Final binaries ---
SERVER_BIN = $(BIN_DIR)/server
CLIENT_BIN = $(BIN_DIR)/client

all: $(SERVER_BIN) $(CLIENT_BIN)

analyze:
	mkdir -p build/analyze
	cd build/analyze && \
	$(CC) $(CFLAGS) -fanalyzer $(addprefix ../../$(SRC_DIR)/,$(SERVER_SRC) $(CLIENT_SRC)) -c


# --- Build binaries ---
$(SERVER_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(CLIENT_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# --- Compile C → object files ---
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf build

.PHONY: all clean analyze
