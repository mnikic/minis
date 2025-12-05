CC=gcc
CFLAGS_BASE=-std=gnu11 -O2 -g -pthread \
        -Wall -Wextra -Werror -Wno-unused-parameter \
        -Wformat=2 -Werror=format-security \
        -Wconversion -Wimplicit-fallthrough \
        -Wwrite-strings -Wstrict-prototypes -Wold-style-definition \
        -Wshadow -Wredundant-decls -Wnested-externs -Wmissing-include-dirs \
        -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=3 \
        -fstack-protector-strong -fstack-clash-protection \
        -fstrict-flex-arrays=3 \
        -Wpedantic -Wundef -Wcast-align -Wswitch-enum \
	-Wno-unused-parameter -Wfloat-equal -Wcast-qual -Wstrict-overflow=2


LDFLAGS_BASE=-pthread \
         -Wl,-z,nodlopen -Wl,-z,noexecstack \
         -Wl,-z,relro,-z,now \
         -Wl,--as-needed -Wl,--no-copy-dt-needed-entries

CFLAGS = $(CFLAGS_BASE)
LDFLAGS = $(LDFLAGS_BASE)

ifeq ($(CC),gcc)
    CFLAGS += -Wjump-misses-init -Wlogical-op
endif

# --- Sanitizer Configurations ---
# Note: Sanitizers are mutually exclusive and require separate builds.
BASE_SANITIZE_FLAGS = -g -O1 -fno-omit-frame-pointer

CFLAGS_ASAN = $(BASE_SANITIZE_FLAGS) -fsanitize=address
LDFLAGS_ASAN = -fsanitize=address

CFLAGS_UBSAN = -g -O2 -fno-omit-frame-pointer -fsanitize=undefined
LDFLAGS_UBSAN = -fsanitize=undefined

CFLAGS_TSAN = $(BASE_SANITIZE_FLAGS) -fsanitize=thread
LDFLAGS_TSAN = -fsanitize=thread

SRC_DIR := src
OBJ_DIR := build/obj
BIN_DIR := build/bin

# --- Sources ---
SERVER_SRC = server.c server_loop.c connections.c list.c out.c hashtable.c zset.c buffer.c \
             common.c avl.c heap.c thread_pool.c deque.c cache.c

CLIENT_SRC = client.c common.c

SERVER_OBJ = $(addprefix $(OBJ_DIR)/,$(SERVER_SRC:.c=.o))
CLIENT_OBJ = $(addprefix $(OBJ_DIR)/,$(CLIENT_SRC:.c=.o))

# --- Final binaries ---
SERVER_BIN = $(BIN_DIR)/server
CLIENT_BIN = $(BIN_DIR)/client

# Sanitizer binaries
SERVER_ASAN_BIN = $(BIN_DIR)/server_asan
CLIENT_ASAN_BIN = $(BIN_DIR)/client_asan
SERVER_UBSAN_BIN = $(BIN_DIR)/server_ubsan
CLIENT_UBSAN_BIN = $(BIN_DIR)/client_ubsan
SERVER_TSAN_BIN = $(BIN_DIR)/server_tsan
CLIENT_TSAN_BIN = $(BIN_DIR)/client_tsan


all: $(SERVER_BIN) $(CLIENT_BIN)

analyze:
	mkdir -p build/analyze
	cd build/analyze && \
	$(CC) $(CFLAGS) -fanalyzer $(addprefix ../../$(SRC_DIR)/,$(SERVER_SRC) $(CLIENT_SRC)) -c

# --- Build binaries (Standard) ---
$(SERVER_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(CLIENT_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# ------------------------------------
# --- Build binaries (Sanitized) ---
# ------------------------------------

asan: $(SERVER_ASAN_BIN) $(CLIENT_ASAN_BIN)

$(SERVER_ASAN_BIN): CFLAGS := $(CFLAGS_ASAN)
$(SERVER_ASAN_BIN): LDFLAGS := $(LDFLAGS_ASAN)
$(SERVER_ASAN_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(CLIENT_ASAN_BIN): CFLAGS := $(CFLAGS_ASAN)
$(CLIENT_ASAN_BIN): LDFLAGS := $(LDFLAGS_ASAN)
$(CLIENT_ASAN_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

ubsan: $(SERVER_UBSAN_BIN) $(CLIENT_UBSAN_BIN)

$(SERVER_UBSAN_BIN): CFLAGS := $(CFLAGS_UBSAN)
$(SERVER_UBSAN_BIN): LDFLAGS := $(LDFLAGS_UBSAN)
$(SERVER_UBSAN_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

$(CLIENT_UBSAN_BIN): CFLAGS := $(CFLAGS_UBSAN)
$(CLIENT_UBSAN_BIN): LDFLAGS := $(LDFLAGS_UBSAN)
$(CLIENT_UBSAN_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

tsan: $(SERVER_TSAN_BIN) $(CLIENT_TSAN_BIN)

$(SERVER_TSAN_BIN): CFLAGS := $(CFLAGS_TSAN)
$(SERVER_TSAN_BIN): LDFLAGS := $(LDFLAGS_TSAN)
$(SERVER_TSAN_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

$(CLIENT_TSAN_BIN): CFLAGS := $(CFLAGS_TSAN)
$(CLIENT_TSAN_BIN): LDFLAGS := $(LDFLAGS_TSAN)
$(CLIENT_TSAN_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

# --- Compile C to object files ---
# Default rule uses standard CFLAGS
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@
	
# --- Test Targets ---
TEST_SCRIPT = test/test_cmds_extra.py

test: $(CLIENT_BIN)
	@echo "--- Running standard tests with $(CLIENT_BIN) ---"
	@python3 $(TEST_SCRIPT) # Use python3 explicitly for consistency

test-asan: $(SERVER_ASAN_BIN) $(CLIENT_ASAN_BIN)
	@echo "--- Running ASan tests with $(CLIENT_ASAN_BIN) ---"
	@CLIENT_BIN_NAME=client_asan python3 $(TEST_SCRIPT)

test-ubsan: $(SERVER_UBSAN_BIN) $(CLIENT_UBSAN_BIN)
	@echo "--- Running UBSan tests with $(CLIENT_UBSAN_BIN) ---"
	@CLIENT_BIN_NAME=client_ubsan python3 $(TEST_SCRIPT)

test-tsan: $(SERVER_TSAN_BIN) $(CLIENT_TSAN_BIN)
	@echo "--- Running TSan tests with $(CLIENT_TSAN_BIN) ---"
	@CLIENT_BIN_NAME=client_tsan python3 $(TEST_SCRIPT)

clean:
	rm -rf build

.PHONY: all clean analyze asan ubsan tsan
