CC=gcc
INCLUDE_PATHS = $(SRC_DIR) test
INCLUDE_DIRS = $(addprefix -I,$(INCLUDE_PATHS))

# ============================================================================
# COMMON FLAGS (shared by all build types)
# ============================================================================
COMMON_FLAGS = -std=gnu11 -pthread \
	-Wall -Wextra -Werror -Wno-unused-parameter \
	-Wformat=2 -Werror=format-security \
	-Wconversion -Wimplicit-fallthrough \
	-Wwrite-strings -Wstrict-prototypes -Wold-style-definition \
	-Wshadow -Wredundant-decls -Wnested-externs -Wmissing-include-dirs \
	-Wpedantic -Wundef -Wcast-align -Wswitch-enum \
	-Wfloat-equal -Wcast-qual -Wstrict-overflow=2
#  -DK_ENABLE_BENCHMARK

# GCC-specific warnings
ifeq ($(CC),gcc)
	COMMON_FLAGS += -Wjump-misses-init -Wlogical-op
endif

# ============================================================================
# RELEASE BUILD (Maximum performance)
# ============================================================================
RELEASE_CFLAGS = $(COMMON_FLAGS) \
	-O3 -flto=auto \
	-march=native -mtune=native \
	-fno-plt -fno-semantic-interposition \
	-fomit-frame-pointer -funroll-loops \
	-DNDEBUG \
	-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=3 \
	-fstack-protector-strong -fstack-clash-protection \
	-fstrict-flex-arrays=3 \
	$(INCLUDE_DIRS)

RELEASE_LDFLAGS = -pthread \
	-flto=auto -march=native -O3 \
	-Wl,-z,nodlopen -Wl,-z,noexecstack \
	-Wl,-z,relro,-z,now \
	-Wl,--as-needed -Wl,--no-copy-dt-needed-entries

# ============================================================================
# DEBUG BUILD (Easy debugging, fast compilation)
# ============================================================================
DEBUG_CFLAGS = $(COMMON_FLAGS) \
	-O0 -g \
	-DDEBUG_LOGGING \
	-fno-omit-frame-pointer \
	$(INCLUDE_DIRS)

DEBUG_LDFLAGS = -pthread -g

# ============================================================================
# SANITIZER BUILDS (Bug detection)
# ============================================================================
SANITIZE_BASE = $(COMMON_FLAGS) \
	-O1 -g \
	-fno-omit-frame-pointer \
	-DDEBUG_LOGGING \
	$(INCLUDE_DIRS)

CFLAGS_ASAN = $(SANITIZE_BASE) -fsanitize=address
LDFLAGS_ASAN = -pthread -fsanitize=address

CFLAGS_UBSAN = $(SANITIZE_BASE) -fsanitize=undefined
LDFLAGS_UBSAN = -pthread -fsanitize=undefined

CFLAGS_TSAN = $(SANITIZE_BASE) -fsanitize=thread
LDFLAGS_TSAN = -pthread -fsanitize=thread

# ============================================================================
# Default to RELEASE build
# ============================================================================
CFLAGS = $(RELEASE_CFLAGS)
LDFLAGS = $(RELEASE_LDFLAGS)

# Override for debug targets
ifeq ($(MAKECMDGOALS),debug)
	CFLAGS = $(DEBUG_CFLAGS)
	LDFLAGS = $(DEBUG_LDFLAGS)
endif

ifeq ($(MAKECMDGOALS),test-debug)
	CFLAGS = $(DEBUG_CFLAGS)
	LDFLAGS = $(DEBUG_LDFLAGS)
endif

# ============================================================================
# DIRECTORY STRUCTURE
# ============================================================================
SRC_DIR := src
OBJ_DIR := build/obj
BIN_DIR := build/bin

# ============================================================================
# SOURCE FILES
# ============================================================================
COMMON_SRCS := $(wildcard $(SRC_DIR)/common/*.c)
IO_SRCS := $(wildcard $(SRC_DIR)/io/*.c)
CACHE_SRCS := $(wildcard $(SRC_DIR)/cache/*.c)
CLIENT_SRC_FILE := $(SRC_DIR)/client.c
SERVER_MAIN_SRC := $(SRC_DIR)/server_main.c
INTERACTIVE_CLIENT_SRC_FILE := $(SRC_DIR)/interactive_client.c

# Combine sources for each binary
SERVER_SOURCES := $(COMMON_SRCS) $(IO_SRCS) $(CACHE_SRCS) $(SERVER_MAIN_SRC)
CLIENT_SOURCES := $(CLIENT_SRC_FILE) $(SRC_DIR)/common/common.c
INTERACTIVE_CLIENT_SOURCES := $(INTERACTIVE_CLIENT_SRC_FILE) $(SRC_DIR)/common/common.c

# ============================================================================
# TEST SOURCE DEFINITIONS
# ============================================================================
TEST_HEAP_SRC = test/heap_test.c
HEAP_TEST_OBJ_RUNNER = $(OBJ_DIR)/heap_test.o

HEAP_TEST_DEPS = \
	$(OBJ_DIR)/cache/heap.o \
	$(OBJ_DIR)/common/common.o

HEAP_TEST_DEP_SRCS = $(patsubst $(OBJ_DIR)/%.o, $(SRC_DIR)/%.c, $(HEAP_TEST_DEPS))

# ============================================================================
# OBJECT FILES (preserves subdirectory structure)
# ============================================================================
SERVER_OBJ_PATHS := $(patsubst $(SRC_DIR)/%.c,%.o,$(SERVER_SOURCES))
SERVER_OBJ := $(addprefix $(OBJ_DIR)/,$(SERVER_OBJ_PATHS))

CLIENT_OBJ_PATHS := $(patsubst $(SRC_DIR)/%.c,%.o,$(CLIENT_SOURCES))
CLIENT_OBJ := $(addprefix $(OBJ_DIR)/,$(CLIENT_OBJ_PATHS))

INTERACTIVE_CLIENT_OBJ_PATHS := $(patsubst $(SRC_DIR)/%.c,%.o,$(INTERACTIVE_CLIENT_SOURCES))
INTERACTIVE_CLIENT_OBJ := $(addprefix $(OBJ_DIR)/,$(INTERACTIVE_CLIENT_OBJ_PATHS))

# ============================================================================
# BINARY NAMES
# ============================================================================
# Standard binaries
SERVER_BIN = $(BIN_DIR)/server
CLIENT_BIN = $(BIN_DIR)/client
INTERACTIVE_CLIENT_BIN = $(BIN_DIR)/interactive_client
HEAP_TEST_BIN = $(BIN_DIR)/heap_test

# Debug binaries
SERVER_DEBUG_BIN = $(BIN_DIR)/server_debug
CLIENT_DEBUG_BIN = $(BIN_DIR)/client_debug
INTERACTIVE_CLIENT_DEBUG_BIN = $(BIN_DIR)/interactive_client_debug

# Sanitizer binaries
SERVER_ASAN_BIN = $(BIN_DIR)/server_asan
CLIENT_ASAN_BIN = $(BIN_DIR)/client_asan
INTERACTIVE_CLIENT_ASAN_BIN = $(BIN_DIR)/interactive_client_asan
HEAP_TEST_ASAN_BIN = $(BIN_DIR)/heap_test_asan

SERVER_UBSAN_BIN = $(BIN_DIR)/server_ubsan
CLIENT_UBSAN_BIN = $(BIN_DIR)/client_ubsan
INTERACTIVE_CLIENT_UBSAN_BIN = $(BIN_DIR)/interactive_client_ubsan
HEAP_TEST_UBSAN_BIN = $(BIN_DIR)/heap_test_ubsan

SERVER_TSAN_BIN = $(BIN_DIR)/server_tsan
CLIENT_TSAN_BIN = $(BIN_DIR)/client_tsan
INTERACTIVE_CLIENT_TSAN_BIN = $(BIN_DIR)/interactive_client_tsan
HEAP_TEST_TSAN_BIN = $(BIN_DIR)/heap_test_tsan

# ============================================================================
# PHONY TARGETS
# ============================================================================
.PHONY: all clean analyze asan ubsan tsan test test-asan test-ubsan test-tsan heap-test debug test-debug

# ============================================================================
# DEFAULT TARGET
# ============================================================================
all: $(SERVER_BIN) $(CLIENT_BIN) $(INTERACTIVE_CLIENT_BIN)

# ============================================================================
# ANALYZER TARGET
# ============================================================================
analyze:
	mkdir -p build/analyze
	cd build/analyze && \
	$(CC) $(CFLAGS) -fanalyzer $(SERVER_SOURCES) $(CLIENT_SRC_FILE) $(INTERACTIVE_CLIENT_SRC_FILE) -c

# ============================================================================
# BUILD TARGETS
# ============================================================================
debug: $(SERVER_DEBUG_BIN) $(CLIENT_DEBUG_BIN) $(INTERACTIVE_CLIENT_DEBUG_BIN)
asan: $(SERVER_ASAN_BIN) $(CLIENT_ASAN_BIN) $(INTERACTIVE_CLIENT_ASAN_BIN) $(HEAP_TEST_ASAN_BIN)
ubsan: $(SERVER_UBSAN_BIN) $(CLIENT_UBSAN_BIN) $(INTERACTIVE_CLIENT_UBSAN_BIN) $(HEAP_TEST_UBSAN_BIN)
tsan: $(SERVER_TSAN_BIN) $(CLIENT_TSAN_BIN) $(INTERACTIVE_CLIENT_TSAN_BIN) $(HEAP_TEST_TSAN_BIN)

# ============================================================================
# STANDARD BINARIES
# ============================================================================
$(SERVER_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(CLIENT_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(INTERACTIVE_CLIENT_BIN): $(INTERACTIVE_CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(HEAP_TEST_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# ============================================================================
# DEBUG BINARIES
# ============================================================================
$(SERVER_DEBUG_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@echo "Debug server built successfully."

$(CLIENT_DEBUG_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@echo "Debug client built successfully."

$(INTERACTIVE_CLIENT_DEBUG_BIN): $(INTERACTIVE_CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@echo "Debug interactive client built successfully."

# ============================================================================
# ASAN BINARIES
# ============================================================================
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

$(INTERACTIVE_CLIENT_ASAN_BIN): CFLAGS := $(CFLAGS_ASAN)
$(INTERACTIVE_CLIENT_ASAN_BIN): LDFLAGS := $(LDFLAGS_ASAN)
$(INTERACTIVE_CLIENT_ASAN_BIN): $(INTERACTIVE_CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(HEAP_TEST_ASAN_BIN): CFLAGS := $(CFLAGS_ASAN)
$(HEAP_TEST_ASAN_BIN): LDFLAGS := $(LDFLAGS_ASAN)
$(HEAP_TEST_ASAN_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# ============================================================================
# UBSAN BINARIES
# ============================================================================
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

$(INTERACTIVE_CLIENT_UBSAN_BIN): CFLAGS := $(CFLAGS_UBSAN)
$(INTERACTIVE_CLIENT_UBSAN_BIN): LDFLAGS := $(LDFLAGS_UBSAN)
$(INTERACTIVE_CLIENT_UBSAN_BIN): $(INTERACTIVE_CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

$(HEAP_TEST_UBSAN_BIN): CFLAGS := $(CFLAGS_UBSAN)
$(HEAP_TEST_UBSAN_BIN): LDFLAGS := $(LDFLAGS_UBSAN)
$(HEAP_TEST_UBSAN_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

# ============================================================================
# TSAN BINARIES
# ============================================================================
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

$(INTERACTIVE_CLIENT_TSAN_BIN): CFLAGS := $(CFLAGS_TSAN)
$(INTERACTIVE_CLIENT_TSAN_BIN): LDFLAGS := $(LDFLAGS_TSAN)
$(INTERACTIVE_CLIENT_TSAN_BIN): $(INTERACTIVE_CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

$(HEAP_TEST_TSAN_BIN): CFLAGS := $(CFLAGS_TSAN)
$(HEAP_TEST_TSAN_BIN): LDFLAGS := $(LDFLAGS_TSAN)
$(HEAP_TEST_TSAN_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@

# ============================================================================
# OBJECT FILE COMPILATION
# ============================================================================
# Compile C source files to object files (preserves directory structure)
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# Compile test runner (outside of SRC_DIR)
$(HEAP_TEST_OBJ_RUNNER): $(TEST_HEAP_SRC)
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

# ============================================================================
# TEST TARGETS
# ============================================================================
TEST_SCRIPT = test/test_cmds_extra.py

heap-test: $(HEAP_TEST_BIN)
	@echo "--- Running C Unit Test (HeapTest) ---"
	@./$(HEAP_TEST_BIN)

test: $(CLIENT_BIN) $(HEAP_TEST_BIN)
	@echo "--- Running C Unit Test (HeapTest) ---"
	@./$(HEAP_TEST_BIN)
	@echo "--- Running standard E2E tests with $(CLIENT_BIN) ---"
	@python3 $(TEST_SCRIPT)

test-debug: $(SERVER_DEBUG_BIN) $(CLIENT_DEBUG_BIN) $(INTERACTIVE_CLIENT_DEBUG_BIN) $(HEAP_TEST_BIN)
	@echo "--- Running C Unit Test (HeapTest) ---"
	@./$(HEAP_TEST_BIN)
	@echo "--- Running debug E2E tests with $(CLIENT_DEBUG_BIN) ---"
	@CLIENT_BIN_NAME=client_debug python3 $(TEST_SCRIPT)

test-asan: $(SERVER_ASAN_BIN) $(CLIENT_ASAN_BIN) $(INTERACTIVE_CLIENT_ASAN_BIN) $(HEAP_TEST_ASAN_BIN)
	@echo "--- Running C Unit Test (HeapTest) with ASan ---"
	@./$(HEAP_TEST_ASAN_BIN)
	@echo "--- Running ASan E2E tests with $(CLIENT_ASAN_BIN) ---"
	@CLIENT_BIN_NAME=client_asan python3 $(TEST_SCRIPT)

test-ubsan: $(SERVER_UBSAN_BIN) $(CLIENT_UBSAN_BIN) $(INTERACTIVE_CLIENT_UBSAN_BIN) $(HEAP_TEST_UBSAN_BIN)
	@echo "--- Running C Unit Test (HeapTest) with UBSan ---"
	@./$(HEAP_TEST_UBSAN_BIN)
	@echo "--- Running UBSan E2E tests with $(CLIENT_UBSAN_BIN) ---"
	@CLIENT_BIN_NAME=client_ubsan python3 $(TEST_SCRIPT)

test-tsan: $(SERVER_TSAN_BIN) $(CLIENT_TSAN_BIN) $(INTERACTIVE_CLIENT_TSAN_BIN) $(HEAP_TEST_TSAN_BIN)
	@echo "--- Running C Unit Test (HeapTest) with TSan ---"
	@./$(HEAP_TEST_TSAN_BIN)
	@echo "--- Running TSan E2E tests with $(CLIENT_TSAN_BIN) ---"
	@CLIENT_BIN_NAME=client_tsan python3 $(TEST_SCRIPT)

# ============================================================================
# CLEAN TARGET
# ============================================================================
clean:
	rm -rf build
	rm -f $(BIN_DIR)/server_debug $(BIN_DIR)/client_debug $(BIN_DIR)/interactive_client_debug
