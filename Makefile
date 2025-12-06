CC=gcc
INCLUDE_PATHS = $(SRC_DIR) test
INCLUDE_DIRS = $(addprefix -I,$(INCLUDE_PATHS))

CFLAGS_BASE=-std=gnu11 -O3 -g -pthread \
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

# Append include directories to all CFLAGS definitions
CFLAGS = $(CFLAGS_BASE) $(INCLUDE_DIRS)
LDFLAGS = $(LDFLAGS_BASE)

ifeq ($(CC),gcc)
	CFLAGS += -Wjump-misses-init -Wlogical-op
endif

# --- Sanitizer Configurations (Includes remain necessary) ---
BASE_SANITIZE_FLAGS = -g -O1 -fno-omit-frame-pointer

CFLAGS_ASAN = $(BASE_SANITIZE_FLAGS) -fsanitize=address $(INCLUDE_DIRS)
LDFLAGS_ASAN = -fsanitize=address

CFLAGS_UBSAN = -g -O2 -fno-omit-frame-pointer -fsanitize=undefined $(INCLUDE_DIRS)
LDFLAGS_UBSAN = -fsanitize=undefined

CFLAGS_TSAN = $(BASE_SANITIZE_FLAGS) -fsanitize=thread $(INCLUDE_DIRS)
LDFLAGS_TSAN = -fsanitize=thread

SRC_DIR := src
OBJ_DIR := build/obj
BIN_DIR := build/bin

# --- Source Discovery (Uses the new directory structure) ---
COMMON_SRCS := $(wildcard $(SRC_DIR)/common/*.c)
IO_SRCS := $(wildcard $(SRC_DIR)/io/*.c)
CACHE_SRCS := $(wildcard $(SRC_DIR)/cache/*.c)
CLIENT_SRC_FILE := $(SRC_DIR)/client.c

# Combine all server-related sources
SERVER_SOURCES := $(COMMON_SRCS) $(IO_SRCS) $(CACHE_SRCS)
CLIENT_SOURCES := $(CLIENT_SRC_FILE) $(SRC_DIR)/common/common.c

# --- Test Source Definitions ---
TEST_HEAP_SRC = test/heap_test.c
# Name for the object file of the test runner itself
HEAP_TEST_OBJ_RUNNER = $(OBJ_DIR)/heap_test.o

HEAP_TEST_DEPS = \
    $(OBJ_DIR)/cache/heap.o \
    $(OBJ_DIR)/common/common.o

# Find the corresponding source files for the required object files
HEAP_TEST_DEP_SRCS = $(patsubst $(OBJ_DIR)/%.o, $(SRC_DIR)/%.c, $(HEAP_TEST_DEPS))


# --- Object File Calculation (Preserves subdirectory structure in OBJ_DIR) ---
SERVER_OBJ_PATHS := $(patsubst $(SRC_DIR)/%.c,%.o,$(SERVER_SOURCES))
SERVER_OBJ := $(addprefix $(OBJ_DIR)/,$(SERVER_OBJ_PATHS))

CLIENT_OBJ_PATHS := $(patsubst $(SRC_DIR)/%.c,%.o,$(CLIENT_SOURCES))
CLIENT_OBJ := $(addprefix $(OBJ_DIR)/,$(CLIENT_OBJ_PATHS))


# --- Final binaries ---
SERVER_BIN = $(BIN_DIR)/server
CLIENT_BIN = $(BIN_DIR)/client
HEAP_TEST_BIN = $(BIN_DIR)/heap_test

# Sanitizer binaries
SERVER_ASAN_BIN = $(BIN_DIR)/server_asan
CLIENT_ASAN_BIN = $(BIN_DIR)/client_asan
HEAP_TEST_ASAN_BIN = $(BIN_DIR)/heap_test_asan

SERVER_UBSAN_BIN = $(BIN_DIR)/server_ubsan
CLIENT_UBSAN_BIN = $(BIN_DIR)/client_ubsan
HEAP_TEST_UBSAN_BIN = $(BIN_DIR)/heap_test_ubsan

SERVER_TSAN_BIN = $(BIN_DIR)/server_tsan
CLIENT_TSAN_BIN = $(BIN_DIR)/client_tsan
HEAP_TEST_TSAN_BIN = $(BIN_DIR)/heap_test_tsan


all: $(SERVER_BIN) $(CLIENT_BIN)

analyze:
	mkdir -p build/analyze
	cd build/analyze && \
	# This rule needs manual adjustment to reference files correctly
	$(CC) $(CFLAGS) -fanalyzer $(SERVER_SOURCES) $(CLIENT_SRC_FILE) -c

# --- Build binaries (Standard) ---
$(SERVER_BIN): $(SERVER_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

$(CLIENT_BIN): $(CLIENT_OBJ)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# --- Build Heap Test Binary (Standard) ---
# DEPENDENCY FIX: The HEAP_TEST_BIN now explicitly depends on the HEAP_TEST_DEPS
# objects, ensuring they are compiled before linking, fixing the "undefined reference" error.
$(HEAP_TEST_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# ------------------------------------
# --- Build binaries (Sanitized) ---
# ------------------------------------

asan: $(SERVER_ASAN_BIN) $(CLIENT_ASAN_BIN) $(HEAP_TEST_ASAN_BIN)

# Standard ASAN rules (unchanged)
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

# ASAN for Heap Test
$(HEAP_TEST_ASAN_BIN): CFLAGS := $(CFLAGS_ASAN)
$(HEAP_TEST_ASAN_BIN): LDFLAGS := $(LDFLAGS_ASAN)
# DEPENDENCY FIX (ASAN): Ensure required library objects are built before linking.
$(HEAP_TEST_ASAN_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^


ubsan: $(SERVER_UBSAN_BIN) $(CLIENT_UBSAN_BIN) $(HEAP_TEST_UBSAN_BIN)

# Standard UBSAN rules (unchanged)
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

# UBSAN for Heap Test
$(HEAP_TEST_UBSAN_BIN): CFLAGS := $(CFLAGS_UBSAN)
$(HEAP_TEST_UBSAN_BIN): LDFLAGS := $(LDFLAGS_UBSAN)
# DEPENDENCY FIX (UBSAN): Ensure required library objects are built before linking.
$(HEAP_TEST_UBSAN_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@


tsan: $(SERVER_TSAN_BIN) $(CLIENT_TSAN_BIN) $(HEAP_TEST_TSAN_BIN)

# Standard TSAN rules (unchanged)
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

# TSAN for Heap Test
$(HEAP_TEST_TSAN_BIN): CFLAGS := $(CFLAGS_TSAN)
$(HEAP_TEST_TSAN_BIN): LDFLAGS := $(LDFLAGS_TSAN)
# DEPENDENCY FIX (TSAN): Ensure required library objects are built before linking.
$(HEAP_TEST_TSAN_BIN): $(HEAP_TEST_OBJ_RUNNER) $(HEAP_TEST_DEPS)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@chmod +x $@


# --- Compile C to object files (General Rule) ---
# Handles all files in the src/ directory (e.g., src/cache/heap.c -> build/obj/cache/heap.o)
# The output object file path is derived from the source file path, preserving subdirectories.
# Example: src/cache/heap.c -> build/obj/cache/heap.o
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	# IMPORTANT: We use the *default* CFLAGS here which now include $(INCLUDE_DIRS)
	$(CC) $(CFLAGS) -c $< -o $@
	 
# --- Compile C to object files (Specific Test Runner Rule) ---
# Rule for the heap_test.c file which is outside of SRC_DIR
$(HEAP_TEST_OBJ_RUNNER): $(TEST_HEAP_SRC)
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@


# --- Test Targets ---
TEST_SCRIPT = test/test_cmds_extra.py

# FIX: Define the PHONY target 'heap-test' to build and run the unit test binary.
heap-test: $(HEAP_TEST_BIN)
	@echo "--- Running C Unit Test (HeapTest) ---"
	@./$(HEAP_TEST_BIN)

test: $(CLIENT_BIN) $(HEAP_TEST_BIN)
	@echo "--- Running C Unit Test (HeapTest) ---"
	@./$(HEAP_TEST_BIN)
	@echo "--- Running standard E2E tests with $(CLIENT_BIN) ---"
	@python3 $(TEST_SCRIPT)

test-asan: $(SERVER_ASAN_BIN) $(CLIENT_ASAN_BIN) $(HEAP_TEST_ASAN_BIN)
	@echo "--- Running C Unit Test (HeapTest) with ASan ---"
	@./$(HEAP_TEST_ASAN_BIN)
	@echo "--- Running ASan E2E tests with $(CLIENT_ASAN_BIN) ---"
	@CLIENT_BIN_NAME=client_asan python3 $(TEST_SCRIPT)

test-ubsan: $(SERVER_UBSAN_BIN) $(CLIENT_UBSAN_BIN) $(HEAP_TEST_UBSAN_BIN)
	@echo "--- Running C Unit Test (HeapTest) with UBSan ---"
	@./$(HEAP_TEST_UBSAN_BIN)
	@echo "--- Running UBSan E2E tests with $(CLIENT_UBSAN_BIN) ---"
	@CLIENT_BIN_NAME=client_ubsan python3 $(TEST_SCRIPT)

test-tsan: $(SERVER_TSAN_BIN) $(CLIENT_TSAN_BIN) $(HEAP_TEST_TSAN_BIN)
	@echo "--- Running C Unit Test (HeapTest) with TSan ---"
	@./$(HEAP_TEST_TSAN_BIN)
	@echo "--- Running TSan E2E tests with $(CLIENT_TSAN_BIN) ---"
	@CLIENT_BIN_NAME=client_tsan python3 $(TEST_SCRIPT)

clean:
	rm -rf build

.PHONY: all clean analyze asan ubsan tsan test test-asan test-ubsan test-tsan heap-test
