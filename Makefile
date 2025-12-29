# ============================================================================
#  MINIS BUILD SYSTEM
# ============================================================================
PROJECT_NAME = minis

# Compiler Settings
CC ?= gcc
INCLUDE_DIRS = -Isrc -Itest

# --- Compilation Flags (Strict & Safe) ---
COMMON_FLAGS = -std=gnu11 -pthread \
    -Wall -Wextra -Werror -Wno-unused-parameter \
    -Wformat=2 -Werror=format-security \
    -Wconversion -Wimplicit-fallthrough \
    -Wwrite-strings -Wstrict-prototypes -Wold-style-definition \
    -Wshadow -Wredundant-decls -Wnested-externs -Wmissing-include-dirs \
    -Wpedantic -Wundef -Wcast-align -Wswitch-enum \
    -Wfloat-equal -Wcast-qual -Wstrict-overflow=2

# Compiler-specific adjustments (GCC vs Clang)
COMPILER_VERSION := $(shell $(CC) --version)
ifneq ($(findstring clang,$(COMPILER_VERSION)),)
    COMMON_FLAGS += -Wno-unknown-warning-option
else
    COMMON_FLAGS += -Wjump-misses-init -Wlogical-op
endif

# --- Build Profiles ---
# RELEASE: Max speed, LTO, CPU specific optimizations
RELEASE_FLAGS = $(COMMON_FLAGS) -O3 -flto=auto -march=native -mtune=native \
    -fno-plt -fno-semantic-interposition -fomit-frame-pointer -funroll-loops \
    -DNDEBUG -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=3 \
    -fstack-protector-strong -fstack-clash-protection

RELEASE_LDFLAGS = -pthread -flto=auto -march=native -O3 \
    -Wl,-z,nodlopen -Wl,-z,noexecstack -Wl,-z,relro,-z,now \
    -Wl,--as-needed

# DEBUG: Symbols, no optimization
DEBUG_FLAGS = $(COMMON_FLAGS) -O0 -g3 -ggdb -DDEBUG_LOGGING -fno-omit-frame-pointer
DEBUG_LDFLAGS = -pthread -g

# SANITIZERS: Bug hunting
SAN_BASE_FLAGS = $(COMMON_FLAGS) -O1 -g -fno-omit-frame-pointer -DDEBUG_LOGGING
ASAN_FLAGS = $(SAN_BASE_FLAGS) -fsanitize=address
UBSAN_FLAGS = $(SAN_BASE_FLAGS) -fsanitize=undefined
TSAN_FLAGS = $(SAN_BASE_FLAGS) -fsanitize=thread

# ============================================================================
#  DIRECTORY STRUCTURE
# ============================================================================
SRC_DIR = src
# Root output directories (subdirectories added per profile)
OBJ_ROOT = build/obj
BIN_ROOT = build/bin

# ============================================================================
#  SOURCES
# ============================================================================
# Core Engine Sources (Excludes clients)
MINIS_SRCS := $(wildcard src/common/*.c src/io/*.c src/cache/*.c) src/main.c

# Tool Sources
CLI_SRCS   := src/interactive_client.c src/common/common.c
BENCH_SRCS := src/client.c src/common/common.c

# Test Sources
TEST_HEAP_SRCS  := test/heap_test.c src/cache/heap.c src/common/common.c
TEST_CACHE_SRCS := test/cache_test.c \
                   src/cache/cache.c src/cache/hashtable.c src/cache/heap.c \
                   src/cache/zset.c src/cache/thread_pool.c src/cache/avl.c \
                   src/cache/deque.c src/cache/hash.c \
                   src/io/buffer.c src/io/out.c \
                   src/common/common.c src/common/glob.c

TEST_PERSIST_SRCS := test/persistence_test.c \
                     src/cache/persistence.c src/cache/cache.c src/cache/hashtable.c \
                     src/cache/zset.c src/cache/heap.c src/cache/avl.c \
                     src/cache/thread_pool.c src/cache/deque.c \
                     src/io/buffer.c src/io/out.c src/cache/hash.c \
                     src/common/common.c src/common/glob.c

# ============================================================================
#  TARGET CONFIGURATION (Magic Happens Here)
# ============================================================================
# Default to Release profile
PROFILE ?= release

# Inject the profile name so C code can print it (e.g. "release", "android")
COMMON_FLAGS += -DBUILD_PROFILE=\"$(PROFILE)\"

ifeq ($(PROFILE),release)
    CFLAGS = $(RELEASE_FLAGS)
    LDFLAGS = $(RELEASE_LDFLAGS)
    BIN_DIR = $(BIN_ROOT)/release
    OBJ_DIR = $(OBJ_ROOT)/release

else ifeq ($(PROFILE),debug)
    CFLAGS = $(DEBUG_FLAGS)
    LDFLAGS = $(DEBUG_LDFLAGS)
    BIN_DIR = $(BIN_ROOT)/debug
    OBJ_DIR = $(OBJ_ROOT)/debug

else ifeq ($(PROFILE),asan)
    CFLAGS = $(ASAN_FLAGS)
    LDFLAGS = -pthread -fsanitize=address
    BIN_DIR = $(BIN_ROOT)/asan
    OBJ_DIR = $(OBJ_ROOT)/asan

else ifeq ($(PROFILE),ubsan)
    CFLAGS = $(UBSAN_FLAGS)
    LDFLAGS = -pthread -fsanitize=undefined
    BIN_DIR = $(BIN_ROOT)/ubsan
    OBJ_DIR = $(OBJ_ROOT)/ubsan

else ifeq ($(PROFILE),tsan)
    CFLAGS = $(TSAN_FLAGS)
    LDFLAGS = -pthread -fsanitize=thread
    BIN_DIR = $(BIN_ROOT)/tsan
    OBJ_DIR = $(OBJ_ROOT)/tsan

else ifeq ($(PROFILE),android)
    CFLAGS = $(RELEASE_FLAGS) -DMINIS_ANDROID
    # Android LDFLAGS: Explicitly set safe flags (no nodlopen/now)
    LDFLAGS = -pthread -flto=auto -march=native -O3 \
              -Wl,-z,noexecstack -Wl,-z,relro \
              -Wl,--as-needed
    BIN_DIR = $(BIN_ROOT)/android
    OBJ_DIR = $(OBJ_ROOT)/android
endif

# --- Binary Output Paths (Always the same names, just different folders) ---
TARGET_MINIS = $(BIN_DIR)/minis
TARGET_CLI   = $(BIN_DIR)/minis-cli
TARGET_BENCH = $(BIN_DIR)/minis-bench
TARGET_TEST_HEAP  = $(BIN_DIR)/heap_test
TARGET_TEST_CACHE = $(BIN_DIR)/cache_test
TARGET_TEST_PERSIST = $(BIN_DIR)/persistence_test

# --- Object Generators ---
MINIS_OBJS = $(patsubst %.c, $(OBJ_DIR)/%.o, $(MINIS_SRCS))
CLI_OBJS   = $(patsubst %.c, $(OBJ_DIR)/%.o, $(CLI_SRCS))
BENCH_OBJS = $(patsubst %.c, $(OBJ_DIR)/%.o, $(BENCH_SRCS))
TEST_HEAP_OBJS  = $(patsubst %.c, $(OBJ_DIR)/%.o, $(TEST_HEAP_SRCS))
TEST_CACHE_OBJS = $(patsubst %.c, $(OBJ_DIR)/%.o, $(TEST_CACHE_SRCS))
TEST_PERSIST_OBJS = $(patsubst %.c, $(OBJ_DIR)/%.o, $(TEST_PERSIST_SRCS))

# ============================================================================
#  RULES
# ============================================================================

.PHONY: all clean install debug asan ubsan tsan android \
        test test-android test-asan test-ubsan test-tsan test-debug heap-test

# Default: Build all binaries for the selected profile
all: $(TARGET_MINIS) $(TARGET_CLI) $(TARGET_BENCH)

# --- Profile Aliases (Shortcuts) ---
debug:
	@$(MAKE) PROFILE=debug all
asan:
	@$(MAKE) PROFILE=asan all
ubsan:
	@$(MAKE) PROFILE=ubsan all
tsan:
	@$(MAKE) PROFILE=tsan all
android:
	@$(MAKE) PROFILE=android all

# --- Linkage Rules ---

$(TARGET_MINIS): $(MINIS_OBJS)
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(CC) $(MINIS_OBJS) -o $@ $(LDFLAGS)

$(TARGET_CLI): $(CLI_OBJS)
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(CC) $(CLI_OBJS) -o $@ $(LDFLAGS)

$(TARGET_BENCH): $(BENCH_OBJS)
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(CC) $(BENCH_OBJS) -o $@ $(LDFLAGS)

$(TARGET_TEST_HEAP): $(TEST_HEAP_OBJS)
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(CC) $(TEST_HEAP_OBJS) -o $@ $(LDFLAGS)

$(TARGET_TEST_CACHE): $(TEST_CACHE_OBJS)
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(CC) $(TEST_CACHE_OBJS) -o $@ $(LDFLAGS) -lm

$(TARGET_TEST_PERSIST): $(TEST_PERSIST_OBJS)
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(CC) $(TEST_PERSIST_OBJS) -o $@ $(LDFLAGS) -lm

# --- Compilation Rule ---
$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	@echo "  CC      $<"
	@$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $< -o $@

# ============================================================================
#  TESTING UTILITIES
# ============================================================================

# Unit Tests (C Heap Test)
heap-test: $(TARGET_TEST_HEAP)
	@echo "--- Running Heap Unit Test ($(PROFILE)) ---"
	@./$(TARGET_TEST_HEAP)

# Unit Tests (C Cache Test)
cache-test: $(TARGET_TEST_CACHE)
	@echo "--- Running Cache Unit Test ($(PROFILE)) ---"
	@./$(TARGET_TEST_CACHE)

# Unit Tests (Persistence)
persist-test: $(TARGET_TEST_PERSIST)
	@echo "--- Running Persistence Test ($(PROFILE)) ---"
	@./$(TARGET_TEST_PERSIST)

# Integration Tests (Python E2E)
# Pass full path to CLIENT_BIN_PATH so python knows exactly which binary to use
test: $(TARGET_BENCH) $(TARGET_TEST_HEAP) $(TARGET_TEST_CACHE)
	@$(MAKE) heap-test
	@$(MAKE) cache-test
	@$(MAKE) persist-test
	@echo "--- Running E2E Tests ($(PROFILE)) ---"
	@CLIENT_BIN_PATH=$(TARGET_BENCH) python3 test/test_cmds_extra.py

# --- Profile-Specific Test Wrappers ---
test-android:
	@$(MAKE) PROFILE=android test

test-asan:
	@$(MAKE) PROFILE=asan test

test-ubsan:
	@$(MAKE) PROFILE=ubsan test

test-tsan:
	@$(MAKE) PROFILE=tsan test

test-debug:
	@$(MAKE) PROFILE=debug test

# ============================================================================
#  INSTALLATION & CLEANUP
# ============================================================================

# Installation (Defaults to release, but can install any profile if specified)
install:
	@echo "Installing minis ($(PROFILE)) to /usr/local/bin..."
	# Ensure the binary exists first
	@$(MAKE) PROFILE=$(PROFILE) all
	@install -m 755 $(BIN_ROOT)/$(PROFILE)/minis /usr/local/bin/
	@install -m 755 $(BIN_ROOT)/$(PROFILE)/minis-cli /usr/local/bin/
	@install -m 755 $(BIN_ROOT)/$(PROFILE)/minis-bench /usr/local/bin/
	@echo "Installation complete."

clean:
	@echo "Cleaning build directory..."
	@rm -rf build

# Debug Helper
print-%:
	@echo $* = $($*)
