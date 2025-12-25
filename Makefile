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
BIN_DIR = build/bin
# Objects are segregated by build profile to prevent conflicts
OBJ_ROOT = build/obj

# ============================================================================
#  SOURCES
# ============================================================================
# Core Engine Sources (Excludes clients)
MINIS_SRCS := $(wildcard src/common/*.c src/io/*.c src/cache/*.c) src/main.c

# Tool Sources
CLI_SRCS   := src/interactive_client.c src/common/common.c
BENCH_SRCS := src/client.c src/common/common.c

# Test Sources
TEST_SRCS  := test/heap_test.c src/cache/heap.c src/common/common.c

# ============================================================================
#  TARGET CONFIGURATION (Magic Happens Here)
# ============================================================================
# Default to Release profile
PROFILE ?= release

ifeq ($(PROFILE),release)
    CFLAGS = $(RELEASE_FLAGS)
    LDFLAGS = $(RELEASE_LDFLAGS)
    OBJ_DIR = $(OBJ_ROOT)/release
    SUFFIX =
else ifeq ($(PROFILE),debug)
    CFLAGS = $(DEBUG_FLAGS)
    LDFLAGS = $(DEBUG_LDFLAGS)
    OBJ_DIR = $(OBJ_ROOT)/debug
    SUFFIX = _debug
else ifeq ($(PROFILE),asan)
    CFLAGS = $(ASAN_FLAGS)
    LDFLAGS = -pthread -fsanitize=address
    OBJ_DIR = $(OBJ_ROOT)/asan
    SUFFIX = _asan
else ifeq ($(PROFILE),ubsan)
    CFLAGS = $(UBSAN_FLAGS)
    LDFLAGS = -pthread -fsanitize=undefined
    OBJ_DIR = $(OBJ_ROOT)/ubsan
    SUFFIX = _ubsan
else ifeq ($(PROFILE),tsan)
    CFLAGS = $(TSAN_FLAGS)
    LDFLAGS = -pthread -fsanitize=thread
    OBJ_DIR = $(OBJ_ROOT)/tsan
    SUFFIX = _tsan
else ifeq ($(PROFILE),android)
    CFLAGS = $(RELEASE_FLAGS) -DMINIS_ANDROID

    # Filter out flags that trigger "unsupported flags" warnings on Android
    LDFLAGS = -pthread -flto=auto -march=native -O3 \
              -Wl,-z,noexecstack -Wl,-z,relro \
              -Wl,--as-needed
    OBJ_DIR = $(OBJ_ROOT)/android
    SUFFIX = _android
endif

# Binary Names
TARGET_MINIS = $(BIN_DIR)/minis$(SUFFIX)
TARGET_CLI   = $(BIN_DIR)/minis-cli$(SUFFIX)
TARGET_BENCH = $(BIN_DIR)/minis-bench$(SUFFIX)
TARGET_TEST  = $(BIN_DIR)/heap_test$(SUFFIX)

# Object Generators
MINIS_OBJS = $(patsubst %.c, $(OBJ_DIR)/%.o, $(MINIS_SRCS))
CLI_OBJS   = $(patsubst %.c, $(OBJ_DIR)/%.o, $(CLI_SRCS))
BENCH_OBJS = $(patsubst %.c, $(OBJ_DIR)/%.o, $(BENCH_SRCS))
TEST_OBJS  = $(patsubst %.c, $(OBJ_DIR)/%.o, $(TEST_SRCS))

# ============================================================================
#  RULES
# ============================================================================

.PHONY: all clean install debug asan ubsan tsan test heap-test

# Default: Build release binaries
all: $(TARGET_MINIS) $(TARGET_CLI) $(TARGET_BENCH)

# --- Profile Aliases ---
# These allow you to run 'make debug' and it sets PROFILE=debug internally
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

$(TARGET_TEST): $(TEST_OBJS)
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(CC) $(TEST_OBJS) -o $@ $(LDFLAGS)

# --- Compilation Rule ---
$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	@echo "  CC      $<"
	@$(CC) $(CFLAGS) $(INCLUDE_DIRS) -c $< -o $@

# ============================================================================
#  UTILITIES
# ============================================================================

# Unit Tests
heap-test: $(TARGET_TEST)
	@echo "--- Running Heap Unit Test ---"
	@./$(TARGET_TEST)

# Integration Tests (Python)
# Auto-detects which binary to use based on PROFILE
test: $(TARGET_BENCH) $(TARGET_TEST)
	@$(MAKE) heap-test
	@echo "--- Running E2E Tests ($(PROFILE)) ---"
	@CLIENT_BIN_NAME=$(notdir $(TARGET_BENCH)) python3 test/test_cmds_extra.py

# Installation (Standard Linux Path)
install: release
	@echo "Installing minis to /usr/local/bin..."
	@install -m 755 build/bin/minis /usr/local/bin/
	@install -m 755 build/bin/minis-cli /usr/local/bin/
	@install -m 755 build/bin/minis-bench /usr/local/bin/
	@echo "Installation complete."

clean:
	@echo "Cleaning build directory..."
	@rm -rf build

# Helper to print vars (for debugging makefile)
print-%:
	@echo $* = $($*)
