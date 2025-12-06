CC=gcc
# Define Include Paths for the new directory structure
# CHANGED: Now only including the root src directory.
INCLUDE_DIRS = -I$(SRC_DIR)

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

# CHANGED: Use the simplified $(INCLUDE_DIRS)
CFLAGS_ASAN = $(BASE_SANITIZE_FLAGS) -fsanitize=address $(INCLUDE_DIRS)
LDFLAGS_ASAN = -fsanitize=address

# CHANGED: Use the simplified $(INCLUDE_DIRS)
CFLAGS_UBSAN = -g -O2 -fno-omit-frame-pointer -fsanitize=undefined $(INCLUDE_DIRS)
LDFLAGS_UBSAN = -fsanitize=undefined

# CHANGED: Use the simplified $(INCLUDE_DIRS)
CFLAGS_TSAN = $(BASE_SANITIZE_FLAGS) -fsanitize=thread $(INCLUDE_DIRS)
LDFLAGS_TSAN = -fsanitize=thread

SRC_DIR := src
OBJ_DIR := build/obj
BIN_DIR := build/bin

# --- Source Discovery (Uses the new directory structure) ---
COMMON_SRCS := $(wildcard $(SRC_DIR)/common/*.c)
IO_SRCS := $(wildcard $(SRC_DIR)/io/*.c)
CACHE_SRCS := $(wildcard $(SRC_DIR)/cache/*.c)
# Assuming client.c is now the only file outside the server subdirs
CLIENT_SRC_FILE := $(SRC_DIR)/client.c

# Combine all server-related sources
SERVER_SOURCES := $(COMMON_SRCS) $(IO_SRCS) $(CACHE_SRCS)

# The client needs its own source file and the common library for linking
# NOTE: If client.c depends on common.c, it needs it here.
CLIENT_SOURCES := $(CLIENT_SRC_FILE) $(SRC_DIR)/common/common.c


# --- Object File Calculation (Preserves subdirectory structure in OBJ_DIR) ---
# patsubst replaces $(SRC_DIR)/%.c with $(OBJ_DIR)/%.o
SERVER_OBJ_PATHS := $(patsubst $(SRC_DIR)/%.c,%.o,$(SERVER_SOURCES))
SERVER_OBJ := $(addprefix $(OBJ_DIR)/,$(SERVER_OBJ_PATHS))

CLIENT_OBJ_PATHS := $(patsubst $(SRC_DIR)/%.c,%.o,$(CLIENT_SOURCES))
CLIENT_OBJ := $(addprefix $(OBJ_DIR)/,$(CLIENT_OBJ_PATHS))


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
	# This rule needs manual adjustment to reference files correctly
	$(CC) $(CFLAGS) -fanalyzer $(SERVER_SOURCES) $(CLIENT_SRC_FILE) -c

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

# Note: The CFLAGS/LDFLAGS redefinition is now critical because
# it ensures the special sanitizer flags (and the INCLUDE_DIRS) are used.

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

# --- Compile C to object files (UPDATED RULE) ---
# This pattern rule handles files in subdirectories, creates the necessary
# directory structure in OBJ_DIR, and uses the correct include flags.
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	# IMPORTANT: We use the *default* CFLAGS here which now include $(INCLUDE_DIRS)
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
