cmake_minimum_required(VERSION 3.18.1)

project("minis")

# 1. DEFINE PATH TO ROOT SOURCE
# Go up: cpp -> main -> src -> app -> android -> ROOT -> src
set(MINIS_CORE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../src)

add_library( minis
             SHARED
             
             # --- JNI SHIM (Lives right here in cpp/) ---
             minis_jni.c
             
             # --- CORE ENGINE (Lives in root src/) ---
             ${MINIS_CORE_DIR}/cache/minis.c
             ${MINIS_CORE_DIR}/cache/entry.c
             ${MINIS_CORE_DIR}/cache/hashtable.c
             ${MINIS_CORE_DIR}/cache/heap.c
             ${MINIS_CORE_DIR}/cache/zset.c
             ${MINIS_CORE_DIR}/cache/avl.c
             ${MINIS_CORE_DIR}/cache/t_string.c
             ${MINIS_CORE_DIR}/cache/t_hash.c
             ${MINIS_CORE_DIR}/cache/t_zset.c
             ${MINIS_CORE_DIR}/cache/hash.c
             ${MINIS_CORE_DIR}/cache/thread_pool.c
             ${MINIS_CORE_DIR}/cache/deque.c
             ${MINIS_CORE_DIR}/cache/persistence.c
             
             # --- UTILS ---
             ${MINIS_CORE_DIR}/io/buffer.c
             ${MINIS_CORE_DIR}/common/common.c
             ${MINIS_CORE_DIR}/common/glob.c
             )

# 2. Find Android libs
find_library( log-lib log )

# 3. Include Directories
target_include_directories(minis PRIVATE
                           # Local (for minis_jni.c)
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           # Root (for cache/minis.h)
                           ${MINIS_CORE_DIR}
                           )

# 4. Flags
target_compile_definitions(minis PRIVATE
                           MINIS_EMBEDDED=1
                           _GNU_SOURCE)

target_compile_options(minis PRIVATE -Wall -Wextra -O3 -pthread)

# 5. Link
target_link_libraries( minis ${log-lib} )
