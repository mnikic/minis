cmake_minimum_required(VERSION 3.18.1)

project("minis")

# 1. Define the library and ALL source files
add_library( minis
             SHARED
             # JNI Entry
             minis_jni.c
             
             # Core Cache Logic
             cache/minis.c
             cache/entry.c
             cache/hashtable.c
             cache/heap.c
             cache/zset.c
             cache/avl.c
             cache/t_string.c
             cache/t_hash.c
             cache/t_zset.c
             cache/hash.c
             cache/thread_pool.c
             cache/deque.c
             cache/persistence.c
             
             # IO & Common Utils
             io/buffer.c
             common/common.c
             common/glob.c
             )

# 2. Find Android system libraries
find_library( log-lib log )

# 3. Include directories
# This corresponds to -I./src in your bash script
target_include_directories(minis PRIVATE
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/common
                           ${CMAKE_CURRENT_SOURCE_DIR}/cache
                           ${CMAKE_CURRENT_SOURCE_DIR}/io
                           )

# 4. Define Flags
# - MINIS_EMBEDDED=1 matches your -DMINIS_EMBEDDED
target_compile_definitions(minis PRIVATE
                           MINIS_EMBEDDED=1
                           _GNU_SOURCE)

# Optimization flags (-O3) and warnings
target_compile_options(minis PRIVATE -Wall -Wextra -O3 -pthread)

# 5. Link libraries
# Android provides JNI and Math (-lm) automatically, but we link Log.
target_link_libraries( minis ${log-lib} )
