#!/bin/bash
# 1. Set JAVA_HOME dynamically
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))

# 2. Compile (Notice the output name change to libminis.so to match Java's expectation)
gcc -shared -Wall -Wextra -O3 -pthread -fPIC -DMINIS_EMBEDDED \
    -I"${JAVA_HOME}/include" \
    -I"${JAVA_HOME}/include/linux" \
    -I./src \
    src/minis_jni.c \
    src/cache/minis.c \
    src/cache/entry.c \
    src/cache/t_string.c \
    src/cache/t_hash.c \
    src/cache/t_zset.c \
    src/cache/hashtable.c \
    src/cache/heap.c \
    src/cache/thread_pool.c \
    src/cache/deque.c \
    src/cache/persistence.c \
    src/io/buffer.c \
    src/common/common.c \
    -o libminis.so -lpthread

javac -d build/classes src/com/minis/*
# javac -d build/classes src/com/minis/Minis.java src/com/minis/Benchmark.java
java --enable-native-access=ALL-UNNAMED -Djava.library.path=. -cp build/classes com.minis.Benchmark
