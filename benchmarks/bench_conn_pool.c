/*
 * bench_conn_pool.c
 * * Compile: gcc -O3 -march=native -o bench bench_conn_pool.c conn_pool.c connection.c common.c -I.
 * Run: ./bench
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include <assert.h>

#include "io/conn_pool.h"
#include "io/connection.h"

// ============================================================================
// Timing & CPU Cycle Utilities
// ============================================================================

static inline uint64_t
get_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline uint64_t 
rdtsc(void) 
{
    uint32_t lo, hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}

// Variables implicitly used by macros must be declared in the function scope
#define BENCH_VARS \
    uint64_t _start_ns, _start_cy

#define BENCH_START() do { \
    _start_ns = get_nanos(); \
    _start_cy = rdtsc(); \
} while(0)

#define BENCH_END(name, ops) do { \
    uint64_t _end_ns = get_nanos(); \
    uint64_t _end_cy = rdtsc(); \
    uint64_t _el_ns = _end_ns - _start_ns; \
    uint64_t _el_cy = _end_cy - _start_cy; \
    double _ns_per_op = (double)_el_ns / (double)(ops); \
    double _cy_per_op = (double)_el_cy / (double)(ops); \
    printf("%-35s: %8.3f ms | %7.1f ns/op | %7.0f cyc/op\n", \
           name, _el_ns / 1000000.0, _ns_per_op, _cy_per_op); \
} while(0)

// Volatile sink to prevent compiler optimizing away read loops
volatile uint64_t g_sink;

// ============================================================================
// Benchmark 1: Sequential allocation and release
// ============================================================================

static void
bench_sequential_alloc_release(uint32_t n_conns)
{
    BENCH_VARS; // Declare timer variables
    
    printf("\n=== Benchmark 1: Sequential Allocation & Release ===\n");
    
    ConnPool *pool = connpool_new(n_conns);
    
    // 1. Allocate
    BENCH_START();
    for (uint32_t i = 1; i <= n_conns; i++) {
        connpool_get(pool, (int)i);
    }
    // Ops = n_conns (we did N allocations)
    BENCH_END("Allocate all connections", n_conns);
    
    // 2. Lookup
    BENCH_START();
    for (uint32_t i = 1; i <= n_conns; i++) {
        Conn *conn = connpool_lookup(pool, (int)i);
        if (conn) conn->rbuf_size++;
    }
    // Ops = n_conns (we did N lookups)
    BENCH_END("Lookup all connections", n_conns);
    
    // 3. Iterate
    BENCH_START();
    Conn **active_list;
    size_t count;
    connpool_iter(pool, &active_list, &count);
    for (size_t i = 0; i < count; i++) {
        Conn *conn = active_list[i];
        conn->state++;
    }
    // Ops = count (we iterated N items)
    BENCH_END("Iterate all connections", count);
    
    // 4. Release
    BENCH_START();
    for (uint32_t i = 1; i <= n_conns; i++) {
        Conn *conn = connpool_lookup(pool, (int)i);
        connpool_release(pool, conn);
    }
    // Ops = n_conns
    BENCH_END("Release all connections", n_conns);
    
    connpool_free(pool);
}

// ============================================================================
// Benchmark 2: Interleaved allocation (Churn)
// ============================================================================

static void
bench_interleaved_churn(uint32_t n_conns)
{
    BENCH_VARS;
    printf("\n=== Benchmark 2: Interleaved Churn ===\n");
    
    ConnPool *pool = connpool_new(n_conns);
    uint32_t initial_alloc = (n_conns * 7) / 10;
    uint32_t churn_count = (n_conns * 3) / 10;
    
    // Setup
    for (uint32_t i = 1; i <= initial_alloc; i++) {
        connpool_get(pool, (int)i);
    }
    
    BENCH_START();
    
    int cycles = 5;
    for (int cycle = 0; cycle < cycles; cycle++) {
        // Release ~1/3rd
        for (uint32_t i = 3; i <= initial_alloc; i += 3) {
            Conn *conn = connpool_lookup(pool, (int)i);
            if (conn) connpool_release(pool, conn);
        }
        
        // Re-allocate
        uint32_t fd_base = initial_alloc + 1 + (cycle * churn_count);
        for (uint32_t i = 0; i < churn_count; i++) {
            connpool_get(pool, (int)(fd_base + i));
        }
    }
    
    // Ops = roughly (churn_count * 2 operations) * cycles
    // This is an estimate, but gives us a "per churn event" metric
    BENCH_END("Full churn cycle (5 rounds)", churn_count * 2 * cycles);
    
    connpool_free(pool);
}

// ============================================================================
// Benchmark 3: Hot path - repeated lookups
// ============================================================================

static void
bench_hot_lookup_path(uint32_t n_conns)
{
    BENCH_VARS;
    printf("\n=== Benchmark 3: Hot Lookup Path (Epoll Simulation) ===\n");
    
    ConnPool *pool = connpool_new(n_conns);
    for (uint32_t i = 1; i <= n_conns; i++) {
        connpool_get(pool, (int)i);
    }
    
    uint32_t n_events = 100000;
    
    BENCH_START();
    for (uint32_t i = 0; i < n_events; i++) {
        // Biased random lookup
        int fd = 1 + (int)((i * 7919) % n_conns);
        Conn *conn = connpool_lookup(pool, fd);
        if (conn) conn->state++;
    }
    // Ops = n_events (100k individual lookups)
    BENCH_END("Process 100k epoll events", n_events);
    
    connpool_free(pool);
}

// ============================================================================
// Benchmark 4: Cache thrashing
// ============================================================================

static void
bench_cache_thrashing(uint32_t n_conns)
{
    BENCH_VARS;
    printf("\n=== Benchmark 4: Cache Thrashing ===\n");
    
    ConnPool *pool = connpool_new(n_conns);
    for (uint32_t i = 1; i <= n_conns; i++) {
        connpool_get(pool, (int)i);
    }
    
    int rounds = 100;

    // 1. Sequential
    BENCH_START();
    for (int r = 0; r < rounds; r++) {
        for (uint32_t i = 1; i <= n_conns; i++) {
            Conn *conn = connpool_lookup(pool, (int)i);
            conn->rbuf_size++;
        }
    }
    BENCH_END("Sequential access", n_conns * rounds);
    
    // 2. Strided (Every 16th)
    uint32_t strided_ops = 0;
    BENCH_START();
    for (int r = 0; r < rounds; r++) {
        for (uint32_t i = 1; i <= n_conns; i += 16) {
            Conn *conn = connpool_lookup(pool, (int)i);
            conn->rbuf_size++;
            strided_ops++;
        }
    }
    // Ops = Actual number of lookups performed
    BENCH_END("Strided access (stride=16)", strided_ops);
    
    // 3. Random
    BENCH_START();
    for (int r = 0; r < rounds; r++) {
        for (uint32_t i = 0; i < n_conns; i++) {
            uint32_t idx = ((i * 7919 + r * 104729) % n_conns) + 1;
            Conn *conn = connpool_lookup(pool, (int)idx);
            conn->rbuf_size++;
        }
    }
    BENCH_END("Random access", n_conns * rounds);
    
    connpool_free(pool);
}

// ============================================================================
// Benchmark 5: Memory bandwidth (CORRECTED)
// ============================================================================

static void
bench_memory_bandwidth(uint32_t n_conns)
{
    BENCH_VARS;
    printf("\n=== Benchmark 5: Memory Bandwidth ===\n");
    
    ConnPool *pool = connpool_new(n_conns);
    for (uint32_t i = 1; i <= n_conns; i++) {
        connpool_get(pool, (int)i);
    }
    
    // 1. Write (memset)
    BENCH_START();
    for (uint32_t i = 1; i <= n_conns; i++) {
        Conn *conn = connpool_lookup(pool, (int)i);
        // FIX: Use K_RBUF_SIZE, not sizeof(pointer)
        memset(conn->rbuf, 1, K_RBUF_SIZE);
    }
    // Ops = n_conns (memset operations)
    BENCH_END("Write bandwidth (memset)", n_conns);
    
    // 2. Read (sum)
    BENCH_START();
    uint64_t sum = 0;
    for (uint32_t i = 1; i <= n_conns; i++) {
        Conn *conn = connpool_lookup(pool, (int)i);
        sum += conn->fd + conn->state + conn->rbuf_size;
    }
    g_sink = sum; // Use volatile sink
    BENCH_END("Read bandwidth (sum fields)", n_conns);
    
    connpool_free(pool);
}

// ============================================================================
// Benchmark 6: Sparse Lookup (NEW)
// ============================================================================

static void
bench_sparse_lookup(uint32_t n_conns)
{
    BENCH_VARS;
    printf("\n=== Benchmark 6: Sparse Lookup (High FDs) ===\n");
    
    ConnPool *pool = connpool_new(n_conns);
    
    // Allocate sparse FDs (e.g., 10, 20, 30...) to force array growth
    for (uint32_t i = 1; i <= n_conns; i++) {
        connpool_get(pool, (int)(i * 10));
    }
    
    uint32_t n_lookups = 100000;
    uint64_t checksum = 0;
    
    BENCH_START();
    for (uint32_t i = 0; i < n_lookups; i++) {
        // Look up valid FDs sparsely distributed
        int target = (1 + (rand() % n_conns)) * 10;
        Conn *conn = connpool_lookup(pool, target);
        if (conn) checksum += conn->fd;
    }
    g_sink = checksum;
    
    // Ops = number of lookup calls
    BENCH_END("Sparse Lookup 100k", n_lookups);
    
    connpool_free(pool);
}

// ============================================================================
// Main
// ============================================================================

int main(void)
{
    printf("Connection Pool Benchmark Suite (Optimized)\n");
    printf("===========================================\n");
    
    uint32_t test_sizes[] = { 1000, 20000 };
    
    for (size_t i = 0; i < sizeof(test_sizes)/sizeof(test_sizes[0]); i++) {
        uint32_t n = test_sizes[i];
        
        printf("\n>>> Testing with %u connections <<<\n", n);
        bench_sequential_alloc_release(n);
        bench_interleaved_churn(n);
        bench_hot_lookup_path(n);
        bench_cache_thrashing(n);
        bench_memory_bandwidth(n);
        bench_sparse_lookup(n);
    }
    
    return 0;
}
