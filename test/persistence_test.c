#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "cache/cache.h"
#include "cache/zset.h"
#include "cache/persistence.h"
#include "common/common.h"

// --- Helpers for Test Verification ---

// Helper to look up an entry by key string
static Entry *
find_entry(Cache *cache, const char *key)
{
    
    HMIter iter;
    hm_iter_init(&cache->db, &iter);
    HNode *node;
    while ((node = hm_iter_next(&iter)) != NULL) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
        Entry *ent = container_of(node, Entry, node);
#pragma GCC diagnostic pop
        if (strcmp(ent->key, key) == 0) {
            return ent;
        }
    }
    return NULL;
}

int main(void) {
    const char *FILENAME = "test_dump.rdb";
    uint64_t now_us = 1;
    printf("=== 1. Initialization ===\n");
    // Ensure clean state
    remove(FILENAME);
    
    Cache *cache1 = cache_init();
    
    printf("=== 2. Populating Cache ===\n");
    
    // A. Add a String
    printf("   -> Adding String 'greeting' = 'Hello Persistence'\n");
    Entry *ent1 = entry_new_str(cache1, "greeting", strdup("Hello Persistence"));
    (void) ent1; 

    // B. Add a ZSet
    printf("   -> Adding ZSet 'leaderboard' with 3 entries\n");
    Entry *ent2 = entry_new_zset(cache1, "leaderboard");
    zset_add(ent2->zset, "Alice", 5, 100.5);
    zset_add(ent2->zset, "Bob", 3, 50.0);
    zset_add(ent2->zset, "Charlie", 7, 75.0);

    // C. Add Expiration
    printf("   -> Adding Expiring Key 'temp' (TTL 5000ms)\n");
    Entry *ent3 = entry_new_str(cache1, "temp", strdup("I will survive"));
    ent3->expire_at_us = 123456789; // Arbitrary future timestamp

    printf("=== 3. Saving to Disk ===\n");
    if (!cache_save_to_file(cache1, FILENAME, now_us)) {
        msgf ("Save failed!\n");
        exit(1);
    }
    printf("   -> Save successful.\n");

    cache_free(cache1);

    printf("=== 4. Loading from Disk (New Cache) ===\n");
    Cache *cache2 = cache_init();
    if (!cache_load_from_file(cache2, FILENAME, now_us)) {
        msg("Load failed!\n");
        exit(1);
    }
    printf("   -> Load successful.\n");

    printf("=== 5. Verification ===\n");

    // Verify String
    Entry *res1 = find_entry(cache2, "greeting");
    (void) res1;
    assert(res1 != NULL);
    assert(res1->type == T_STR);
    assert(strcmp(res1->val, "Hello Persistence") == 0);
    printf("   [PASS] String content matches.\n");

    // Verify ZSet
    Entry *res2 = find_entry(cache2, "leaderboard");
    assert(res2 != NULL);
    assert(res2->type == T_ZSET);
    assert(res2->zset != NULL);

    // 1. Verify HMap Lookups (zset_lookup)
    ZNode *z_alice = zset_lookup(res2->zset, "Alice", 5);
    (void) z_alice;
    assert(z_alice != NULL);
    assert(fabs(z_alice->score - 100.5) < 0.001);
    
    ZNode *z_bob = zset_lookup(res2->zset, "Bob", 3);
    (void) z_bob;
    assert(z_bob != NULL);
    assert(fabs(z_bob->score - 50.0) < 0.001);
    
    printf("   [PASS] ZSet dictionary lookups match.\n");

    // 2. Verify Tree Queries (zset_query)
    //    Arguments: (zset, score, name, len)
    
    // Case A: Exact Match
    ZNode *q_alice = zset_query(res2->zset, 100.5, "Alice", 5);
    (void) q_alice;
    assert(q_alice != NULL);
    assert(q_alice->len == 5);
    assert(memcmp(q_alice->name, "Alice", 5) == 0);

    // Case B: Wrong Score (Should fail because tree is sorted by score)
    ZNode *q_wrong_score = zset_query(res2->zset, 101.0, "Alice", 5);
    (void) q_wrong_score;
    assert(q_wrong_score == NULL);

    // Case C: Non-existent Name
    ZNode *q_unknown = zset_query(res2->zset, 100.5, "Nobody", 6);
    (void)q_unknown;
    assert(q_unknown == NULL);

    printf("   [PASS] ZSet tree queries (zset_query) match.\n");

    // Verify Expiration
    Entry *res3 = find_entry(cache2, "temp");
    (void) res3;
    assert(res3 != NULL);
    assert(res3->expire_at_us == 123456789);
    printf("   [PASS] Expiration timestamp matches.\n");
    // TAMPERING TEST
    printf("=== Tampering Test ===\n");
    FILE *file = fopen(FILENAME, "r+b");
    assert(file);
    
    // Skip Header (Magic 4 + CRC 4 + Ver 4 = 12 bytes)
    // Flip a bit in the body (Byte 13)
    fseek(file, 13, SEEK_SET);
    uint8_t byte;
    size_t ret = fread(&byte, 1, 1, file);
    (void) ret;
    byte ^= 0xFF; // Invert byte
    fseek(file, 13, SEEK_SET);
    fwrite(&byte, 1, 1, file);
    fclose(file);

    printf("   -> File tampered. Attempting load...\n");
    Cache *cache_bad = cache_init();
    if (cache_load_from_file(cache_bad, FILENAME, now_us)) {
        msg("   [FAIL] Loader accepted corrupted file!\n");
        exit(1);
    } else {
        printf("   [PASS] Loader correctly rejected corrupted file.\n");
    }
    cache_free(cache_bad);
    printf("\n=== ALL TESTS PASSED ===\n");

    cache_free(cache2);
    remove(FILENAME);
    return 0;
}
