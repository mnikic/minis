#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "cache/cache.h"
#include "cache/zset.h"
#include "cache/hash.h"   // <--- Added for hash_set/hash_lookup
#include "cache/persistence.h"
#include "common/common.h"

#define GREETING_KEY "greeting"
#define LEADERBOARD_KEY "leaderboard"
#define USER_HASH_KEY "user:100"  // <--- Added Key
#define TEMP_KEY "temp"
#define LONG_EXPIRED_KEY "long expired"
#define RECENTLY_EXPIRED_KEY "recently expired"

// --- Helpers for Test Verification ---

// Helper to look up an entry by key string
static Entry *
find_entry (Cache *cache, const char *key)
{
  HMIter iter;
  hm_iter_init (&cache->db, &iter);
  HNode *node;
  while ((node = hm_iter_next (&iter)) != NULL)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Entry *ent = container_of (node, Entry, node);
#pragma GCC diagnostic pop
      if (strcmp (ent->key, key) == 0)
    {
      return ent;
    }
    }
  return NULL;
}

int
main (void)
{
  const char *FILENAME = "test_dump.rdb";
  uint64_t now_us = 123450000;
  uint64_t in_between_us = 123452500;
  uint64_t new_now_us = 123455000;
  msgf ("=== 1. Initialization ===");
  // Ensure clean state
  remove (FILENAME);

  Cache *cache1 = cache_init ();

  msgf ("=== 2. Populating Cache ===");

  // A. Add a String
  msgf ("   -> Adding String 'greeting' = 'Hello Persistence'");
  Entry *ent1 = entry_new_str (cache1, GREETING_KEY, "Hello Persistence");
  (void) ent1;

  // B. Add a ZSet
  msgf ("   -> Adding ZSet 'leaderboard' with 3 entries");
  Entry *ent2 = entry_new_zset (cache1, LEADERBOARD_KEY);
  zset_add (ent2->zset, "Alice", 5, 100.5);
  zset_add (ent2->zset, "Bob", 3, 50.0);
  zset_add (ent2->zset, "Charlie", 7, 75.0);

  // C. Add a Hash (NEW)
  msgf ("   -> Adding Hash 'user:100' with 2 fields");
  Entry *ent_hash = entry_new_hash (cache1, USER_HASH_KEY);
  hash_set (ent_hash->hash, "username", "jdoe");
  hash_set (ent_hash->hash, "role", "admin");

  // D. Add Expiration
  msgf ("   -> Adding Expiring Key 'temp' (TTL 5000ms)");
  Entry *ent3 = entry_new_str (cache1, TEMP_KEY, "I will survive");
  ent3->expire_at_us = 123456789;    // Arbitrary future timestamp

  msg ("   -> Adding Long Expired Key 'long expired'");
  Entry *ent4 =
    entry_new_str (cache1, LONG_EXPIRED_KEY, "I won't get resurected.");
  ent4->expire_at_us = 1000;

  msg ("   -> Adding Recently Expired Key 'recently expired'");
  Entry *ent5 = entry_new_str (cache1, RECENTLY_EXPIRED_KEY,
                   "I won't get resurected either.");
  ent5->expire_at_us = in_between_us;

  msgf ("=== 3. Saving to Disk ===");
  if (!cache_save_to_file (cache1, FILENAME, now_us))
    {
      msgf ("Save failed!");
      exit (1);
    }
  msgf ("   -> Save successful.");

  cache_free (cache1);

  msgf ("=== 4. Loading from Disk (New Cache) ===");
  Cache *cache2 = cache_init ();
  if (!cache_load_from_file (cache2, FILENAME, new_now_us))
    {
      msg ("Load failed!");
      exit (1);
    }
  msgf ("   -> Load successful.");

  msgf ("=== 5. Verification ===");

  // Verify String
  Entry *res1 = find_entry (cache2, GREETING_KEY);
  (void) res1;
  assert (res1 != NULL);
  assert (res1->type == T_STR);
  assert (strcmp (res1->val, "Hello Persistence") == 0);
  msgf ("   [PASS] String content matches.");

  // Verify ZSet
  Entry *res2 = find_entry (cache2, LEADERBOARD_KEY);
  assert (res2 != NULL);
  assert (res2->type == T_ZSET);
  assert (res2->zset != NULL);

  // 1. Verify HMap Lookups (zset_lookup)
  ZNode *z_alice = zset_lookup (res2->zset, "Alice", 5);
  (void) z_alice;
  assert (z_alice != NULL);
  assert (fabs (z_alice->score - 100.5) < 0.001);

  ZNode *z_bob = zset_lookup (res2->zset, "Bob", 3);
  (void) z_bob;
  assert (z_bob != NULL);
  assert (fabs (z_bob->score - 50.0) < 0.001);

  msgf ("   [PASS] ZSet dictionary lookups match.");

  // 2. Verify Tree Queries (zset_query)
  //    Arguments: (zset, score, name, len)

  // Case A: Exact Match
  ZNode *q_alice = zset_query (res2->zset, 100.5, "Alice", 5);
  (void) q_alice;
  assert (q_alice != NULL);
  assert (q_alice->len == 5);
  assert (memcmp (q_alice->name, "Alice", 5) == 0);

  // Case B: Wrong Score (Should fail because tree is sorted by score)
  ZNode *q_wrong_score = zset_query (res2->zset, 101.0, "Alice", 5);
  (void) q_wrong_score;
  assert (q_wrong_score == NULL);

  // Case C: Non-existent Name
  ZNode *q_unknown = zset_query (res2->zset, 100.5, "Nobody", 6);
  (void) q_unknown;
  assert (q_unknown == NULL);

  msgf ("   [PASS] ZSet tree queries (zset_query) match.");

  // Verify Hash (NEW)
  Entry *res_hash = find_entry (cache2, USER_HASH_KEY);
  assert (res_hash != NULL);
  assert (res_hash->type == T_HASH);
  assert (res_hash->hash != NULL);

  HashEntry *h_user = hash_lookup (res_hash->hash, "username");
  (void) h_user;
  assert (h_user != NULL);
  assert (strcmp (h_user->value, "jdoe") == 0);

  HashEntry *h_role = hash_lookup (res_hash->hash, "role");
  (void) h_role;
  assert (h_role != NULL);
  assert (strcmp (h_role->value, "admin") == 0);

  HashEntry *h_missing = hash_lookup (res_hash->hash, "missing");
  (void) h_missing;
  assert (h_missing == NULL);
  
  msgf ("   [PASS] Hash fields and values match.");

  // Verify Expiration
  Entry *res3 = find_entry (cache2, TEMP_KEY);
  (void) res3;
  assert (res3 != NULL);
  assert (res3->expire_at_us == 123456789);
  assert (res3->heap_idx != (size_t) -1);
  msgf ("   [PASS] Expiration timestamp matches.");

  Entry *res4 = find_entry (cache2, LONG_EXPIRED_KEY);
  (void) res4;
  assert (res4 == NULL);

  Entry *res5 = find_entry (cache2, RECENTLY_EXPIRED_KEY);
  (void) res5;
  assert (res5 == NULL);

  // TAMPERING TEST
  msgf ("=== Tampering Test ===");
  FILE *file = fopen (FILENAME, "r+b");
  assert (file);

  // Skip Header (Magic 4 + CRC 4 + Ver 4 = 12 bytes)
  // Flip a bit in the body (Byte 13)
  fseek (file, 13, SEEK_SET);
  uint8_t byte;
  size_t ret = fread (&byte, 1, 1, file);
  (void) ret;
  byte ^= 0xFF;            // Invert byte
  fseek (file, 13, SEEK_SET);
  fwrite (&byte, 1, 1, file);
  fclose (file);

  msgf ("   -> File tampered. Attempting load...");
  Cache *cache_bad = cache_init ();
  if (cache_load_from_file (cache_bad, FILENAME, now_us))
    {
      msg ("   [FAIL] Loader accepted corrupted file!");
      exit (1);
    }
  else
    {
      msgf ("   [PASS] Loader correctly rejected corrupted file.");
    }
  cache_free (cache_bad);
  msgf ("=== ALL TESTS PASSED ===");

  cache_free (cache2);
  remove (FILENAME);
  return 0;
}
