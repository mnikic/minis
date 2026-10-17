// The checks are asserts, keep them in release (-DNDEBUG) builds too
#undef NDEBUG
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <arpa/inet.h>

#include "cache/cache.h"
#include "cache/entry.h"
#include "cache/minis_private.h"
#include "cache/zset.h"
#include "cache/hash.h"		// <--- Added for hash_set/hash_lookup
#include "cache/list.h"
#include "cache/persistence.h"
#include "common/common.h"

#define GREETING_KEY "greeting"
#define LEADERBOARD_KEY "leaderboard"
#define USER_HASH_KEY "user:100"	// <--- Added Key
#define QUEUE_KEY "queue"
#define BIG_QUEUE_KEY "big queue"
#define BIG_QUEUE_LEN 1000
// Entries much bigger than K_MAX_MSG, which snapshots used to skip
#define HUGE_HASH_KEY "huge hash"
#define HUGE_LIST_KEY "huge list"
#define HUGE_COUNT 200
#define HUGE_VALUE_LEN 10000
#define TEMP_KEY "temp"
#define BINARY_KEY "binary"
#define LONG_EXPIRED_KEY "long expired"
#define RECENTLY_EXPIRED_KEY "recently expired"

// --- Helpers for Test Verification ---

// Helper to look up an entry by key string
static Entry *
find_entry (Cache *cache, const char *key)
{
  HMIter iter;
  int shard_id = get_shard_id(key);
  hm_iter_init (&cache->shards[shard_id].db, &iter);
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

// Wall clock the snapshot code sees, so the test can stage a reboot
static uint64_t g_fake_wall_us;

static uint64_t
fake_wall_clock (void)
{
  return g_fake_wall_us;
}

int
main (void)
{
  const char *FILENAME = "test_dump.rdb";
  // Saved at 123.45s of uptime, loaded right after a reboot (5s of
  // uptime) when 5ms of wall time passed. Expiries are monotonic in
  // memory, so they only survive this if the file holds wall time.
  uint64_t now_us = 123450000;
  uint64_t in_between_us = 123452500;
  uint64_t new_now_us = 5000000;
  uint64_t save_wall_us = 1700000000000000ULL;
  uint64_t load_wall_us = save_wall_us + 5000;
  persistence_set_wall_clock (fake_wall_clock);
  msgf ("=== 1. Initialization ===");
  // Ensure clean state
  remove (FILENAME);

  Cache *cache1 = cache_init ();

  msgf ("=== 2. Populating Cache ===");

  // A. Add a String
  msgf ("   -> Adding String 'greeting' = 'Hello Persistence'");
  Entry *ent1 = entry_new_str (cache1, GREETING_KEY, "Hello Persistence", 17);
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

  // D. Add Lists: one built from both ends, and one long enough to
  // wrap and grow the ring buffer, with a TTL
  msgf ("   -> Adding List 'queue' = [c, a, b] and List 'big queue'");
  Entry *ent_list = entry_new_list (cache1, QUEUE_KEY);
  list_push (ent_list->list, "a", false);
  list_push (ent_list->list, "b", false);
  list_push (ent_list->list, "c", true);

  Entry *ent_big = entry_new_list (cache1, BIG_QUEUE_KEY);
  for (int i = 0; i < BIG_QUEUE_LEN; i++)
    {
      char val[32];
      snprintf (val, sizeof (val), "item-%d", i);
      list_push (ent_big->list, val, i % 2 == 0);
    }
  entry_set_expiration (cache1, ent_big, 123456789);

  // A string holding NUL bytes
  entry_new_str (cache1, BINARY_KEY, "a\0b\0", 4);

  // E. Add a hash and a list of ~2MB each
  msgf ("   -> Adding ~2MB Hash 'huge hash' and List 'huge list'");
  static char huge_value[HUGE_VALUE_LEN + 1];
  memset (huge_value, 'h', HUGE_VALUE_LEN);
  Entry *ent_huge_hash = entry_new_hash (cache1, HUGE_HASH_KEY);
  Entry *ent_huge_list = entry_new_list (cache1, HUGE_LIST_KEY);
  for (int i = 0; i < HUGE_COUNT; i++)
    {
      char field[32];
      snprintf (field, sizeof (field), "field-%d", i);
      hash_set (ent_huge_hash->hash, field, huge_value);
      list_push (ent_huge_list->list, huge_value, false);
    }

  // F. Add Expiration
  msgf ("   -> Adding Expiring Key 'temp' (TTL 5000ms)");
  Entry *ent3 = entry_new_str (cache1, TEMP_KEY, "I will survive", 14);
  ent3->expire_at_us = 123456789;	// Arbitrary future timestamp

  msg ("   -> Adding Long Expired Key 'long expired'");
  Entry *ent4 =
    entry_new_str (cache1, LONG_EXPIRED_KEY, "I won't get resurected.", 23);
  ent4->expire_at_us = 1000;

  msg ("   -> Adding Recently Expired Key 'recently expired'");
  Entry *ent5 = entry_new_str (cache1, RECENTLY_EXPIRED_KEY,
			       "I won't get resurected either.", 30);
  ent5->expire_at_us = in_between_us;

  msgf ("=== 3. Saving to Disk ===");
  g_fake_wall_us = save_wall_us;
  if (!cache_save_to_file (cache1, FILENAME, now_us))
    {
      msgf ("Save failed!");
      exit (1);
    }
  msgf ("   -> Save successful.");

  cache_free (cache1);

  msgf ("=== 4. Loading from Disk (New Cache, after a reboot) ===");
  g_fake_wall_us = load_wall_us;
  Cache *cache2 = cache_init ();
  if (!cache_load_from_file (cache2, FILENAME, -1, new_now_us))
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

  Entry *res_bin = find_entry (cache2, BINARY_KEY);
  assert (res_bin != NULL);
  assert (res_bin->val_len == 4);
  assert (memcmp (res_bin->val, "a\0b\0", 4) == 0);
  msgf ("   [PASS] String with NUL bytes matches.");

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

  // Verify Lists
  Entry *res_list = find_entry (cache2, QUEUE_KEY);
  assert (res_list != NULL);
  assert (res_list->type == T_LIST);
  assert (list_len (res_list->list) == 3);
  assert (strcmp (list_at (res_list->list, 0), "c") == 0);
  assert (strcmp (list_at (res_list->list, 1), "a") == 0);
  assert (strcmp (list_at (res_list->list, 2), "b") == 0);

  // Even items went to the front, so the head is the last even one
  Entry *res_big = find_entry (cache2, BIG_QUEUE_KEY);
  assert (res_big != NULL);
  assert (res_big->type == T_LIST);
  assert (list_len (res_big->list) == BIG_QUEUE_LEN);
  assert (strcmp (list_at (res_big->list, 0), "item-998") == 0);
  assert (strcmp (list_at (res_big->list, BIG_QUEUE_LEN / 2), "item-1") == 0);
  assert (strcmp (list_at (res_big->list, BIG_QUEUE_LEN - 1), "item-999") ==
	  0);
  // 6789us were left at the save, 5000 of them passed
  assert (res_big->expire_at_us == new_now_us + 1789);
  assert (res_big->heap_idx != (size_t) -1);
  msgf ("   [PASS] Lists match.");

  // Verify the huge entries
  Entry *res_huge_hash = find_entry (cache2, HUGE_HASH_KEY);
  assert (res_huge_hash != NULL);
  assert (res_huge_hash->type == T_HASH);
  assert (hm_size (res_huge_hash->hash) == HUGE_COUNT);
  HashEntry *h_last = hash_lookup (res_huge_hash->hash, "field-199");
  (void) h_last;
  assert (h_last != NULL);
  assert (strcmp (h_last->value, huge_value) == 0);

  Entry *res_huge_list = find_entry (cache2, HUGE_LIST_KEY);
  assert (res_huge_list != NULL);
  assert (res_huge_list->type == T_LIST);
  assert (list_len (res_huge_list->list) == HUGE_COUNT);
  assert (strcmp (list_at (res_huge_list->list, HUGE_COUNT - 1), huge_value)
	  == 0);
  msgf ("   [PASS] Entries larger than K_MAX_MSG match.");

  // Verify Expiration
  Entry *res3 = find_entry (cache2, TEMP_KEY);
  (void) res3;
  assert (res3 != NULL);
  assert (res3->expire_at_us == new_now_us + 1789);
  assert (res3->heap_idx != (size_t) -1);
  msgf ("   [PASS] Expiration keeps its time left across a reboot.");

  Entry *res4 = find_entry (cache2, LONG_EXPIRED_KEY);
  (void) res4;
  assert (res4 == NULL);

  Entry *res5 = find_entry (cache2, RECENTLY_EXPIRED_KEY);
  (void) res5;
  assert (res5 == NULL);

  // Version 1 files, with monotonic expiries, still load as they did
  msgf ("=== Version 1 Test ===");
  {
    FILE *v1 = fopen (FILENAME, "r+b");
    assert (v1);
    uint32_t ver_net = htonl (1);
    fseek (v1, 8, SEEK_SET);	// Magic 4 + CRC 4
    size_t wrote = fwrite (&ver_net, 4, 1, v1);
    (void) wrote;
    fclose (v1);

    Cache *cache_v1 = cache_init ();
    assert (cache_load_from_file (cache_v1, FILENAME, -1, new_now_us));
    Entry *v1_temp = find_entry (cache_v1, TEMP_KEY);
    (void) v1_temp;
    assert (v1_temp != NULL);
    // Taken as monotonic as it stands
    assert (v1_temp->expire_at_us == save_wall_us + 6789);
    cache_free (cache_v1);
    msgf ("   [PASS] Version 1 snapshot loads.");
  }

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
  byte ^= 0xFF;			// Invert byte
  fseek (file, 23, SEEK_SET);
  fwrite (&byte, 1, 1, file);
  fclose (file);

  msgf ("   -> File tampered. Attempting load...");
  Cache *cache_bad = cache_init ();
  if (cache_load_from_file (cache_bad, FILENAME, -1, now_us))
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
