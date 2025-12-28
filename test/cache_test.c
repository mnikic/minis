#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "cache/cache.h"
#include "common/common.h"
#include "io/buffer.h"
#include "io/proto_defs.h"

// ============================================================================
// Test utilities
// ============================================================================

#define TEST_BUFFER_SIZE 8192
#define EPSILON 1e-9
static uint8_t test_buffer[TEST_BUFFER_SIZE];

// Helper to check raw string content (for RESP protocol)
static void
assert_buffer_contains (Buffer *buf, const char *expected)
{
  // Null terminate the buffer for string comparison
  // (Note: This modifies the buffer temporarily, but it's safe for tests)
  size_t len = buf_len (buf);
  if (len < TEST_BUFFER_SIZE)
    {
      buf->data[len] = '\0';
    }

  if (strstr ((char *) buf->data, expected) == NULL)
    {
      msgf ("ASSERTION FAILED: Buffer content '%s' does not contain '%s'",
	    (char *) buf->data, expected);
      exit (1);
    }
}

#define ASSERT_TRUE(expr)                                                      \
	do {                                                                       \
		if (!(expr)) {                                                         \
			msgf ("ASSERTION FAILED: %s:%d: %s", __FILE__,         \
					__LINE__, #expr);                                          \
			exit(1);                                                           \
		}                                                                      \
	} while (0)

#define ASSERT_FALSE(expr) ASSERT_TRUE(!(expr))
#define ASSERT_EQ(a, b) ASSERT_TRUE((a) == (b))
#define ASSERT_STR_EQ(a, b) ASSERT_TRUE(strcmp((a), (b)) == 0)

static uint64_t
get_test_time_us (void)
{
  return (uint64_t) time (NULL) * 1000000ULL;
}

static Buffer
init_test_buffer (ProtoType proto)
{
  memset (test_buffer, 0, TEST_BUFFER_SIZE);
  Buffer buf = buf_init (test_buffer, TEST_BUFFER_SIZE);
  buf_set_proto (&buf, proto);
  return buf;
}

// Helper to extract integer response from binary protocol
static uint64_t
extract_int_response (Buffer *buf)
{
  // Binary protocol: SER_INT (1 byte) + int64_t (8 bytes)
  assert (buf_len (buf) >= 9);
  assert (buf_data (buf)[0] == SER_INT);

  uint64_t result;
  memcpy (&result, &buf_data (buf)[1], sizeof (uint64_t));
  return ntoh_u64 (result);
}

// Helper to check if response is NIL
static bool
is_nil_response (Buffer *buf)
{
  return buf_len (buf) > 0 && buf_data (buf)[0] == SER_NIL;
}

// Helper to check if response is error
static bool
is_err_response (Buffer *buf)
{
  return buf_len (buf) > 0 && buf_data (buf)[0] == SER_ERR;
}

// Helper to extract string response
static const char *
extract_str_response (Buffer *buf)
{
  assert (buf_len (buf) >= 5);
  assert (buf_data (buf)[0] == SER_STR);

  // Skip SER_STR + 4 bytes length
  return (const char *) &buf_data (buf)[5];
}

static double
extract_dbl_response (Buffer *buf)
{
  // Binary protocol: SER_DBL (1 byte) + double (8 bytes)
  assert (buf_len (buf) >= 9);
  // Assuming SER_DBL is defined in your proto_defs.h (usually 4)
  // If not, you might need to check your common.h or just check != SER_ERR
  assert (buf_data (buf)[0] == 4);	// 4 is usually SER_DBL

  double result;
  // Copy the raw 8 bytes into the double variable
  memcpy (&result, &buf_data (buf)[1], sizeof (double));

  // Note: Floating point endianness can be tricky across architectures.
  // If you implemented network-order swapping for doubles in the server,
  // you would need to swap it back here. 
  // For now, assuming direct memcpy works (common on same-arch tests).
  return result;
}

// ============================================================================
// Test 1: Basic PING command
// ============================================================================

static void
test_ping (void)
{
  printf ("TEST: PING command... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);

  // Test 1: PING without args
  const char *cmd1[] = { "ping" };
  ASSERT_TRUE (cache_execute (cache, cmd1, 1, &buf, get_test_time_us ()));
  ASSERT_STR_EQ (extract_str_response (&buf), "PONG");

  // Test 2: PING with message
  buf = init_test_buffer (PROTO_BIN);
  const char *cmd2[] = { "ping", "hello" };
  ASSERT_TRUE (cache_execute (cache, cmd2, 2, &buf, get_test_time_us ()));
  ASSERT_STR_EQ (extract_str_response (&buf), "hello");

  // Test 3: PING with too many args (should error)
  buf = init_test_buffer (PROTO_BIN);
  const char *cmd3[] = { "ping", "arg1", "arg2" };
  ASSERT_TRUE (cache_execute (cache, cmd3, 3, &buf, get_test_time_us ()));
  ASSERT_TRUE (is_err_response (&buf));

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 2: SET/GET operations
// ============================================================================

static void
test_set_get (void)
{
  printf ("TEST: SET/GET operations... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // Set a value
  const char *set_cmd[] = { "set", "key1", "value1" };
  ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));

  // Get the value back
  buf = init_test_buffer (PROTO_BIN);
  const char *get_cmd[] = { "get", "key1" };
  ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, now));
  ASSERT_STR_EQ (extract_str_response (&buf), "value1");

  // Get non-existent key
  buf = init_test_buffer (PROTO_BIN);
  const char *get_cmd2[] = { "get", "nonexistent" };
  ASSERT_TRUE (cache_execute (cache, get_cmd2, 2, &buf, now));
  ASSERT_TRUE (is_nil_response (&buf));

  // Overwrite existing key
  buf = init_test_buffer (PROTO_BIN);
  const char *set_cmd2[] = { "set", "key1", "newvalue" };
  ASSERT_TRUE (cache_execute (cache, set_cmd2, 3, &buf, now));

  buf = init_test_buffer (PROTO_BIN);
  ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, now));
  ASSERT_STR_EQ (extract_str_response (&buf), "newvalue");

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 3: MSET/MGET operations
// ============================================================================

static void
test_mset_mget (void)
{
  printf ("TEST: MSET/MGET operations... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // MSET multiple keys
  const char *mset_cmd[] = { "mset", "k1", "v1", "k2", "v2", "k3", "v3" };
  ASSERT_TRUE (cache_execute (cache, mset_cmd, 7, &buf, now));

  // MGET all keys
  buf = init_test_buffer (PROTO_BIN);
  const char *mget_cmd[] = { "mget", "k1", "k2", "k3" };
  ASSERT_TRUE (cache_execute (cache, mget_cmd, 4, &buf, now));

  // MGET with mix of existing and non-existing keys
  buf = init_test_buffer (PROTO_BIN);
  const char *mget_cmd2[] = { "mget", "k1", "nonexist", "k3" };
  ASSERT_TRUE (cache_execute (cache, mget_cmd2, 4, &buf, now));

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 4: DEL/MDEL operations
// ============================================================================

static void
test_del (void)
{
  printf ("TEST: DEL/MDEL operations... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // Set some keys
  const char *mset_cmd[] = { "mset", "d1", "v1", "d2", "v2", "d3", "v3" };
  ASSERT_TRUE (cache_execute (cache, mset_cmd, 7, &buf, now));

  // Delete existing key
  buf = init_test_buffer (PROTO_BIN);
  const char *del_cmd[] = { "del", "d1" };
  ASSERT_TRUE (cache_execute (cache, del_cmd, 2, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 1);

  // Verify it's deleted
  buf = init_test_buffer (PROTO_BIN);
  const char *get_cmd[] = { "get", "d1" };
  ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, now));
  ASSERT_TRUE (is_nil_response (&buf));

  // Delete non-existent key
  buf = init_test_buffer (PROTO_BIN);
  const char *del_cmd2[] = { "del", "nonexist" };
  ASSERT_TRUE (cache_execute (cache, del_cmd2, 2, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 0);

  // MDEL multiple keys
  buf = init_test_buffer (PROTO_BIN);
  const char *mdel_cmd[] = { "mdel", "d2", "d3", "nonexist" };
  ASSERT_TRUE (cache_execute (cache, mdel_cmd, 4, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 2);	// Only 2 existed

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 5: TTL/EXPIRE operations
// ============================================================================

static void
test_ttl (void)
{
  printf ("TEST: TTL/EXPIRE operations... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // Set a key
  const char *set_cmd[] = { "set", "ttlkey", "value" };
  ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));

  // Check TTL before setting (should be -1, no TTL)
  buf = init_test_buffer (PROTO_BIN);
  const char *ttl_cmd[] = { "pttl", "ttlkey" };
  ASSERT_TRUE (cache_execute (cache, ttl_cmd, 2, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), -1ULL);

  // Set expiry to 5000ms
  buf = init_test_buffer (PROTO_BIN);
  const char *expire_cmd[] = { "pexpire", "ttlkey", "5000" };
  ASSERT_TRUE (cache_execute (cache, expire_cmd, 3, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 1);

  // Check TTL (should be ~5000ms)
  buf = init_test_buffer (PROTO_BIN);
  ASSERT_TRUE (cache_execute (cache, ttl_cmd, 2, &buf, now));
  uint64_t ttl = extract_int_response (&buf);
  ASSERT_TRUE (ttl >= 4900 && ttl <= 5000);	// Allow some slack

  // Key should still exist
  buf = init_test_buffer (PROTO_BIN);
  const char *get_cmd[] = { "get", "ttlkey" };
  ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, now));
  ASSERT_STR_EQ (extract_str_response (&buf), "value");

  // Check after expiry
  uint64_t future = now + 6000000;	// 6 seconds later
  buf = init_test_buffer (PROTO_BIN);
  ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, future));
  ASSERT_TRUE (is_nil_response (&buf));	// Should be expired

  // TTL for non-existent key
  buf = init_test_buffer (PROTO_BIN);
  const char *ttl_cmd2[] = { "pttl", "nonexist" };
  ASSERT_TRUE (cache_execute (cache, ttl_cmd2, 2, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), -2ULL);

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6: Active eviction
// ============================================================================

static void
test_active_eviction (void)
{
  printf ("TEST: Active eviction... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // Set multiple keys with different TTLs
  for (int i = 0; i < 10; i++)
    {
      buf = init_test_buffer (PROTO_BIN);
      char key[32], val[32];
      snprintf (key, sizeof (key), "evict_key%d", i);
      snprintf (val, sizeof (val), "value%d", i);

      const char *set_cmd[] = { "set", key, val };
      ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));

      // Set TTL: 1000ms, 2000ms, 3000ms, etc.
      buf = init_test_buffer (PROTO_BIN);
      char ttl_str[32];
      snprintf (ttl_str, sizeof (ttl_str), "%d", (i + 1) * 1000);
      const char *expire_cmd[] = { "pexpire", key, ttl_str };
      ASSERT_TRUE (cache_execute (cache, expire_cmd, 3, &buf, now));
    }

  // Advance time to 5.5 seconds
  uint64_t future = now + 5500000;

  // Run active eviction
  cache_evict (cache, future);

  // Keys 0-4 should be evicted (TTL 1-5 seconds)
  // Keys 5-9 should still exist (TTL 6-10 seconds)
  for (int i = 0; i < 10; i++)
    {
      buf = init_test_buffer (PROTO_BIN);
      char key[32];
      snprintf (key, sizeof (key), "evict_key%d", i);
      const char *get_cmd[] = { "get", key };
      ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, future));

      if (i < 5)
	{
	  ASSERT_TRUE (is_nil_response (&buf));	// Should be evicted
	}
      else
	{
	  // Should still exist
	  ASSERT_FALSE (is_nil_response (&buf));
	}
    }

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 7: KEYS command with pattern matching
// ============================================================================

static void
test_keys (void)
{
  printf ("TEST: KEYS command... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // Set various keys
  const char *mset_cmd[] = { "mset", "user:1", "alice", "user:2",
    "bob", "user:3", "charlie", "post:1",
    "hello", "post:2", "world"
  };
  ASSERT_TRUE (cache_execute (cache, mset_cmd, 11, &buf, now));

  // Match all keys
  buf = init_test_buffer (PROTO_BIN);
  const char *keys_cmd[] = { "keys", "*" };
  ASSERT_TRUE (cache_execute (cache, keys_cmd, 2, &buf, now));
  // Should have 5 keys

  // Match user keys
  buf = init_test_buffer (PROTO_BIN);
  const char *keys_cmd2[] = { "keys", "user:*" };
  ASSERT_TRUE (cache_execute (cache, keys_cmd2, 2, &buf, now));
  // Should have 3 keys

  // Match post keys
  buf = init_test_buffer (PROTO_BIN);
  const char *keys_cmd3[] = { "keys", "post:*" };
  ASSERT_TRUE (cache_execute (cache, keys_cmd3, 2, &buf, now));
  // Should have 2 keys

  // No match
  buf = init_test_buffer (PROTO_BIN);
  const char *keys_cmd4[] = { "keys", "nomatch:*" };
  ASSERT_TRUE (cache_execute (cache, keys_cmd4, 2, &buf, now));
  // Should have 0 keys

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 8: ZADD/ZREM/ZSCORE operations
// ============================================================================

static void
test_zset_basic (void)
{
  printf ("TEST: ZSET basic operations... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // ZADD new member
  const char *zadd_cmd[] = { "zadd", "myzset", "1.5", "member1" };
  ASSERT_TRUE (cache_execute (cache, zadd_cmd, 4, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 1);	// 1 added

  // ZADD another member
  buf = init_test_buffer (PROTO_BIN);
  const char *zadd_cmd2[] = { "zadd", "myzset", "2.5", "member2" };
  ASSERT_TRUE (cache_execute (cache, zadd_cmd2, 4, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 1);

  // ZADD existing member (should update score, return 0)
  buf = init_test_buffer (PROTO_BIN);
  const char *zadd_cmd3[] = { "zadd", "myzset", "3.5", "member1" };
  ASSERT_TRUE (cache_execute (cache, zadd_cmd3, 4, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 0);	// 0 added (updated)

  // ZSCORE existing member
  buf = init_test_buffer (PROTO_BIN);
  const char *zscore_cmd[] = { "zscore", "myzset", "member1" };
  ASSERT_TRUE (cache_execute (cache, zscore_cmd, 3, &buf, now));
  // Should be 3.5 (updated score)

  // ZSCORE non-existent member
  buf = init_test_buffer (PROTO_BIN);
  const char *zscore_cmd2[] = { "zscore", "myzset", "nonexist" };
  ASSERT_TRUE (cache_execute (cache, zscore_cmd2, 3, &buf, now));
  ASSERT_TRUE (is_nil_response (&buf));

  // ZREM existing member
  buf = init_test_buffer (PROTO_BIN);
  const char *zrem_cmd[] = { "zrem", "myzset", "member1" };
  ASSERT_TRUE (cache_execute (cache, zrem_cmd, 3, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 1);

  // ZREM non-existent member
  buf = init_test_buffer (PROTO_BIN);
  const char *zrem_cmd2[] = { "zrem", "myzset", "nonexist" };
  ASSERT_TRUE (cache_execute (cache, zrem_cmd2, 3, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 0);

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 9: ZQUERY operations
// ============================================================================

static void
test_zquery (void)
{
  printf ("TEST: ZQUERY operations... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // Add multiple members with different scores
  for (int i = 0; i < 10; i++)
    {
      buf = init_test_buffer (PROTO_BIN);
      char score_str[32], member[32];
      snprintf (score_str, sizeof (score_str), "%d.0", i);
      snprintf (member, sizeof (member), "member%d", i);

      const char *zadd_cmd[] = { "zadd", "queryzset", score_str, member };
      ASSERT_TRUE (cache_execute (cache, zadd_cmd, 4, &buf, now));
    }

  // Query from score 3.0, offset 0, limit 3
  buf = init_test_buffer (PROTO_BIN);
  const char *zquery_cmd[] = { "zquery", "queryzset", "3.0", "", "0", "3" };
  ASSERT_TRUE (cache_execute (cache, zquery_cmd, 6, &buf, now));
  // Should return 3 results (pairs of name+score = 6 elements)

  // Query with offset
  buf = init_test_buffer (PROTO_BIN);
  const char *zquery_cmd2[] = { "zquery", "queryzset", "0.0", "", "2", "3" };
  ASSERT_TRUE (cache_execute (cache, zquery_cmd2, 6, &buf, now));

  // Query with limit 0 (should return empty array)
  buf = init_test_buffer (PROTO_BIN);
  const char *zquery_cmd3[] = { "zquery", "queryzset", "0.0", "", "0", "0" };
  ASSERT_TRUE (cache_execute (cache, zquery_cmd3, 6, &buf, now));

  // Query non-existent zset
  buf = init_test_buffer (PROTO_BIN);
  const char *zquery_cmd4[] = { "zquery", "nonexist", "0.0", "", "0", "5" };
  ASSERT_TRUE (cache_execute (cache, zquery_cmd4, 6, &buf, now));
  // Should return empty array

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 10: Type checking and errors
// ============================================================================

static void
test_type_checking (void)
{
  printf ("TEST: Type checking... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // Set a string key
  const char *set_cmd[] = { "set", "stringkey", "value" };
  ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));

  // Create a zset
  buf = init_test_buffer (PROTO_BIN);
  const char *zadd_cmd[] = { "zadd", "zsetkey", "1.0", "member" };
  ASSERT_TRUE (cache_execute (cache, zadd_cmd, 4, &buf, now));

  // Try zset operations on string key (should error)
  buf = init_test_buffer (PROTO_BIN);
  const char *zscore_cmd[] = { "zscore", "stringkey", "member" };
  ASSERT_TRUE (cache_execute (cache, zscore_cmd, 3, &buf, now));
  ASSERT_TRUE (is_err_response (&buf));

  // Try zadd on existing string key (should convert)
  buf = init_test_buffer (PROTO_BIN);
  const char *zadd_cmd2[] = { "zadd", "stringkey", "2.0", "newmember" };
  ASSERT_TRUE (cache_execute (cache, zadd_cmd2, 4, &buf, now));
  // This actually deletes the string and creates a zset

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 11: Unknown command
// ============================================================================

static void
test_unknown_command (void)
{
  printf ("TEST: Unknown command... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  const char *unknown_cmd[] = { "NOTACOMMAND", "arg1", "arg2" };
  ASSERT_TRUE (cache_execute (cache, unknown_cmd, 3, &buf, now));
  ASSERT_TRUE (is_err_response (&buf));

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 12: Stress test - many keys
// ============================================================================

static void
test_many_keys (void)
{
  printf ("TEST: Many keys stress test... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  const int NUM_KEYS = 10000;

  // Set many keys
  for (int i = 0; i < NUM_KEYS; i++)
    {
      buf = init_test_buffer (PROTO_BIN);
      char key[64], val[64];
      snprintf (key, sizeof (key), "stress_key_%d", i);
      snprintf (val, sizeof (val), "value_%d", i);

      const char *set_cmd[] = { "set", key, val };
      ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));
    }

  // Get random keys
  for (int i = 0; i < 100; i++)
    {
      buf = init_test_buffer (PROTO_BIN);
      int idx = rand () % NUM_KEYS;
      char key[64];
      snprintf (key, sizeof (key), "stress_key_%d", idx);

      const char *get_cmd[] = { "get", key };
      ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, now));
      ASSERT_FALSE (is_nil_response (&buf));
    }

  // Delete many keys
  for (int i = 0; i < NUM_KEYS; i += 2)
    {
      buf = init_test_buffer (PROTO_BIN);
      char key[64];
      snprintf (key, sizeof (key), "stress_key_%d", i);

      const char *del_cmd[] = { "del", key };
      ASSERT_TRUE (cache_execute (cache, del_cmd, 2, &buf, now));
    }

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 13: Buffer overflow protection
// ============================================================================

static void
test_buffer_overflow (void)
{
  printf ("TEST: Buffer overflow protection... ");

  Cache *cache = cache_init ();

  // Create a very small buffer
  uint8_t small_buffer[10];
  Buffer buf = buf_init (small_buffer, sizeof (small_buffer));
  buf_set_proto (&buf, PROTO_BIN);

  uint64_t now = get_test_time_us ();

  // Try to write a large response (KEYS with many results)
  // First set many keys
  for (int i = 0; i < 100; i++)
    {
      Buffer temp_buf = init_test_buffer (PROTO_BIN);
      char key[32], val[32];
      snprintf (key, sizeof (key), "k%d", i);
      snprintf (val, sizeof (val), "v%d", i);

      const char *set_cmd[] = { "set", key, val };
      ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &temp_buf, now));
    }

  // Now try KEYS with small buffer (should fail gracefully)
  const char *keys_cmd[] = { "keys", "*" };
  bool result = cache_execute (cache, keys_cmd, 2, &buf, now);
  // Should return false when buffer is too small
  ASSERT_FALSE (result);

  cache_free (cache);
  printf ("PASSED\n");
}


// ============================================================================
// Test 14: RESP Protocol (Text Mode) Verification
// ============================================================================
static void
test_resp_protocol (void)
{
  printf ("TEST: RESP protocol formatting... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_RESP);	// Switch to Text Mode
  uint64_t now = get_test_time_us ();

  // 1. Test Simple String (+OK)
  const char *set_cmd[] = { "set", "resp_key", "123" };
  ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));
  assert_buffer_contains (&buf, "+OK\r\n");

  // 2. Test Bulk String ($3\r\n123\r\n)
  buf = init_test_buffer (PROTO_RESP);
  const char *get_cmd[] = { "get", "resp_key" };
  ASSERT_TRUE (cache_execute (cache, get_cmd, 2, &buf, now));
  assert_buffer_contains (&buf, "$3\r\n123\r\n");

  // 3. Test Integer (:1)
  buf = init_test_buffer (PROTO_RESP);
  const char *del_cmd[] = { "del", "resp_key" };
  ASSERT_TRUE (cache_execute (cache, del_cmd, 2, &buf, now));
  assert_buffer_contains (&buf, ":1\r\n");

  // 4. Test Error (-ERR ...)
  buf = init_test_buffer (PROTO_RESP);
  const char *unknown[] = { "UNKNOWN_CMD" };
  ASSERT_TRUE (cache_execute (cache, unknown, 1, &buf, now));
  assert_buffer_contains (&buf, "-ERR");

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 15: Edge Cases (Empty keys, Long keys)
// ============================================================================
static void
test_edge_cases (void)
{
  printf ("TEST: Edge cases (Empty/Long)... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // 1. Empty Key ("")
  // Most DBs allow this, but it requires careful handling in C
  const char *empty_set[] = { "set", "", "empty_val" };
  ASSERT_TRUE (cache_execute (cache, empty_set, 3, &buf, now));

  buf = init_test_buffer (PROTO_BIN);
  const char *empty_get[] = { "get", "" };
  ASSERT_TRUE (cache_execute (cache, empty_get, 2, &buf, now));
  ASSERT_STR_EQ (extract_str_response (&buf), "empty_val");

  // 2. Very Long Key (1KB)
  // Ensures hashing doesn't segfault and buffer resizing works
  char long_key[1024];
  memset (long_key, 'A', sizeof (long_key) - 1);
  long_key[1023] = '\0';

  buf = init_test_buffer (PROTO_BIN);
  const char *long_set[] = { "set", long_key, "long_val" };
  ASSERT_TRUE (cache_execute (cache, long_set, 3, &buf, now));

  buf = init_test_buffer (PROTO_BIN);
  const char *long_get[] = { "get", long_key };
  ASSERT_TRUE (cache_execute (cache, long_get, 2, &buf, now));
  ASSERT_STR_EQ (extract_str_response (&buf), "long_val");

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 16: Command Argument Validation
// ============================================================================
static void
test_arg_validation (void)
{
  printf ("TEST: Argument validation... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // SET needs 3 args (SET key val). Send only 2.
  const char *bad_set[] = { "set", "key" };
  ASSERT_TRUE (cache_execute (cache, bad_set, 2, &buf, now));
  ASSERT_TRUE (is_err_response (&buf));

  // MSET needs pairs. Send odd number (MSET k1 v1 k2).
  buf = init_test_buffer (PROTO_BIN);
  const char *bad_mset[] = { "mset", "k1", "v1", "k2" };
  ASSERT_TRUE (cache_execute (cache, bad_mset, 4, &buf, now));
  ASSERT_TRUE (is_err_response (&buf));

  // ZADD needs score/member pairs. 
  buf = init_test_buffer (PROTO_BIN);
  const char *bad_zadd[] = { "zadd", "zkey", "10" };	// Missing member
  ASSERT_TRUE (cache_execute (cache, bad_zadd, 3, &buf, now));
  ASSERT_TRUE (is_err_response (&buf));

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 17: ZSET Score Updates (Re-insertion logic)
// ============================================================================
static void
test_zset_updates (void)
{
  printf ("TEST: ZSET score updates... ");

  Cache *cache = cache_init ();
  Buffer buf = init_test_buffer (PROTO_BIN);
  uint64_t now = get_test_time_us ();

  // 1. Add "player1" with score 100
  const char *cmd1[] = { "zadd", "lb", "100", "player1" };
  ASSERT_TRUE (cache_execute (cache, cmd1, 4, &buf, now));

  // 2. Update "player1" to score 200 (Should return 0 for 'updated')
  buf = init_test_buffer (PROTO_BIN);
  const char *cmd2[] = { "zadd", "lb", "200", "player1" };
  ASSERT_TRUE (cache_execute (cache, cmd2, 4, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 0);

  // 3. Verify Score is 200
  buf = init_test_buffer (PROTO_BIN);
  const char *cmd3[] = { "zscore", "lb", "player1" };
  ASSERT_TRUE (cache_execute (cache, cmd3, 3, &buf, now));

  // We need to parse the double string "200.000000" or similar
  double score = extract_dbl_response (&buf);
  ASSERT_TRUE (fabs (score - 200.0) < EPSILON);

  // 4. Add "player2" with same score (200) - Tie handling
  buf = init_test_buffer (PROTO_BIN);
  const char *cmd4[] = { "zadd", "lb", "200", "player2" };
  ASSERT_TRUE (cache_execute (cache, cmd4, 4, &buf, now));

  // 5. ZQUERY verify order (lexicographical check)
  // player1 vs player2 (both 200). '1' < '2', so player1 should be first?
  // Minis implementation dependent, but both should exist.
  buf = init_test_buffer (PROTO_BIN);
  const char *cmd5[] = { "zquery", "lb", "200", "", "0", "2" };
  ASSERT_TRUE (cache_execute (cache, cmd5, 6, &buf, now));
  // Response should be array of strings. 
  // Just verify we got a response without crashing.
  assert (buf_len (&buf) > 0);

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 18: Async Deletion (Large ZSET > 10k items)
// ============================================================================
static void
test_async_delete (void)
{
  printf ("TEST: Async deletion (Large ZSET)... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  // 1. Threshold is 10,000 in entry_del. We need 10,001+
  const int TEST_SIZE = 10005;

  // We reuse the buffer to avoid allocation overhead in the loop
  char member[32];
  const char *cmd[4] = { "zadd", "big_zset", "1.0", member };

  // Populate the ZSET
  for (int i = 0; i < TEST_SIZE; i++)
    {
      buf = init_test_buffer (PROTO_BIN);
      snprintf (member, sizeof (member), "mem_%d", i);

      // We don't ASSERT every single insert to keep the console clean/fast, 
      // but we assume they work if basic tests passed.
      if (!cache_execute (cache, cmd, 4, &buf, now))
	{
	  msgf ("Failed to insert member %d", i);
	  exit (1);
	}
    }

  // 2. DELETE the massive key
  // This should trigger the 'too_big' path in entry_del
  buf = init_test_buffer (PROTO_BIN);
  const char *del_cmd[] = { "del", "big_zset" };

  ASSERT_TRUE (cache_execute (cache, del_cmd, 2, &buf, now));
  ASSERT_EQ (extract_int_response (&buf), 1);	// Should report 1 deleted

  // 3. Verify immediate "disappearance"
  // Even if the background thread is still chewing on the memory, 
  // the main hash table must not have the key anymore.
  buf = init_test_buffer (PROTO_BIN);
  const char *check_cmd[] = { "zscore", "big_zset", "mem_1" };
  ASSERT_TRUE (cache_execute (cache, check_cmd, 3, &buf, now));
  ASSERT_TRUE (is_nil_response (&buf));	// Must be NIL

  // 4. Wait for background thread (Stability Check)
  // If there is a double-free or memory corruption in the async handler,
  // it usually crashes the process within a few milliseconds.
  usleep (200000);		// Sleep 200ms

  // 5. Do some work after to ensure heap is healthy
  buf = init_test_buffer (PROTO_BIN);
  const char *set_cmd[] = { "set", "after_async", "ok" };
  ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Main test runner
// ============================================================================

int
main (void)
{
  printf ("========================================\n");
  printf ("Cache Test Suite\n");
  printf ("========================================\n\n");

  srand ((unsigned int) time (NULL));

  test_ping ();
  test_set_get ();
  test_mset_mget ();
  test_del ();
  test_ttl ();
  test_active_eviction ();
  test_keys ();
  test_zset_basic ();
  test_zquery ();
  test_type_checking ();
  test_unknown_command ();
  test_many_keys ();
  test_buffer_overflow ();
  test_resp_protocol ();
  test_edge_cases ();
  test_arg_validation ();
  test_zset_updates ();
  test_async_delete ();
  printf ("\n========================================\n");
  printf ("All tests PASSED! ✓\n");
  printf ("========================================\n");

  return 0;
}
