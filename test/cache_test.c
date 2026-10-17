#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>

#include "cache/cache.h"
#include "cache/hashtable.h"
#include "common/common.h"
#include "common/glob.h"
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

// Helper to check an error response and its message
static void
assert_err_msg (Buffer *buf, const char *expected)
{
  // Binary protocol: SER_ERR + 4 bytes code + 4 bytes length + message.
  // The test buffer is zeroed, so the message is NUL terminated.
  ASSERT_TRUE (is_err_response (buf));
  const char *msg = (const char *) &buf_data (buf)[9];
  if (strstr (msg, expected) == NULL)
    {
      msgf ("ASSERTION FAILED: error '%s' does not contain '%s'", msg,
	    expected);
      exit (1);
    }
}

// Run one command into a fresh binary protocol 'buf'
#define RUN(cache, now, ...)                                                   \
	do {                                                                       \
		const char *run_cmd_[] = { __VA_ARGS__ };                              \
		buf = init_test_buffer (PROTO_BIN);                                    \
		ASSERT_TRUE (cache_execute ((cache), run_cmd_,                         \
					sizeof (run_cmd_) / sizeof (run_cmd_[0]),                  \
					&buf, (now)));                                             \
	} while (0)

// Helper to check a binary array of strings
static void
assert_str_array (Buffer *buf, const char **expected, size_t count)
{
  const uint8_t *data = buf_data (buf);
  ASSERT_EQ (data[0], SER_ARR);
  uint32_t len;
  memcpy (&len, &data[1], 4);
  ASSERT_EQ (ntoh_u32 (len), count);

  size_t pos = 5;
  for (size_t i = 0; i < count; i++)
    {
      ASSERT_EQ (data[pos], SER_STR);
      uint32_t slen;
      memcpy (&slen, &data[pos + 1], 4);
      slen = ntoh_u32 (slen);
      ASSERT_EQ (slen, strlen (expected[i]));
      ASSERT_TRUE (memcmp (&data[pos + 5], expected[i], slen) == 0);
      pos += 5 + slen;
    }
  ASSERT_EQ (pos, buf_len (buf));
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

  // DEL takes several keys too, a repeated key counts once
  RUN (cache, now, "mset", "d4", "v4", "d5", "v5");
  RUN (cache, now, "del", "d4", "nonexist", "d5", "d4");
  ASSERT_EQ (extract_int_response (&buf), 2);
  RUN (cache, now, "exists", "d4", "d5");
  ASSERT_EQ (extract_int_response (&buf), 0);

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
// Test 6b: SET clears the TTL of an existing key
// ============================================================================

// SET on a key with a TTL must also drop its expiry heap record, otherwise
// eviction keeps finding a due record for a key that no longer expires
// and spins forever.
static void
check_set_clears_ttl (const char *new_val)
{
  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  RUN (cache, now, "set", "setttl", "value");
  RUN (cache, now, "pexpire", "setttl", "1000");
  ASSERT_EQ (extract_int_response (&buf), 1);
  ASSERT_EQ (cache_next_expiry (cache), now + 1000000);

  RUN (cache, now, "set", "setttl", new_val);
  RUN (cache, now, "pttl", "setttl");
  ASSERT_EQ (extract_int_response (&buf), -1ULL);

  // Nothing is left to expire
  ASSERT_EQ (cache_next_expiry (cache), (uint64_t) - 1);

  // Must return, and must not evict the key
  uint64_t future = now + 2000000;
  alarm (5);
  cache_evict (cache, future);
  alarm (0);

  RUN (cache, future, "get", "setttl");
  ASSERT_STR_EQ (extract_str_response (&buf), new_val);

  // A new TTL on the same key still works
  RUN (cache, future, "pexpire", "setttl", "1000");
  ASSERT_EQ (extract_int_response (&buf), 1);

  uint64_t later = future + 1500000;
  alarm (5);
  cache_evict (cache, later);
  alarm (0);

  RUN (cache, later, "get", "setttl");
  ASSERT_TRUE (is_nil_response (&buf));
  ASSERT_EQ (cache_next_expiry (cache), (uint64_t) - 1);

  cache_free (cache);
}

static void
test_set_clears_ttl (void)
{
  printf ("TEST: SET clears TTL (new value)... ");
  check_set_clears_ttl ("other");
  printf ("PASSED\n");

  printf ("TEST: SET clears TTL (same value)... ");
  check_set_clears_ttl ("value");
  printf ("PASSED\n");
}

// ============================================================================
// Test 6c: PEXPIRE with a non-positive or huge TTL
// ============================================================================

static void
test_pexpire_edges (void)
{
  printf ("TEST: PEXPIRE edge cases... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  // A negative TTL deletes the key, and its pending expiry record
  RUN (cache, now, "set", "pexp", "v");
  RUN (cache, now, "pexpire", "pexp", "100");
  RUN (cache, now, "pexpire", "pexp", "-1");
  ASSERT_EQ (extract_int_response (&buf), 1);
  RUN (cache, now, "get", "pexp");
  ASSERT_TRUE (is_nil_response (&buf));
  ASSERT_EQ (cache_next_expiry (cache), (uint64_t) - 1);

  // So does zero, visible at once
  RUN (cache, now, "set", "pexp", "v");
  RUN (cache, now, "pexpire", "pexp", "0");
  ASSERT_EQ (extract_int_response (&buf), 1);
  RUN (cache, now, "exists", "pexp");
  ASSERT_EQ (extract_int_response (&buf), 0);

  // Missing key
  RUN (cache, now, "pexpire", "pexp", "-1");
  ASSERT_EQ (extract_int_response (&buf), 0);

  // Overflowing TTL is refused and leaves the key alone
  RUN (cache, now, "set", "pexp", "v");
  RUN (cache, now, "pexpire", "pexp", "9223372036854775807");
  assert_err_msg (&buf, "invalid expire time in 'pexpire' command");
  RUN (cache, now, "pttl", "pexp");
  ASSERT_EQ (extract_int_response (&buf), -1ULL);

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6d: EXPIRE and TTL
// ============================================================================

static void
test_expire_ttl (void)
{
  printf ("TEST: EXPIRE/TTL operations... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  RUN (cache, now, "set", "exp", "v");
  RUN (cache, now, "ttl", "exp");
  ASSERT_EQ (extract_int_response (&buf), -1ULL);
  RUN (cache, now, "ttl", "exp_missing");
  ASSERT_EQ (extract_int_response (&buf), -2ULL);

  RUN (cache, now, "expire", "exp", "100");
  ASSERT_EQ (extract_int_response (&buf), 1);
  ASSERT_EQ (cache_next_expiry (cache), now + 100000000);
  RUN (cache, now, "ttl", "exp");
  ASSERT_EQ (extract_int_response (&buf), 100);

  // TTL rounds to the nearest second
  RUN (cache, now, "pexpire", "exp", "1600");
  RUN (cache, now, "ttl", "exp");
  ASSERT_EQ (extract_int_response (&buf), 2);
  RUN (cache, now + 200000, "ttl", "exp");	// 1400ms left
  ASSERT_EQ (extract_int_response (&buf), 1);

  RUN (cache, now, "expire", "exp_missing", "10");
  ASSERT_EQ (extract_int_response (&buf), 0);

  RUN (cache, now, "expire", "exp", "9223372036854775");
  assert_err_msg (&buf, "invalid expire time in 'expire' command");

  // The most negative TTL still just deletes
  RUN (cache, now, "expire", "exp", "-9223372036854775808");
  ASSERT_EQ (extract_int_response (&buf), 1);
  RUN (cache, now, "exists", "exp");
  ASSERT_EQ (extract_int_response (&buf), 0);

  // INCR keeps the TTL
  RUN (cache, now, "incr", "exp_counter");
  RUN (cache, now, "expire", "exp_counter", "3600");
  RUN (cache, now, "incr", "exp_counter");
  ASSERT_EQ (extract_int_response (&buf), 2);
  RUN (cache, now, "ttl", "exp_counter");
  ASSERT_EQ (extract_int_response (&buf), 3600);

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6e: SET EX/PX, SETEX and PSETEX
// ============================================================================

static void
test_set_with_ttl (void)
{
  printf ("TEST: SET EX/PX, SETEX, PSETEX... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  RUN (cache, now, "set", "sx", "v", "EX", "100");
  ASSERT_FALSE (is_err_response (&buf));
  ASSERT_EQ (cache_next_expiry (cache), now + 100000000);
  RUN (cache, now, "get", "sx");
  ASSERT_STR_EQ (extract_str_response (&buf), "v");

  // Options are case insensitive; a new TTL replaces the old one
  RUN (cache, now, "set", "sx", "w", "px", "5000");
  RUN (cache, now, "pttl", "sx");
  ASSERT_EQ (extract_int_response (&buf), 5000);
  ASSERT_EQ (cache_next_expiry (cache), now + 5000000);

  // Same value without a TTL gets one
  RUN (cache, now, "set", "sx_same", "v");
  RUN (cache, now, "set", "sx_same", "v", "EX", "50");
  RUN (cache, now, "ttl", "sx_same");
  ASSERT_EQ (extract_int_response (&buf), 50);

  // Expires and is evicted
  uint64_t later = now + 6000000;
  cache_evict (cache, later);
  RUN (cache, later, "get", "sx");
  ASSERT_TRUE (is_nil_response (&buf));

  // Bad options change nothing
  RUN (cache, now, "set", "sx_bad", "orig");
  RUN (cache, now, "set", "sx_bad", "x", "EX", "0");
  assert_err_msg (&buf, "invalid expire time in 'set' command");
  RUN (cache, now, "set", "sx_bad", "x", "PX", "-5");
  assert_err_msg (&buf, "invalid expire time in 'set' command");
  RUN (cache, now, "set", "sx_bad", "x", "EX", "abc");
  assert_err_msg (&buf, "value is not an integer");
  RUN (cache, now, "set", "sx_bad", "x", "EX");
  assert_err_msg (&buf, "syntax error");
  RUN (cache, now, "set", "sx_bad", "x", "EX", "10", "PX", "10");
  assert_err_msg (&buf, "syntax error");
  RUN (cache, now, "set", "sx_bad", "x", "KEEPTTL");
  assert_err_msg (&buf, "syntax error");
  RUN (cache, now, "get", "sx_bad");
  ASSERT_STR_EQ (extract_str_response (&buf), "orig");
  RUN (cache, now, "pttl", "sx_bad");
  ASSERT_EQ (extract_int_response (&buf), -1ULL);

  RUN (cache, now, "setex", "sx_cmd", "1800", "v");
  RUN (cache, now, "ttl", "sx_cmd");
  ASSERT_EQ (extract_int_response (&buf), 1800);
  RUN (cache, now, "psetex", "sx_cmd", "5000", "w");
  RUN (cache, now, "pttl", "sx_cmd");
  ASSERT_EQ (extract_int_response (&buf), 5000);
  RUN (cache, now, "get", "sx_cmd");
  ASSERT_STR_EQ (extract_str_response (&buf), "w");
  RUN (cache, now, "setex", "sx_cmd", "0", "x");
  assert_err_msg (&buf, "invalid expire time in 'setex' command");
  RUN (cache, now, "psetex", "sx_cmd", "-1", "x");
  assert_err_msg (&buf, "invalid expire time in 'psetex' command");

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6f: Integer argument parsing
// ============================================================================

static void
test_integer_args (void)
{
  printf ("TEST: Integer argument parsing... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  const char *bad[] = { "", " 5", "+5", "5 ", "5x", "1.5",
    "99999999999999999999"
  };

  RUN (cache, now, "set", "ints", "10");
  for (size_t i = 0; i < sizeof (bad) / sizeof (bad[0]); i++)
    {
      RUN (cache, now, "pexpire", "ints", bad[i]);
      assert_err_msg (&buf, "value is not an integer or out of range");
      RUN (cache, now, "incrby", "ints", bad[i]);
      assert_err_msg (&buf, "value is not an integer or out of range");
      RUN (cache, now, "decrby", "ints", bad[i]);
      assert_err_msg (&buf, "value is not an integer or out of range");
    }
  RUN (cache, now, "pttl", "ints");
  ASSERT_EQ (extract_int_response (&buf), -1ULL);

  RUN (cache, now, "decrby", "ints", "-9223372036854775808");
  assert_err_msg (&buf, "decrement would overflow");
  RUN (cache, now, "get", "ints");
  ASSERT_STR_EQ (extract_str_response (&buf), "10");

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6g: RESP encodes the int64 limits
// ============================================================================

static void
test_resp_int64_limits (void)
{
  printf ("TEST: RESP int64 limits... ");

  Cache *cache = cache_init ();
  uint64_t now = get_test_time_us ();

  const char *set_cmd[] = { "set", "lim", "0" };
  const char *min_cmd[] = { "incrby", "lim", "-9223372036854775808" };
  const char *max_cmd[] = { "incrby", "lim", "9223372036854775807" };

  Buffer buf = init_test_buffer (PROTO_RESP);
  ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));
  buf = init_test_buffer (PROTO_RESP);
  ASSERT_TRUE (cache_execute (cache, min_cmd, 3, &buf, now));
  ASSERT_STR_EQ ((const char *) buf_data (&buf), ":-9223372036854775808\r\n");

  buf = init_test_buffer (PROTO_RESP);
  ASSERT_TRUE (cache_execute (cache, set_cmd, 3, &buf, now));
  buf = init_test_buffer (PROTO_RESP);
  ASSERT_TRUE (cache_execute (cache, max_cmd, 3, &buf, now));
  ASSERT_STR_EQ ((const char *) buf_data (&buf), ":9223372036854775807\r\n");

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6h: Lists, push and pop
// ============================================================================

static void
test_list_push_pop (void)
{
  printf ("TEST: List push/pop... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  RUN (cache, now, "rpush", "lst", "a", "b");
  ASSERT_EQ (extract_int_response (&buf), 2);
  RUN (cache, now, "lpush", "lst", "y", "z");
  ASSERT_EQ (extract_int_response (&buf), 4);
  RUN (cache, now, "llen", "lst");
  ASSERT_EQ (extract_int_response (&buf), 4);

  // List is now z y a b
  RUN (cache, now, "lpop", "lst");
  ASSERT_STR_EQ (extract_str_response (&buf), "z");
  RUN (cache, now, "rpop", "lst");
  ASSERT_STR_EQ (extract_str_response (&buf), "b");

  RUN (cache, now, "rpush", "lst", "c", "d");
  RUN (cache, now, "lpop", "lst", "2");
  const char *first_two[] = { "y", "a" };
  assert_str_array (&buf, first_two, 2);
  RUN (cache, now, "rpop", "lst", "0");
  assert_str_array (&buf, NULL, 0);
  RUN (cache, now, "rpop", "lst", "10");
  const char *rest[] = { "d", "c" };
  assert_str_array (&buf, rest, 2);

  // Popping the last element deletes the key
  RUN (cache, now, "exists", "lst");
  ASSERT_EQ (extract_int_response (&buf), 0);
  RUN (cache, now, "llen", "lst");
  ASSERT_EQ (extract_int_response (&buf), 0);
  RUN (cache, now, "lpop", "lst");
  ASSERT_TRUE (is_nil_response (&buf));
  RUN (cache, now, "lpop", "lst", "3");
  ASSERT_TRUE (is_nil_response (&buf));

  RUN (cache, now, "rpush", "lst", "a");
  RUN (cache, now, "lpop", "lst", "-1");
  assert_err_msg (&buf, "value is out of range, must be positive");
  RUN (cache, now, "lpop", "lst", "x");
  assert_err_msg (&buf, "value is out of range, must be positive");

  // Wrong types, both ways
  RUN (cache, now, "set", "lst_str", "v");
  RUN (cache, now, "rpush", "lst_str", "a");
  assert_err_msg (&buf, "WRONGTYPE");
  RUN (cache, now, "lpop", "lst_str");
  assert_err_msg (&buf, "WRONGTYPE");
  RUN (cache, now, "llen", "lst_str");
  assert_err_msg (&buf, "WRONGTYPE");
  RUN (cache, now, "get", "lst");
  assert_err_msg (&buf, "WRONGTYPE");
  RUN (cache, now, "hset", "lst", "f", "v");
  assert_err_msg (&buf, "WRONGTYPE");

  // Lists expire like any key
  RUN (cache, now, "pexpire", "lst", "1000");
  ASSERT_EQ (cache_next_expiry (cache), now + 1000000);
  uint64_t later = now + 2000000;
  cache_evict (cache, later);
  RUN (cache, later, "llen", "lst");
  ASSERT_EQ (extract_int_response (&buf), 0);
  ASSERT_EQ (cache_next_expiry (cache), (uint64_t) - 1);

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6i: LRANGE
// ============================================================================

static void
test_list_range (void)
{
  printf ("TEST: LRANGE... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  // Pushes at both ends wrap the ring buffer: a b c d e
  RUN (cache, now, "rpush", "lr", "c", "d");
  RUN (cache, now, "lpush", "lr", "b", "a", "x");
  RUN (cache, now, "lpop", "lr");
  RUN (cache, now, "rpush", "lr", "e");

  const char *all[] = { "a", "b", "c", "d", "e" };
  RUN (cache, now, "lrange", "lr", "0", "-1");
  assert_str_array (&buf, all, 5);
  RUN (cache, now, "lrange", "lr", "-100", "100");
  assert_str_array (&buf, all, 5);
  RUN (cache, now, "lrange", "lr", "1", "2");
  assert_str_array (&buf, &all[1], 2);
  RUN (cache, now, "lrange", "lr", "-2", "-1");
  assert_str_array (&buf, &all[3], 2);
  RUN (cache, now, "lrange", "lr", "4", "4");
  assert_str_array (&buf, &all[4], 1);

  // Empty ranges
  RUN (cache, now, "lrange", "lr", "3", "1");
  assert_str_array (&buf, NULL, 0);
  RUN (cache, now, "lrange", "lr", "5", "10");
  assert_str_array (&buf, NULL, 0);
  RUN (cache, now, "lrange", "lr", "-9223372036854775808",
       "-9223372036854775808");
  assert_str_array (&buf, NULL, 0);
  RUN (cache, now, "lrange", "lr", "0", "9223372036854775807");
  assert_str_array (&buf, all, 5);
  RUN (cache, now, "lrange", "lr_missing", "0", "-1");
  assert_str_array (&buf, NULL, 0);

  // Nothing was removed
  RUN (cache, now, "llen", "lr");
  ASSERT_EQ (extract_int_response (&buf), 5);

  RUN (cache, now, "lrange", "lr", "x", "1");
  assert_err_msg (&buf, "value is not an integer or out of range");
  RUN (cache, now, "set", "lr_str", "v");
  RUN (cache, now, "lrange", "lr_str", "0", "-1");
  assert_err_msg (&buf, "WRONGTYPE");

  // Too big for the reply buffer: fails instead of truncating
  static char big[3001];
  memset (big, 'x', sizeof (big) - 1);
  RUN (cache, now, "rpush", "lr_big", big, big, big);
  const char *range_cmd[] = { "lrange", "lr_big", "0", "-1" };
  buf = init_test_buffer (PROTO_BIN);
  ASSERT_FALSE (cache_execute (cache, range_cmd, 4, &buf, now));

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6j: LREM
// ============================================================================

// Fills key with a b a c a b a and runs LREM count val
static void
lrem_case (Cache *cache, uint64_t now, const char *count, const char *val,
	   int64_t removed, const char **rest, size_t rest_len)
{
  Buffer buf;
  RUN (cache, now, "del", "lrem");
  RUN (cache, now, "rpush", "lrem", "a", "b", "a", "c", "a", "b", "a");
  RUN (cache, now, "lrem", "lrem", count, val);
  ASSERT_EQ (extract_int_response (&buf), (uint64_t) removed);
  RUN (cache, now, "lrange", "lrem", "0", "-1");
  assert_str_array (&buf, rest, rest_len);
}

static void
test_list_rem (void)
{
  printf ("TEST: LREM... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  const char *no_a[] = { "b", "c", "b" };
  lrem_case (cache, now, "0", "a", 4, no_a, 3);
  lrem_case (cache, now, "100", "a", 4, no_a, 3);
  lrem_case (cache, now, "-100", "a", 4, no_a, 3);

  const char *head2[] = { "b", "c", "a", "b", "a" };
  lrem_case (cache, now, "2", "a", 2, head2, 5);

  const char *tail2[] = { "a", "b", "a", "c", "b" };
  lrem_case (cache, now, "-2", "a", 2, tail2, 5);

  const char *tail_b[] = { "a", "b", "a", "c", "a", "a" };
  lrem_case (cache, now, "-1", "b", 1, tail_b, 6);

  const char *all[] = { "a", "b", "a", "c", "a", "b", "a" };
  lrem_case (cache, now, "0", "z", 0, all, 7);

  // Removing everything deletes the key
  RUN (cache, now, "rpush", "lrem_x", "x", "x");
  RUN (cache, now, "lrem", "lrem_x", "0", "x");
  ASSERT_EQ (extract_int_response (&buf), 2);
  RUN (cache, now, "exists", "lrem_x");
  ASSERT_EQ (extract_int_response (&buf), 0);

  RUN (cache, now, "lrem", "lrem_missing", "0", "x");
  ASSERT_EQ (extract_int_response (&buf), 0);
  RUN (cache, now, "lrem", "lrem", "x", "a");
  assert_err_msg (&buf, "value is not an integer or out of range");
  RUN (cache, now, "lrem", "lrem", "-9223372036854775808", "a");
  assert_err_msg (&buf, "value is out of range");
  RUN (cache, now, "set", "lrem_str", "v");
  RUN (cache, now, "lrem", "lrem_str", "0", "v");
  assert_err_msg (&buf, "WRONGTYPE");

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6k: MGET over other types
// ============================================================================

static void
test_mget_other_types (void)
{
  printf ("TEST: MGET over other types... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  RUN (cache, now, "set", "mg_s", "v");
  RUN (cache, now, "hset", "mg_h", "f", "v");
  RUN (cache, now, "rpush", "mg_l", "a");
  RUN (cache, now, "zadd", "mg_z", "1", "m");

  // Binary: SER_ARR, count, then SER_STR "v" and four SER_NIL
  RUN (cache, now, "mget", "mg_s", "mg_h", "mg_l", "mg_z", "mg_none");
  const uint8_t *data = buf_data (&buf);
  ASSERT_EQ (data[0], SER_ARR);
  ASSERT_EQ (data[5], SER_STR);
  ASSERT_EQ (data[10], 'v');
  for (int i = 0; i < 4; i++)
    ASSERT_EQ (data[11 + i], SER_NIL);
  ASSERT_EQ (buf_len (&buf), 15);

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6l: Glob patterns
// ============================================================================

// Expected results come from valkey's KEYS. The first 60 are cases the
// old matcher, which knew only * and ?, got wrong.
static void
test_glob (void)
{
  printf ("TEST: Glob patterns... ");

  static const struct
  {
    const char *pattern;
    const char *string;
    bool match;
  } cases[] = {
    { "*", "*a", true },
    { "a*", "a*c", true },
    { "h[ae]llo", "hello", true },
    { "h[ae]llo", "hallo", true },
    { "h[^e]llo", "hallo", true },
    { "h[^e]llo", "hxllo", true },
    { "h[a-b]llo", "hallo", true },
    { "h[b-a]llo", "hallo", true },
    { "[a-c]", "a", true },
    { "[a-c]", "b", true },
    { "[a-c]", "c", true },
    { "[^a-c]", "?", true },
    { "[^a-c]", "[", true },
    { "[^a-c]", "]", true },
    { "[^a-c]", "-", true },
    { "[^a-c]", "z", true },
    { "[^a-c]", "Z", true },
    { "[abc]*", "a", true },
    { "[abc]*", "b", true },
    { "[abc]*", "c", true },
    { "[abc]*", "ab", true },
    { "[abc]*", "abc", true },
    { "[abc]*", "abd", true },
    { "[abc]*", "aXc", true },
    { "[abc]*", "a]c", true },
    { "[abc]*", "a-c", true },
    { "[abc]*", "a^c", true },
    { "[abc]*", "a\\c", true },
    { "[abc]*", "a*c", true },
    { "[abc]*", "a?c", true },
    { "[abc]*", "aa", true },
    { "[abc]*", "aaa", true },
    { "[abc]*", "aaab", true },
    { "[abc]*", "baaa", true },
    { "a[-]c", "a-c", true },
    { "a[\\]]c", "a]c", true },
    { "a\\*c", "a\\c", false },
    { "a\\*c", "a*c", true },
    { "a\\?c", "a?c", true },
    { "a\\\\c", "a\\c", true },
    { "a[\\-]c", "a-c", true },
    { "[", "[", false },
    { "[a", "a", true },
    { "seen:1[0-2]", "seen:10", true },
    { "*a*", "a*c", true },
    { "[*]a", "*a", true },
    { "[?]", "?", true },
    { "[a-]", "a", true },
    { "[a-]", "]", true },
    { "[!a]", "a", true },
    { "[z-z]", "z", true },
    { "[Z]", "Z", true },
    { "[^Z]", "a", true },
    { "[^Z]", "b", true },
    { "[^Z]", "c", true },
    { "[^Z]", "?", true },
    { "[^Z]", "[", true },
    { "[^Z]", "]", true },
    { "[^Z]", "-", true },
    { "[^Z]", "z", true },
    { "h?llo", "hxllo", true },
    { "?", "-", true },
    { "**", "aXc", true },
    { "*?", "heeeello", true },
    { "*", "a?c", true },
    { "*", "hallo", true },
    { "*a*", "a?c", true },
    { "*", "seen:123", true },
    { "*seen*", "seen:123", true },
    { "*?", "abc", true },
    { "*", "[", true },
    { "*a*", "ab", true },
    { "a*", "aaab", true },
    { "*", "a^c", true },
    { "*", "seen:10", true },
    { "**", "]", true },
    { "**", "a?c", true },
    { "*", "hello", true },
    { "*c", "a\\c", true },
    { "*", "seen:1", true },
    { "*a*", "aaa", true },
    { "**", "*a", true },
    { "*", "]", true },
    { "a*a*b", "aaab", true },
    { "*", "baaa", true },
    { "a[-]c", "hallo", false },
    { "[Z]", "c", false },
    { "a*c", "c", false },
    { "[z-z]", "hllo", false },
    { "[Z]", "a-c", false },
    { "seen:1[0-2]", "hallo", false },
    { "a?c", "c", false },
    { "a[-]c", "?", false },
    { "*c", "aa", false },
    { "a**c", "?", false },
    { "h[a-b]llo", "ab", false },
    { "a\\\\c", "Z", false },
    { "*seen*", "ab", false },
    { "h[a-b]llo", "z", false },
    { "[!a]", "aXc", false },
  };

  for (size_t i = 0; i < sizeof (cases) / sizeof (cases[0]); i++)
    if (glob_match (cases[i].pattern, cases[i].string) != cases[i].match)
      {
	msgf ("ASSERTION FAILED: glob '%s' on '%s' should be %d",
	      cases[i].pattern, cases[i].string, cases[i].match);
	exit (1);
      }

  printf ("PASSED\n");
}

// ============================================================================
// Test 6m: hm_scan_step across resizes
// ============================================================================

typedef struct
{
  HNode node;
  int id;
} ScanNode;

static bool
scan_node_eq (HNode *node, const void *key)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  return container_of (node, ScanNode, node)->id == *(const int *) key;
#pragma GCC diagnostic pop
}

#define SCAN_STABLE 1000
#define SCAN_MAX_NODES 40000

static int scan_visits[SCAN_MAX_NODES];

static void
cb_scan_visit (HNode *node, void *arg)
{
  (void) arg;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  scan_visits[container_of (node, ScanNode, node)->id]++;
#pragma GCC diagnostic pop
}

static void
scan_insert (HMap *map, ScanNode *nodes, int id)
{
  nodes[id].id = id;
  nodes[id].node.hcode = str_hash ((const uint8_t *) &id, sizeof (id));
  hm_insert (map, &nodes[id].node, &id, scan_node_eq);
}

// The SCAN guarantee at the table level: nodes there for the whole scan
// are visited, while the map grows through many resizes (some steps
// landing mid resize) and other nodes are deleted, shifting entries back
static void
test_hm_scan_step (void)
{
  printf ("TEST: hm_scan_step across resizes... ");

  static ScanNode nodes[SCAN_MAX_NODES];
  HMap map;
  ASSERT_TRUE (hm_init (&map, 8));
  memset (scan_visits, 0, sizeof (scan_visits));

  int next_id = 0;
  for (; next_id < SCAN_STABLE; next_id++)
    scan_insert (&map, nodes, next_id);

  size_t steps = 0;
  size_t steps_mid_resize = 0;
  int deleted = 0;
  size_t cursor = 0;
  do
    {
      // A cursor that never returns to 0 fails instead of hanging
      ASSERT_TRUE (steps < 1000000);
      if (map.ht2.tab != NULL)
	steps_mid_resize++;
      cursor = hm_scan_step (&map, cursor, cb_scan_visit, NULL);
      steps++;

      // Grow by more than one step visits, so tables keep resizing
      for (int i = 0; i < 3 && next_id < SCAN_MAX_NODES; i++)
	scan_insert (&map, nodes, next_id++);
      // And delete some of the added nodes
      if (steps % 4 == 0 && SCAN_STABLE + deleted < next_id)
	{
	  int id = SCAN_STABLE + deleted++;
	  ASSERT_TRUE (hm_pop (&map, &id, nodes[id].node.hcode,
			       scan_node_eq) != NULL);
	}
    }
  while (cursor != 0);

  for (int id = 0; id < SCAN_STABLE; id++)
    ASSERT_TRUE (scan_visits[id] >= 1);
  ASSERT_TRUE (steps_mid_resize > 0);

  // Without changes, a scan visits each node exactly once
  memset (scan_visits, 0, sizeof (scan_visits));
  cursor = 0;
  steps = 0;
  do
    {
      ASSERT_TRUE (steps++ < 1000000);
      cursor = hm_scan_step (&map, cursor, cb_scan_visit, NULL);
    }
  while (cursor != 0);
  size_t live = 0;
  for (int id = 0; id < next_id; id++)
    {
      ASSERT_TRUE (scan_visits[id] <= 1);
      live += (size_t) scan_visits[id];
    }
  ASSERT_EQ (live, hm_size (&map));

  hm_destroy (&map);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6n: SCAN command
// ============================================================================

// Runs SCAN and parses the binary reply: returns the next cursor and
// fills keys (pointers into buf) and *nkeys
static uint64_t
scan_reply (Buffer *buf, const char **keys, size_t max_keys, size_t *nkeys)
{
  const uint8_t *data = buf_data (buf);
  ASSERT_EQ (data[0], SER_ARR);
  ASSERT_EQ (data[5], SER_STR);
  uint32_t len;
  memcpy (&len, &data[6], 4);
  len = ntoh_u32 (len);
  char cursor_str[32] = { 0 };
  ASSERT_TRUE (len < sizeof (cursor_str));
  memcpy (cursor_str, &data[10], len);

  size_t pos = 10 + len;
  ASSERT_EQ (data[pos], SER_ARR);
  uint32_t count;
  memcpy (&count, &data[pos + 1], 4);
  count = ntoh_u32 (count);
  ASSERT_TRUE (count <= max_keys);
  pos += 5;
  for (uint32_t i = 0; i < count; i++)
    {
      ASSERT_EQ (data[pos], SER_STR);
      uint32_t klen;
      memcpy (&klen, &data[pos + 1], 4);
      klen = ntoh_u32 (klen);
      // The test buffer is zeroed and keys are followed by a type byte,
      // so copy each key out
      static char key_store[4096][64];
      ASSERT_TRUE (klen < 64);
      memcpy (key_store[i], &data[pos + 5], klen);
      key_store[i][klen] = '\0';
      keys[i] = key_store[i];
      pos += 5 + klen;
    }
  ASSERT_EQ (pos, buf_len (buf));
  *nkeys = count;
  return strtoull (cursor_str, NULL, 10);
}

static void
test_scan_command (void)
{
  printf ("TEST: SCAN command... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();
  const char *keys[4096];
  size_t nkeys = 0;

  RUN (cache, now, "scan", "0");
  ASSERT_EQ (scan_reply (&buf, keys, 4096, &nkeys), 0);
  ASSERT_EQ (nkeys, 0);

  // 500 strings, a list, and 50 keys that expire
  static bool seen[500];
  for (int i = 0; i < 500; i++)
    {
      char key[32];
      snprintf (key, sizeof (key), "sc:%d", i);
      RUN (cache, now, "set", key, "v");
    }
  RUN (cache, now, "rpush", "sc_list", "a");
  for (int i = 0; i < 50; i++)
    {
      char key[32];
      snprintf (key, sizeof (key), "sc_dying:%d", i);
      RUN (cache, now, "set", key, "v", "PX", "100");
    }
  uint64_t later = now + 1000000;

  // COUNT 1 walks the keyspace one bucket family at a time
  char cursor_str[32] = "0";
  size_t total = 0;
  int calls = 0;
  do
    {
      RUN (cache, later, "scan", cursor_str, "count", "1");
      uint64_t next = scan_reply (&buf, keys, 4096, &nkeys);
      for (size_t i = 0; i < nkeys; i++)
	{
	  int id;
	  ASSERT_TRUE (strncmp (keys[i], "sc_dying", 8) != 0);
	  if (sscanf (keys[i], "sc:%d", &id) == 1)
	    {
	      ASSERT_FALSE (seen[id]);
	      seen[id] = true;
	    }
	  total++;
	}
      snprintf (cursor_str, sizeof (cursor_str), "%" PRIu64, next);
      calls++;
    }
  while (strcmp (cursor_str, "0") != 0);
  ASSERT_EQ (total, 501);
  ASSERT_TRUE (calls > 1);
  for (int i = 0; i < 500; i++)
    ASSERT_TRUE (seen[i]);

  // The dying keys were skipped, not deleted, scanning is read only
  RUN (cache, now, "exists", "sc_dying:0");
  ASSERT_EQ (extract_int_response (&buf), 1);

  // A big COUNT finishes in one call; MATCH and TYPE filter
  RUN (cache, later, "scan", "0", "COUNT", "100000");
  ASSERT_EQ (scan_reply (&buf, keys, 4096, &nkeys), 0);
  ASSERT_EQ (nkeys, 501);
  RUN (cache, later, "scan", "0", "MATCH", "sc:4?", "COUNT", "100000");
  ASSERT_EQ (scan_reply (&buf, keys, 4096, &nkeys), 0);
  ASSERT_EQ (nkeys, 10);
  RUN (cache, later, "scan", "0", "TYPE", "LIST", "COUNT", "100000");
  ASSERT_EQ (scan_reply (&buf, keys, 4096, &nkeys), 0);
  ASSERT_EQ (nkeys, 1);
  ASSERT_STR_EQ (keys[0], "sc_list");

  // Errors, with the valkey texts
  RUN (cache, now, "scan", "x");
  assert_err_msg (&buf, "invalid cursor");
  RUN (cache, now, "scan", "-1");
  assert_err_msg (&buf, "invalid cursor");
  RUN (cache, now, "scan", "18446744073709551616");
  assert_err_msg (&buf, "invalid cursor");
  RUN (cache, now, "scan", "0", "COUNT", "0");
  assert_err_msg (&buf, "syntax error");
  RUN (cache, now, "scan", "0", "COUNT", "x");
  assert_err_msg (&buf, "value is not an integer or out of range");
  RUN (cache, now, "scan", "0", "MATCH");
  assert_err_msg (&buf, "syntax error");
  RUN (cache, now, "scan", "0", "TYPE", "set");
  assert_err_msg (&buf, "unknown type name 'set'");
  RUN (cache, now, "scan", "0", "BOGUS", "1");
  assert_err_msg (&buf, "syntax error");

  // Any cursor value terminates
  snprintf (cursor_str, sizeof (cursor_str), "%s", "18446744073709551615");
  for (calls = 0; strcmp (cursor_str, "0") != 0; calls++)
    {
      ASSERT_TRUE (calls < 100000);
      RUN (cache, later, "scan", cursor_str, "count", "10");
      snprintf (cursor_str, sizeof (cursor_str), "%" PRIu64,
		scan_reply (&buf, keys, 4096, &nkeys));
    }

  cache_free (cache);
  printf ("PASSED\n");
}

// ============================================================================
// Test 6o: BLPOP/BRPOP without the waiting (cache_bpop)
// ============================================================================

// Runs cache_bpop into a fresh 'buf', result in 'res'
#define BPOP(retry, ...)                                                        \
	do {                                                                       \
		const char *bpop_cmd_[] = { __VA_ARGS__ };                             \
		buf = init_test_buffer (PROTO_BIN);                                    \
		res = cache_bpop (cache, bpop_cmd_,                                    \
				  sizeof (bpop_cmd_) / sizeof (bpop_cmd_[0]),       \
				  (retry), &buf, now, &timeout_us);                 \
	} while (0)

static void
test_bpop (void)
{
  printf ("TEST: BLPOP/BRPOP (cache_bpop)... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();
  uint64_t timeout_us = 0;
  BPopResult res;

  // Pops from the first non-empty list: [key, value]
  RUN (cache, now, "rpush", "bp2", "a", "b");
  BPOP (false, "blpop", "bp1", "bp2", "1");
  ASSERT_EQ (res, BPOP_DONE);
  const char *first[] = { "bp2", "a" };
  assert_str_array (&buf, first, 2);
  BPOP (false, "brpop", "bp1", "bp2", "1");
  ASSERT_EQ (res, BPOP_DONE);
  const char *last[] = { "bp2", "b" };
  assert_str_array (&buf, last, 2);

  // Nothing there: block, writing nothing, with the timeout parsed
  struct
  {
    const char *arg;
    uint64_t us;
  } timeouts[] = {
    {"0", 0}, {"1", 1000000}, {"1.5", 1500000}, {"0.001", 1000},
    {"0x10", 16000000}, {"2e3", 2000000000ULL}
  };
  for (size_t i = 0; i < sizeof (timeouts) / sizeof (timeouts[0]); i++)
    {
      BPOP (false, "blpop", "bp1", "bp2", timeouts[i].arg);
      ASSERT_EQ (res, BPOP_BLOCK);
      ASSERT_EQ (buf_len (&buf), 0);
      ASSERT_EQ (timeout_us, timeouts[i].us);
    }
  // The largest allowed timeout is capped instead of overflowing
  BPOP (false, "blpop", "bp1", "9223372036854775");
  ASSERT_EQ (res, BPOP_BLOCK);
  ASSERT_EQ (timeout_us, UINT64_MAX / 4);

  // Errors are replies
  struct
  {
    const char *arg;
    const char *msg;
  } bad[] = {
    {"x", "timeout is not a float or out of range"},
    {"", "timeout is not a float or out of range"},
    {" 1", "timeout is not a float or out of range"},
    {"nan", "timeout is not a float or out of range"},
    {"-1", "timeout is negative"},
    {"inf", "timeout is out of range"},
    {"1e400", "timeout is out of range"},
    {"9223372036854775807", "timeout is out of range"}
  };
  for (size_t i = 0; i < sizeof (bad) / sizeof (bad[0]); i++)
    {
      BPOP (false, "blpop", "bp1", bad[i].arg);
      ASSERT_EQ (res, BPOP_DONE);
      assert_err_msg (&buf, bad[i].msg);
    }
  BPOP (false, "blpop", "bp1");
  ASSERT_EQ (res, BPOP_DONE);
  assert_err_msg (&buf, "wrong number of arguments for 'blpop' command");
  BPOP (false, "brpop", "bp1");
  ASSERT_EQ (res, BPOP_DONE);
  assert_err_msg (&buf, "wrong number of arguments for 'brpop' command");

  // Another type: an error the first time, skipped on a retry
  RUN (cache, now, "set", "bp_str", "v");
  BPOP (false, "blpop", "bp_str", "1");
  ASSERT_EQ (res, BPOP_DONE);
  assert_err_msg (&buf, "WRONGTYPE");
  BPOP (true, "blpop", "bp_str", "1");
  ASSERT_EQ (res, BPOP_BLOCK);
  ASSERT_EQ (buf_len (&buf), 0);
  RUN (cache, now, "rpush", "bp3", "c");
  BPOP (true, "blpop", "bp_str", "bp3", "1");
  ASSERT_EQ (res, BPOP_DONE);
  const char *third[] = { "bp3", "c" };
  assert_str_array (&buf, third, 2);

  // A reply that does not fit pops nothing
  static char big[9000];
  memset (big, 'x', sizeof (big) - 1);
  RUN (cache, now, "rpush", "bp_big", big);
  BPOP (false, "blpop", "bp_big", "1");
  ASSERT_EQ (res, BPOP_FAIL);
  RUN (cache, now, "llen", "bp_big");
  ASSERT_EQ (extract_int_response (&buf), 1);

  // Without a server to wait in, cache_execute times out at once
  RUN (cache, now, "blpop", "bp_none", "5");
  ASSERT_TRUE (is_nil_response (&buf));
  RUN (cache, now, "rpush", "bp4", "d");
  RUN (cache, now, "brpop", "bp4", "5");
  const char *fourth[] = { "bp4", "d" };
  assert_str_array (&buf, fourth, 2);

  // The push counter moves on pushes only
  uint64_t pushes = cache_list_pushes (cache);
  RUN (cache, now, "lpush", "bp5", "e");
  ASSERT_EQ (cache_list_pushes (cache), pushes + 1);
  RUN (cache, now, "rpush", "bp5", "f", "g");
  ASSERT_EQ (cache_list_pushes (cache), pushes + 2);
  RUN (cache, now, "lpop", "bp5");
  RUN (cache, now, "lpush", "bp_str", "x");
  ASSERT_EQ (cache_list_pushes (cache), pushes + 2);

  cache_free (cache);
  printf ("PASSED\n");
}

// A pop whose reply does not fit must leave the list alone
static void
test_list_pop_too_big (void)
{
  printf ("TEST: List pop too big for reply... ");

  Cache *cache = cache_init ();
  Buffer buf;
  uint64_t now = get_test_time_us ();

  static char big[3001];
  memset (big, 'x', sizeof (big) - 1);
  RUN (cache, now, "rpush", "lbig", big, big, big);

  // 9KB of reply in the 8KB test buffer
  const char *pop_cmd[] = { "lpop", "lbig", "3" };
  buf = init_test_buffer (PROTO_BIN);
  ASSERT_FALSE (cache_execute (cache, pop_cmd, 3, &buf, now));

  RUN (cache, now, "llen", "lbig");
  ASSERT_EQ (extract_int_response (&buf), 3);
  RUN (cache, now, "lpop", "lbig", "2");
  const char *two[] = { big, big };
  assert_str_array (&buf, two, 2);

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
  test_set_clears_ttl ();
  test_pexpire_edges ();
  test_expire_ttl ();
  test_set_with_ttl ();
  test_integer_args ();
  test_resp_int64_limits ();
  test_list_push_pop ();
  test_list_pop_too_big ();
  test_list_range ();
  test_list_rem ();
  test_mget_other_types ();
  test_glob ();
  test_hm_scan_step ();
  test_scan_command ();
  test_bpop ();
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
