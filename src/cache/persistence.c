// ============================================================================
// Persistence Layer - Save/Load Cache to Disk
// ============================================================================

#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>

#include "cache/hashtable.h"
#include "cache/minis_private.h"
#include "common/common.h"
#include "io/buffer.h"
#include "cache/minis.h"
#include "cache/entry.h"
#include "cache/zset.h"
#include "cache/hash.h"
#include "cache/persistence.h"

// ============================================================================
// CRC32 Utilities
// ============================================================================

static uint32_t
crc32_update (uint32_t crc, const uint8_t *buf, size_t len)
{
  crc = ~crc;
  for (size_t i = 0; i < len; i++)
    {
      crc ^= buf[i];
      for (int k = 0; k < 8; k++)
	{
	  crc = (crc & 1) ? (crc >> 1) ^ 0xEDB88320 : (crc >> 1);
	}
    }
  return ~crc;
}

// ============================================================================
// Read/Write Helpers (Unchanged)
// ============================================================================

static bool
read_exact_crc (FILE *file_ptr, void *buf, size_t len, uint32_t *crc)
{
  if (fread (buf, 1, len, file_ptr) != len)
    return false;
  *crc = crc32_update (*crc, (const uint8_t *) buf, len);
  return true;
}

static bool
read_u32_crc (FILE *file_ptr, uint32_t *out, uint32_t *crc)
{
  uint32_t net;
  if (!read_exact_crc (file_ptr, &net, 4, crc))
    return false;
  *out = ntoh_u32 (net);
  return true;
}

static bool
read_double_crc (FILE *file_ptr, double *out, uint32_t *crc)
{
  return read_exact_crc (file_ptr, out, 8, crc);
}

static bool
read_u32_plain (FILE *file_ptr, uint32_t *out)
{
  uint32_t net;
  if (fread (&net, 1, 4, file_ptr) != 4)
    return false;
  *out = ntoh_u32 (net);
  return true;
}

static bool
out_raw_str (Buffer *buf, const char *str, size_t len)
{
  return buf_append_u32 (buf, (uint32_t) len) &&
    buf_append_bytes (buf, (const uint8_t *) str, len);
}

static bool
out_typed_str (Buffer *buf, const char *str, size_t len)
{
  return buf_append_byte (buf, T_STR) && out_raw_str (buf, str, len);
}

// ============================================================================
// Serialization Primitives (Unchanged logic)
// ============================================================================

static bool
serialize_znode (Buffer *buf, AVLNode *node)
{
  if (!node)
    return true;
  if (!serialize_znode (buf, node->left))
    return false;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  ZNode *znode = container_of (node, ZNode, tree);
#pragma GCC diagnostic pop

  if (!buf_append_double (buf, znode->score))
    return false;
  if (!buf_append_byte (buf, T_STR))
    return false;
  if (!out_raw_str (buf, znode->name, znode->len))
    return false;

  return serialize_znode (buf, node->right);
}

static bool
serialize_zset (Buffer *buf, ZSet *zset)
{
  if (!buf_append_byte (buf, T_ZSET))
    return false;
  uint32_t count = (zset && zset->tree) ? zset->tree->cnt : 0;
  if (!buf_append_u32 (buf, count))
    return false;
  return count > 0 ? serialize_znode (buf, zset->tree) : true;
}

static bool
serialize_hash (Buffer *buf, HMap *hmap)
{
  if (!buf_append_byte (buf, T_HASH))
    return false;
  uint32_t count = (uint32_t) hm_size (hmap);
  if (!buf_append_u32 (buf, count))
    return false;

  HMIter iter;
  hm_iter_init (hmap, &iter);
  HNode *node;
  while ((node = hm_iter_next (&iter)) != NULL)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      HashEntry *hash_entry = container_of (node, HashEntry, node);
#pragma GCC diagnostic pop
      if (!out_raw_str (buf, hash_entry->field, hash_entry->field_len))
	return false;
      if (!out_raw_str (buf, hash_entry->value, hash_entry->value_len))
	return false;
    }
  return true;
}

static bool
serialize_list (Buffer *buf, const List *list)
{
  if (!buf_append_byte (buf, T_LIST))
    return false;
  if (!buf_append_u32 (buf, (uint32_t) list_len (list)))
    return false;
  for (size_t i = 0; i < list_len (list); i++)
    {
      const ListItem *item = list_at (list, i);
      if (!out_raw_str (buf, item->data, item->len))
	return false;
    }
  return true;
}

// The two clocks at one moment: entries expire on the monotonic one,
// files hold the wall clock
typedef struct
{
  uint64_t mono_us;
  uint64_t wall_us;
} SnapClock;

static uint64_t
real_wall_clock_us (void)
{
  struct timespec tvs = { 0, 0 };
  clock_gettime (CLOCK_REALTIME, &tvs);
  return (uint64_t) tvs.tv_sec * 1000000 + (uint64_t) tvs.tv_nsec / 1000;
}

static uint64_t (*g_wall_clock_us) (void) = real_wall_clock_us;

void
persistence_set_wall_clock (uint64_t (*wall_clock_us) (void))
{
  g_wall_clock_us = wall_clock_us ? wall_clock_us : real_wall_clock_us;
}

static SnapClock
snap_clock_now (uint64_t now_us)
{
  SnapClock clock = {.mono_us = now_us,.wall_us = g_wall_clock_us () };
  return clock;
}

// Monotonic expiry to wall clock, 0 stays "no expiry". Saved entries
// have not expired, so the time left is not negative.
static uint64_t
expiry_to_wall (uint64_t expire_at_us, const SnapClock *clock)
{
  if (expire_at_us == 0)
    return 0;
  uint64_t left = expire_at_us > clock->mono_us
    ? expire_at_us - clock->mono_us : 0;
  return clock->wall_us + left;
}

// Wall clock expiry back to monotonic. Sets *expired if it already
// passed, which is also how a key that expired while minis was down
// shows.
static uint64_t
expiry_from_wall (uint64_t wall_us, const SnapClock *clock, bool *expired)
{
  *expired = false;
  if (wall_us == 0)
    return 0;
  if (wall_us <= clock->wall_us)
    {
      *expired = true;
      return 0;
    }
  return clock->mono_us + (wall_us - clock->wall_us);
}

static bool
serialize_entry (Buffer *buf, Entry *entry, const SnapClock *clock)
{
  uint64_t expire = expiry_to_wall (entry->expire_at_us, clock);
  if (!buf_append_i64 (buf, (int64_t) expire))
    return false;

  uint32_t klen = (uint32_t) entry->key_len;
  if (!buf_append_u32 (buf, klen))
    return false;
  if (!buf_append_bytes (buf, (const uint8_t *) entry->key, klen))
    return false;

  if (entry->type == T_STR)
    return out_typed_str (buf, entry->val, entry->val_len);
  if (entry->type == T_ZSET)
    return serialize_zset (buf, entry->zset);
  if (entry->type == T_HASH)
    return serialize_hash (buf, entry->hash);
  if (entry->type == T_LIST)
    return serialize_list (buf, entry->list);

  return false;
}

// ============================================================================
// File Format Helpers
// ============================================================================

static bool
write_file_header (FILE *file_ptr, long *crc_offset, uint32_t item_count)
{
  if (fwrite (MINIS_DB_MAGIC, 1, 4, file_ptr) != 4)
    return false;

  *crc_offset = ftell (file_ptr);
  uint32_t zero_crc = 0;
  if (fwrite (&zero_crc, 4, 1, file_ptr) != 1)
    return false;

  uint32_t ver = htonl (MINIS_DB_VERSION);
  if (fwrite (&ver, 4, 1, file_ptr) != 1)
    return false;
  uint32_t cnt = htonl (item_count);
  return fwrite (&cnt, 4, 1, file_ptr) == 1;
}

static bool
patch_crc (FILE *file_ptr, long crc_offset, uint32_t crc)
{
  if (fseek (file_ptr, crc_offset, SEEK_SET) != 0)
    return false;
  uint32_t net_crc = htonl (crc);
  return fwrite (&net_crc, 4, 1, file_ptr) == 1;
}

static bool
read_and_verify_header (FILE *file_ptr, uint32_t *expected_crc,
			uint32_t *out_item_count, uint32_t *out_version)
{
  char magic[4];
  if (fread (magic, 1, 4, file_ptr) != 4
      || memcmp (magic, MINIS_DB_MAGIC, 4) != 0)
    {
      msgf ("Snapshot: invalid magic signature");
      return false;
    }

  if (!read_u32_plain (file_ptr, expected_crc))
    {
      msgf ("Snapshot: failed to read CRC");
      return false;
    }

  uint32_t ver;
  if (!read_u32_plain (file_ptr, &ver))
    {
      msgf ("Snapshot: failed to read version");
      return false;
    }

  // Version 1 differs only in its monotonic expiries
  if (ver != MINIS_DB_VERSION && ver != 1)
    {
      msgf ("Snapshot: unsupported version %u", ver);
      return false;
    }
  *out_version = ver;

  if (!read_u32_plain (file_ptr, out_item_count))
    {
      msgf ("Snapshot: failed to read item count");
      return false;
    }
  return true;
}

// ============================================================================
// Entry Deserialization
// ============================================================================

static bool
load_hash_entry (FILE *file_ptr, Minis *minis, const char *key,
		 size_t key_len,
		 uint64_t expire_at, uint32_t *crc, bool skip)
{
  uint32_t count;
  if (!read_u32_crc (file_ptr, &count, crc))
    return false;

  Entry *ent = NULL;
  if (!skip)
    {
      ent = entry_new_hash (minis, key, key_len);
      if (!ent)
	return false;
      if (expire_at > 0)
	entry_set_expiration (minis, ent, expire_at);
    }

  for (uint32_t i = 0; i < count; i++)
    {
      // 1. Read Field
      uint32_t flen;
      if (!read_u32_crc (file_ptr, &flen, crc))
	return false;

      char field_buf[K_MAX_MSG];	// Assuming fields are reasonably small keys
      if (flen >= sizeof (field_buf))
	return false;
      if (!read_exact_crc (file_ptr, field_buf, flen, crc))
	return false;
      field_buf[flen] = '\0';

      // 2. Read Value (handling potentially large values)
      uint32_t vlen;
      if (!read_u32_crc (file_ptr, &vlen, crc))
	return false;

      if (skip)
	{
	  // Drain the value data to update CRC
	  char dump[1024];
	  size_t rem = vlen;
	  while (rem > 0)
	    {
	      size_t chunk = (rem < sizeof (dump)) ? rem : sizeof (dump);
	      if (!read_exact_crc (file_ptr, dump, chunk, crc))
		return false;
	      rem -= chunk;
	    }
	}
      else
	{
	  // Read full value into memory to set it
	  char *val_buf = malloc (vlen + 1);
	  if (!val_buf)
	    return false;

	  if (!read_exact_crc (file_ptr, val_buf, vlen, crc))
	    {
	      free (val_buf);
	      return false;
	    }
	  val_buf[vlen] = '\0';

	  // Insert into Hash
	  if (ent)
	    {
	      hash_set (ent->hash, field_buf, flen, val_buf, vlen);
	    }
	  free (val_buf);
	}
    }
  return true;
}

static bool
load_list_entry (FILE *file_ptr, Minis *minis, const char *key,
		 size_t key_len,
		 uint64_t expire_at, uint32_t *crc, bool skip)
{
  uint32_t count;
  if (!read_u32_crc (file_ptr, &count, crc))
    return false;

  Entry *ent = NULL;
  if (!skip)
    {
      ent = entry_new_list (minis, key, key_len);
      if (!ent)
	return false;
      if (expire_at > 0)
	entry_set_expiration (minis, ent, expire_at);
    }

  for (uint32_t i = 0; i < count; i++)
    {
      uint32_t vlen;
      if (!read_u32_crc (file_ptr, &vlen, crc))
	return false;
      if (vlen > K_MAX_MSG)
	{
	  msgf ("Snapshot: list value too long (%u)", vlen);
	  return false;
	}

      // Read even when skipping, to keep the stream and CRC in sync
      char *val_buf = malloc (vlen + 1);
      if (!val_buf)
	return false;
      if (!read_exact_crc (file_ptr, val_buf, vlen, crc))
	{
	  free (val_buf);
	  return false;
	}
      val_buf[vlen] = '\0';

      bool pushed = !ent || list_push (ent->list, val_buf, vlen, false);
      free (val_buf);
      if (!pushed)
	return false;
    }

  // Lists are never empty, drop one from a bad file
  if (ent && list_len (ent->list) == 0)
    entry_dispose_atomic (minis, ent);
  return true;
}

// Reads data to update CRC/Stream, but ignores the result if skip=true
static bool
load_string_entry (FILE *file_ptr, Minis *minis, const char *key,
		 size_t key_len,
		   uint64_t expire_at, uint32_t *crc, bool skip)
{
  uint32_t vlen;
  if (!read_u32_crc (file_ptr, &vlen, crc))
    return false;

  // OPTIMIZATION: If skipping, don't malloc the whole string. 
  // Read in chunks just to update CRC.
  if (skip)
    {
      char dump_buf[1024];
      size_t remain = vlen;
      while (remain > 0)
	{
	  size_t chunk =
	    (remain < sizeof (dump_buf)) ? remain : sizeof (dump_buf);
	  if (!read_exact_crc (file_ptr, dump_buf, chunk, crc))
	    return false;
	  remain -= chunk;
	}
      return true;		// Done successfully (ignored)
    }

  // Normal Loading Logic
  char *val = malloc (vlen + 1);
  if (!val)
    return false;

  if (!read_exact_crc (file_ptr, val, vlen, crc))
    {
      free (val);
      return false;
    }
  val[vlen] = '\0';

  Entry *ent = entry_new_str (minis, key, key_len, val, vlen);
  if (!ent)
    return false;
  if (expire_at > 0)
    entry_set_expiration (minis, ent, expire_at);
  free (val);
  return true;
}

static bool
load_zset_entry (FILE *file_ptr, Minis *minis, const char *key,
		 size_t key_len,
		 uint64_t expire_at, uint32_t *crc, bool skip)
{
  uint32_t count;
  if (!read_u32_crc (file_ptr, &count, crc))
    return false;

  Entry *ent = NULL;

  // Only create the object if we aren't skipping
  if (!skip)
    {
      ent = entry_new_zset (minis, key, key_len);
      if (!ent)
	return false;
      if (expire_at > 0)
	entry_set_expiration (minis, ent, expire_at);
    }

  for (uint32_t i = 0; i < count; i++)
    {
      double score;
      if (!read_double_crc (file_ptr, &score, crc))
	return false;

      uint8_t sub_type;
      if (!read_exact_crc (file_ptr, &sub_type, 1, crc) || sub_type != T_STR)
	{
	  msgf ("Snapshot: invalid ZSet member format");
	  return false;
	}

      uint32_t nlen;
      if (!read_u32_crc (file_ptr, &nlen, crc))
	return false;

      // We must read the name to keep stream synced
      char name_buf[K_MAX_MSG];
      if (nlen > K_MAX_MSG)
	return false;
      if (!read_exact_crc (file_ptr, name_buf, nlen, crc))
	return false;

      // Only add if not skipping
      if (!skip && ent)
	{
	  zset_add (ent->zset, name_buf, nlen, score);
	}
    }

  return true;
}

static bool
load_one_entry (FILE *file_ptr, const SnapClock *clock, uint32_t version,
		Minis *minis, uint32_t *crc, bool *is_eof)
{
  uint64_t expire_net;
  size_t read_count = fread (&expire_net, 1, 8, file_ptr);

  if (read_count == 0)
    {
      if (feof (file_ptr))
	{
	  *is_eof = true;
	  return true;		// Not an error, just done
	}
      return false;		// Read error
    }

  *is_eof = false;

  if (read_count != 8)
    {
      msgf ("Snapshot: truncated expiration field");
      return false;
    }
  *crc = crc32_update (*crc, (uint8_t *) & expire_net, 8);
  uint64_t expire_at = ntoh_u64 (expire_net);
  bool skip = false;
  if (version == 1)
    // Monotonic: right only if the machine did not reboot since
    skip = expire_at > 0 && expire_at < clock->mono_us;
  else
    expire_at = expiry_from_wall (expire_at, clock, &skip);
  uint32_t klen;
  if (!read_u32_crc (file_ptr, &klen, crc))
    return false;
  // key_buf needs room for the NUL after the key
  if (klen >= K_MAX_MSG)
    {
      msgf ("Snapshot: key too long (%u)", klen);
      return false;
    }

  char key_buf[K_MAX_MSG];
  if (!read_exact_crc (file_ptr, key_buf, klen, crc))
    return false;
  key_buf[klen] = '\0';

  uint8_t type;
  if (!read_exact_crc (file_ptr, &type, 1, crc))
    return false;

  if (type == T_STR)
    return load_string_entry (file_ptr, minis, key_buf, klen, expire_at, crc,
			    skip);
  if (type == T_ZSET)
    return load_zset_entry (file_ptr, minis, key_buf, klen, expire_at, crc,
			    skip);
  if (type == T_HASH)
    return load_hash_entry (file_ptr, minis, key_buf, klen, expire_at, crc,
			    skip);
  if (type == T_LIST)
    return load_list_entry (file_ptr, minis, key_buf, klen, expire_at, crc,
			    skip);

  msgf ("Snapshot: unknown type %u for key %s", type, key_buf);
  return false;
}

static void
cleanup_failed_save (FILE *file_ptr, const char *tmp_filename)
{
  msgf ("Snapshot: write error: %s", strerror (errno));
  if (file_ptr)
    fclose (file_ptr);
  if (tmp_filename)
    unlink (tmp_filename);
}

// ============================================================================
// CORE SERIALIZATION LOGIC (Refactored)
// ============================================================================

// Serializes entry into buf, growing buf (malloc'd memory) until it
// fits. Snapshots run in the child or at shutdown, not on the request
// path, so this allocation is fine.
static bool
serialize_entry_growing (Buffer *buf, Entry *entry, const SnapClock *clock)
{
  // Only running out of space is worth retrying
  if (entry->type != T_STR && entry->type != T_ZSET
      && entry->type != T_HASH && entry->type != T_LIST)
    return false;

  while (true)
    {
      buf_clear (buf);
      if (serialize_entry (buf, entry, clock))
	return true;

      size_t new_cap = buf->capacity * 2;
      if (new_cap < buf->capacity || new_cap > ((size_t) 1 << 40))
	return false;
      uint8_t *mem = realloc (buf->data, new_cap);
      if (!mem)
	return false;
      buf->data = mem;
      buf->capacity = new_cap;
    }
}

/**
 * Serializes all entries in a single Shard to the file stream.
 * * @param buf Scratch buffer of malloc'd memory, which may be grown
 * @param shard The locked shard to save
 * @param running_crc Pointer to accumulate CRC
 * @param file_ptr Destination file stream
 * @param now_us Current time for expiry filtering
 */
static bool
save_shard_entries_to_stream (Buffer *buf, const Shard *shard,
			      uint32_t *running_crc, FILE *file_ptr,
			      const SnapClock *clock)
{
  HMIter iter;
  hm_iter_init (&shard->db, &iter);

  HNode *node;
  while ((node = hm_iter_next (&iter)) != NULL)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Entry *entry = container_of (node, Entry, node);
#pragma GCC diagnostic pop

      // Skip expired or invalid entries
      if (!entry
	  || (entry->expire_at_us != 0 && entry->expire_at_us < clock->mono_us))
	continue;

      // Fail the save rather than skip the entry: the new file replaces
      // the old one, which may hold the entry.
      if (!serialize_entry_growing (buf, entry, clock))
	{
	  msgf ("Snapshot: cannot serialize %s (%zu bytes tried)",
		entry->key, buf->capacity);
	  return false;
	}

      *running_crc = crc32_update (*running_crc, buf->data, buf->length);

      if (fwrite (buf->data, 1, buf->length, file_ptr) != buf->length)
	return false;
    }
  return true;
}

bool
minis_save_shard_file (const Shard *shard, const char *filename,
		       uint64_t now_us)
{
  char tmp_filename[256];
  snprintf (tmp_filename, sizeof (tmp_filename), "%s.tmp", filename);

  FILE *file_ptr = fopen (tmp_filename, "wb");
  if (!file_ptr)
    {
      msgf ("Snapshot: failed to open temp file: %s", tmp_filename);
      return false;
    }

  long crc_offset;
  uint32_t item_count = (uint32_t) hm_size (&shard->db);

  if (!write_file_header (file_ptr, &crc_offset, item_count))
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }

  size_t buf_cap = K_MAX_MSG + 1024;
  uint8_t *raw_mem = malloc (buf_cap);
  if (!raw_mem)
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }
  Buffer buf = buf_init (raw_mem, buf_cap);

  uint32_t running_crc = 0;
  SnapClock clock = snap_clock_now (now_us);
  if (!save_shard_entries_to_stream
      (&buf, shard, &running_crc, file_ptr, &clock))
    {
      free (buf.data);
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }
  free (buf.data);
  if (!patch_crc (file_ptr, crc_offset, running_crc))
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }

  fseek (file_ptr, 0, SEEK_END);
  if (fflush (file_ptr) != 0 || fsync (fileno (file_ptr)) != 0)
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }
  fclose (file_ptr);

  if (rename (tmp_filename, filename) != 0)
    {
      msgf ("Snapshot: rename failed: %s", strerror (errno));
      unlink (tmp_filename);
      return false;
    }

  return true;
}

bool
cache_save_to_file (const Minis *minis, const char *filename, uint64_t now_us)
{
  char tmp_filename[256];
  snprintf (tmp_filename, sizeof (tmp_filename), "%s.tmp", filename);

  FILE *file_ptr = fopen (tmp_filename, "wb");
  if (!file_ptr)
    {
      msgf ("Snapshot: failed to open temp file: %s", tmp_filename);
      return false;
    }

  // 1. Calculate Total Count
  size_t total_count = 0;
  for (int i = 0; i < NUM_SHARDS; ++i)
    total_count += hm_size (&minis->shards[i].db);

  long crc_offset;
  if (!write_file_header (file_ptr, &crc_offset, (uint32_t) total_count))
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }

  // 2. Init Scratch Buffer
  size_t buf_cap = K_MAX_MSG + 1024;
  uint8_t *raw_mem = malloc (buf_cap);
  if (!raw_mem)
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }
  Buffer buf = buf_init (raw_mem, buf_cap);

  // 3. Loop ALL shards and dump to same stream
  uint32_t running_crc = 0;
  SnapClock clock = snap_clock_now (now_us);
  for (int i = 0; i < NUM_SHARDS; ++i)
    {
      if (!save_shard_entries_to_stream
	  (&buf, &minis->shards[i], &running_crc, file_ptr, &clock))
	{
	  free (buf.data);
	  cleanup_failed_save (file_ptr, tmp_filename);
	  return false;
	}
    }
  free (buf.data);

  // 4. Finalize
  if (!patch_crc (file_ptr, crc_offset, running_crc))
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }

  fseek (file_ptr, 0, SEEK_END);
  if (fflush (file_ptr) != 0 || fsync (fileno (file_ptr)) != 0)
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }
  fclose (file_ptr);

  if (rename (tmp_filename, filename) != 0)
    {
      msgf ("Snapshot: rename failed: %s", strerror (errno));
      unlink (tmp_filename);
      return false;
    }

  return true;
}

bool
cache_load_from_file (Minis *minis, const char *filename, int shard_id_hint,
		      uint64_t now_us)
{
  FILE *file_ptr = fopen (filename, "rb");
  if (!file_ptr)
    return false;

  uint32_t expected_crc;
  uint32_t item_count = 0;
  uint32_t version;
  if (!read_and_verify_header (file_ptr, &expected_crc, &item_count,
			       &version))
    {
      fclose (file_ptr);
      return false;
    }

  if (item_count > 0)
    {
      if (shard_id_hint == -1)
	{
	  uint32_t per_shard = (item_count / NUM_SHARDS) + 1;
	  for (int i = 0; i < NUM_SHARDS; i++)
	    {
	      if (hm_size (&minis->shards[i].db) == 0)
		{		// ONLY IF EMPTY
		  hm_destroy (&minis->shards[i].db);
		  hm_init (&minis->shards[i].db, per_shard);
		}
	    }
	}
      else if (shard_id_hint >= 0 && shard_id_hint < NUM_SHARDS)
	{
	  Shard *target_shard = &minis->shards[shard_id_hint];
	  if (hm_size (&target_shard->db) == 0)
	    {
	      msgf ("Snapshot: Shard %d pre-allocating for %u items.",
		    shard_id_hint, item_count);
	      hm_destroy (&target_shard->db);
	      if (!hm_init (&target_shard->db, item_count))
		{
		  fclose (file_ptr);
		  return false;
		}
	    }
	  else
	    {
	      msgf
		("Snapshot: Shard %d not empty (detected migration). Skipping pre-alloc.",
		 shard_id_hint);
	    }
	}
    }

  // --------------------------------------------------

  uint32_t running_crc = 0;
  SnapClock clock = snap_clock_now (now_us);
  while (true)
    {
      bool is_eof = false;
      // load_one_entry internally hashes the key. 
      // If shard_id_hint is correct, the key will naturally fall into that shard.
      if (!load_one_entry (file_ptr, &clock, version, minis, &running_crc,
			   &is_eof))
	{
	  fclose (file_ptr);
	  return false;
	}
      if (is_eof)
	break;
    }

  bool result = true;
  if (running_crc != expected_crc)
    {
      msgf ("Snapshot: CRC mismatch! Expected %08x, got %08x",
	    expected_crc, running_crc);
      result = false;
    }
  fclose (file_ptr);
  return result;
}
