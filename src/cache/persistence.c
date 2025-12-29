// ============================================================================
// Persistence Layer - Save/Load Cache to Disk
// ============================================================================

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>

#include "common/common.h"
#include "io/buffer.h"
#include "cache/minis.h"
#include "cache/entry.h"
#include "cache/zset.h"
#include "cache/hash.h"

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
// Read Helpers (with CRC tracking)
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

// ============================================================================
// Write Helpers
// ============================================================================

static bool
out_raw_str (Buffer *buf, const char *str, size_t len)
{
  return buf_append_u32 (buf, (uint32_t) len) &&
    buf_append_bytes (buf, (const uint8_t *) str, len);
}

static bool
out_typed_str (Buffer *buf, const char *str)
{
  return buf_append_byte (buf, T_STR) && out_raw_str (buf, str, strlen (str));
}

// ============================================================================
// ZSet Serialization
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

  // Write the number of fields
  uint32_t count = (uint32_t) hm_size (hmap);
  if (!buf_append_u32 (buf, count))
    return false;

  // Iterate over the hash map
  HMIter iter;
  hm_iter_init (hmap, &iter);
  HNode *node;
  while ((node = hm_iter_next (&iter)) != NULL)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      HashEntry *hash_entry = container_of (node, HashEntry, node);
#pragma GCC diagnostic pop

      // Write Field
      if (!out_raw_str (buf, hash_entry->field, strlen (hash_entry->field)))
	return false;
      // Write Value
      if (!out_raw_str (buf, hash_entry->value, strlen (hash_entry->value)))
	return false;
    }
  return true;
}

// ============================================================================
// Entry Serialization
// ============================================================================

static bool
serialize_entry (Buffer *buf, Entry *entry)
{
  if (!buf_append_i64 (buf, (int64_t) entry->expire_at_us))
    return false;

  uint32_t klen = (uint32_t) strlen (entry->key);
  if (!buf_append_u32 (buf, klen))
    return false;
  if (!buf_append_bytes (buf, (const uint8_t *) entry->key, klen))
    return false;

  if (entry->type == T_STR)
    return out_typed_str (buf, entry->val);
  if (entry->type == T_ZSET)
    return serialize_zset (buf, entry->zset);
  if (entry->type == T_HASH)
    return serialize_hash (buf, entry->hash);

  return false;
}

// ============================================================================
// File Header Operations
// ============================================================================

static bool
write_file_header (FILE *file_ptr, long *crc_offset)
{
  if (fwrite (MINIS_DB_MAGIC, 1, 4, file_ptr) != 4)
    return false;

  *crc_offset = ftell (file_ptr);
  uint32_t zero_crc = 0;
  if (fwrite (&zero_crc, 4, 1, file_ptr) != 1)
    return false;

  uint32_t ver = htonl (MINIS_DB_VERSION);
  return fwrite (&ver, 4, 1, file_ptr) == 1;
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
read_and_verify_header (FILE *file_ptr, uint32_t *expected_crc)
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

  if (ver != MINIS_DB_VERSION)
    {
      msgf ("Snapshot: unsupported version %u", ver);
      return false;
    }

  return true;
}

// ============================================================================
// Entry Deserialization
// ============================================================================

static bool
load_hash_entry (FILE *file_ptr, Minis *minis, const char *key,
		 uint64_t expire_at, uint32_t *crc, bool skip)
{
  uint32_t count;
  if (!read_u32_crc (file_ptr, &count, crc))
    return false;

  Entry *ent = NULL;
  if (!skip)
    {
      ent = entry_new_hash (minis, key);
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
	      hash_set (ent->hash, field_buf, val_buf);
	    }
	  free (val_buf);
	}
    }
  return true;
}

// Reads data to update CRC/Stream, but ignores the result if skip=true
static bool
load_string_entry (FILE *file_ptr, Minis *minis, const char *key,
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

  Entry *ent = entry_new_str (minis, key, val);
  if (!ent)
    return false;
  if (expire_at > 0)
    entry_set_expiration (minis, ent, expire_at);
  free (val);
  return true;
}

static bool
load_zset_entry (FILE *file_ptr, Minis *minis, const char *key,
		 uint64_t expire_at, uint32_t *crc, bool skip)
{
  uint32_t count;
  if (!read_u32_crc (file_ptr, &count, crc))
    return false;

  Entry *ent = NULL;

  // Only create the object if we aren't skipping
  if (!skip)
    {
      ent = entry_new_zset (minis, key);
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
load_one_entry (FILE *file_ptr, uint64_t now_us, Minis *minis, uint32_t *crc,
		bool *is_eof)
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
  if (expire_at > 0 && expire_at < now_us)
    {
      skip = true;
    }
  uint32_t klen;
  if (!read_u32_crc (file_ptr, &klen, crc))
    return false;
  if (klen > K_MAX_MSG)
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
    return load_string_entry (file_ptr, minis, key_buf, expire_at, crc, skip);
  if (type == T_ZSET)
    return load_zset_entry (file_ptr, minis, key_buf, expire_at, crc, skip);
  if (type == T_HASH)
    return load_hash_entry (file_ptr, minis, key_buf, expire_at, crc, skip);

  msgf ("Snapshot: unknown type %u for key %s", type, key_buf);
  return false;
}

// ============================================================================
// Cleanup Helper
// ============================================================================

static void
cleanup_failed_save (FILE *file_ptr, char *tmp_filename)
{
  msgf ("Snapshot: write error: %s", strerror (errno));
  if (file_ptr)
    fclose (file_ptr);
  unlink (tmp_filename);
}

// ============================================================================
// Main Save Function
// ============================================================================

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

  long crc_offset;
  if (!write_file_header (file_ptr, &crc_offset))
    {
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }

  uint32_t running_crc = 0;
  size_t buf_cap = K_MAX_MSG + 1024;
  uint8_t *raw_mem = malloc (buf_cap);
  if (!raw_mem)
    {
      msgf ("Snapshot: failed to allocate buffer");
      cleanup_failed_save (file_ptr, tmp_filename);
      return false;
    }

  Buffer buf = buf_init (raw_mem, buf_cap);
  HMIter iter;
  hm_iter_init (&minis->db, &iter);

  HNode *node;
  while ((node = hm_iter_next (&iter)) != NULL)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Entry *entry = container_of (node, Entry, node);
#pragma GCC diagnostic pop
      if (!entry
	  || (entry->expire_at_us != 0 && entry->expire_at_us < now_us))
	continue;

      buf_clear (&buf);

      if (!serialize_entry (&buf, entry))
	{
	  msgf ("Snapshot: entry too large, skipping: %s", entry->key);
	  continue;
	}

      running_crc = crc32_update (running_crc, buf.data, buf.length);

      if (fwrite (buf.data, 1, buf.length, file_ptr) != buf.length)
	{
	  free (raw_mem);
	  cleanup_failed_save (file_ptr, tmp_filename);
	  return false;
	}
    }

  free (raw_mem);

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

// ============================================================================
// Main Load Function
// ============================================================================

bool
cache_load_from_file (Minis *minis, const char *filename, uint64_t now_us)
{
  FILE *file_ptr = fopen (filename, "rb");
  if (!file_ptr)
    {
      if (errno == ENOENT)
	return false;		// Fresh start
      msgf ("Snapshot: failed to open file: %s", strerror (errno));
      return false;
    }

  uint32_t expected_crc;
  if (!read_and_verify_header (file_ptr, &expected_crc))
    {
      fclose (file_ptr);
      return false;
    }

  msgf ("Snapshot: loading data (v%u)...", MINIS_DB_VERSION);

  uint32_t running_crc = 0;

  while (true)
    {
      bool is_eof = false;
      if (!load_one_entry (file_ptr, now_us, minis, &running_crc, &is_eof))
	{
	  // Logic error encountered (printed inside helper)
	  fclose (file_ptr);
	  return false;
	}
      if (is_eof)
	{
	  break;		// Clean exit
	}
    }

  if (running_crc != expected_crc)
    {
      msgf ("Snapshot: CRC mismatch! Expected %08x, got %08x",
	    expected_crc, running_crc);
      fclose (file_ptr);
      return false;
    }

  fclose (file_ptr);
  msgf ("Snapshot: load complete.");
  return true;
}
