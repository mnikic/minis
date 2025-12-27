#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h> // For htonl, ntohl

#include "common/common.h"
#include "io/buffer.h"
#include "cache/cache.h"
#include "cache/zset.h"

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

static bool
read_u32 (FILE *file_ptr, uint32_t *out)
{
  uint32_t net;
  if (fread(&net, 1, 4, file_ptr) != 4) return false;
  *out = ntoh_u32 (net);
  return true;
}

// Helper context to avoid passing globals
static bool
read_exact_crc (FILE *file_ptr, void *buf, size_t len, uint32_t *crc_ptr)
{
  if (fread (buf, 1, len,file_ptr) != len) return false;
  // Update the running CRC with the data we just read
  *crc_ptr = crc32_update(*crc_ptr, (const uint8_t *)buf, len);
  return true;
}

// Update your type readers to accept the CRC pointer
static bool
read_u32_crc (FILE *file_ptr, uint32_t *out, uint32_t *crc_ptr)
{
  uint32_t net;
  if (!read_exact_crc (file_ptr, &net, 4, crc_ptr)) return false;
  *out = ntoh_u32 (net);
  return true;
}

static bool
read_double_crc (FILE *file_ptr , double *out, uint32_t *crc_ptr)
{
  // Assumes IEEE 754 compatibility
  return read_exact_crc (file_ptr , out, 8, crc_ptr);
}

// Do the same for read_u64, read_double, etc.

// --- Serialization Helpers ---

// 1. Ensure this function ONLY writes [LEN] + [BYTES]
static bool
out_raw_str (Buffer *buf, const char *str, size_t len)
{
  if (!buf_append_u32 (buf, (uint32_t)len))
    return false;
  
  if (!buf_append_bytes (buf, (const uint8_t *)str, len))
    return false;

  return true;
}

// 2. Ensure this function writes [TYPE] + [RAW STR]
static bool
out_typed_str (Buffer *buf, const char *str)
{
  // Write the Type (0x00)
  if (!buf_append_byte (buf, T_STR))
    return false;
  
  // CALL RAW STR directly. 
  // Do NOT call 'out_str' if 'out_str' also adds a type byte!
  return out_raw_str (buf, str, strlen(str));
}

// --- ZSet Serialization ---

/**
 * Recursively writes ZSet nodes into the buffer.
 * Format for each node: [SCORE (8b)] [T_STR (1b)] [LEN (4b)] [NAME (N bytes)]
 */
static bool
zset_node_to_buf (Buffer *buf, AVLNode *node)
{
  if (!node)
    return true;

  // 1. Traverse Left (In-order)
  if (!zset_node_to_buf (buf, node->left))
    return false;

  // 2. Serialize Current Node
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
  ZNode *znode = container_of (node, ZNode, tree);
#pragma GCC diagnostic pop

  // Write Score (8 bytes raw double)
  if (!buf_append_double (buf, znode->score))
    return false;

  // Write Name
  // We explicitly write the T_STR marker here to satisfy the loader's check.
  if (!buf_append_byte (buf, T_STR))
    return false;

  // Write Name Length + Bytes
  if (!out_raw_str (buf, znode->name, znode->len))
    return false;

  // 3. Traverse Right
  if (!zset_node_to_buf (buf, node->right))
    return false;

  return true;
}

/**
 * Writes the ZSet header and payload.
 * Format: [T_ZSET(1b)] [COUNT (4b)] [NODES...]
 */
static bool
out_zset (Buffer *buf, ZSet *zset)
{
  // 1. Write Type Marker
  if (!buf_append_byte (buf, T_ZSET))
    return false;

  // 2. Write Count
  // We use the Tree count, ignoring the redundant HMap
  uint32_t count = (zset && zset->tree) ? zset->tree->cnt : 0;
  if (!buf_append_u32 (buf, count))
    return false;

  // 3. Write Nodes (if any)
  if (count > 0)
    {
      return zset_node_to_buf (buf, zset->tree);
    }
  return true;
}

// --- File Operations Helpers ---

static inline void
err_no_memory (FILE *file_ptr, char tmp_filename[256])
{
  msgf ("Snapshot: write error: %s", strerror (errno));
  if (file_ptr) fclose (file_ptr);
  unlink (tmp_filename);
}

// --- Main Save Function ---

bool
cache_save_to_file (const Cache *cache, const char *filename)
{
  // 1. Prepare Temporary Filename
  // We write to a temp file first to ensure atomic updates.
  char tmp_filename[256];
  snprintf (tmp_filename, sizeof (tmp_filename), "%s.tmp", filename);

  FILE *file_ptr = fopen (tmp_filename, "wb");
  if (!file_ptr)
    {
      msgf ("Snapshot: failed to open temp file: %s", tmp_filename);
      return false;
    }

  // 2. Write File Header
  
  // A. Magic Signature
  if (fwrite (MINIS_DB_MAGIC, 1, 4, file_ptr) != 4)
    {
      err_no_memory (file_ptr, tmp_filename);
      return false;
    }

  // B. CRC32 Placeholder
  // We remember the position (byte offset 4) to overwrite it later.
  long crc_offset = ftell (file_ptr);
  uint32_t zero_crc = 0;
  if (fwrite (&zero_crc, 4, 1, file_ptr) != 1)
    {
      err_no_memory (file_ptr, tmp_filename);
      return false;
    }

  // C. Version
  uint32_t ver = htonl (MINIS_DB_VERSION);
  if (fwrite (&ver, 4, 1, file_ptr) != 1)
    {
      err_no_memory (file_ptr, tmp_filename);
      return false;
    }

  // --- Start CRC Calculation Scope ---
  uint32_t running_crc = 0;

  // 3. Allocate Serialization Buffer
  size_t buf_cap = K_MAX_MSG + 1024;
  uint8_t *raw_mem = malloc (buf_cap);
  if (!raw_mem)
    {
      msgf ("Snapshot: failed to allocate buffer");
      err_no_memory (file_ptr, tmp_filename);
      return false;
    }

  Buffer buf = buf_init (raw_mem, buf_cap);

  // 4. Iterate Cache
  HMIter iter;
  hm_iter_init (&cache->db, &iter);

  HNode *node;
  while ((node = hm_iter_next (&iter)) != NULL)
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-align"
      Entry *entry = container_of (node, Entry, node);
#pragma GCC diagnostic pop

      buf_clear (&buf);

      // --- SERIALIZATION ---

      // A. Entry Metadata: [EXPIRE] [KEY_LEN] [KEY]
      if (!buf_append_i64 (&buf, (int64_t) entry->expire_at_us)) continue;

      uint32_t klen = (uint32_t) strlen (entry->key);
      if (!buf_append_u32 (&buf, klen)) continue;
      if (!buf_append_bytes (&buf, (const uint8_t *)entry->key, klen)) continue;

      // B. Entry Value
      bool success = false;
      
      if (entry->type == T_STR)
        {
          // out_typed_str writes: [T_STR] [LEN] [BYTES]
          success = out_typed_str (&buf, entry->val);
        }
      else if (entry->type == T_ZSET)
        {
          // out_zset writes: [T_ZSET] [COUNT] [ITEMS...]
          success = out_zset (&buf, entry->zset);
        }
      else
        {
           msgf ("Snapshot: unknown type, %s", entry->key);
           continue;
        }

      if (!success)
        {
          msgf ("Snapshot: entry too large, skipping: %s", entry->key);
          continue;
        }

      // --- CRC UPDATE ---
      // Update the checksum with the serialized buffer BEFORE writing.
      running_crc = crc32_update(running_crc, buf.data, buf.length);

      // C. Flush to disk
      if (fwrite (buf.data, 1, buf.length, file_ptr) != buf.length)
        {
          free (raw_mem);
          err_no_memory (file_ptr, tmp_filename);
          return false;
        }
    }

  free (raw_mem);

  // 5. Patch the Header with the Calculated CRC
  // Jump back to the placeholder position.
  if (fseek (file_ptr, crc_offset, SEEK_SET) != 0) 
  {
    err_no_memory (file_ptr, tmp_filename);
    return false;
  }

  // Write the final CRC (Network Byte Order)
  uint32_t final_crc = htonl (running_crc);
  if (fwrite (&final_crc, 4, 1, file_ptr) != 1) 
  {
    goto err_no_mem;
  }

  // Jump back to end of file (good practice, though we are closing anyway)
  fseek (file_ptr, 0, SEEK_END);

  // 6. Durability
  if (fflush (file_ptr) != 0) goto err_no_mem;
  if (fsync (fileno (file_ptr)) != 0) goto err_no_mem;
  fclose (file_ptr);

  // 7. Atomic Rename
  if (rename (tmp_filename, filename) != 0)
    {
      msgf ("Snapshot: rename failed: %s", strerror (errno));
      unlink (tmp_filename);
      return false;
    }

  return true;
err_no_mem:
  err_no_memory (file_ptr, tmp_filename);
  return false;
}

// --- Main Load Function ---

bool
cache_load_from_file (Cache *cache, const char *filename)
{
  FILE *file_ptr = fopen (filename, "rb");
  if (!file_ptr)
    {
      // If file doesn't exist, it's a fresh start, not an error.
      if (errno == ENOENT) return false;
      msgf ("Snapshot: failed to open file: %s", strerror (errno));
      return false;
    }

  // 1. Verify Magic
  char magic[4];
  if (fread (magic, 1, 4, file_ptr) != 4 || memcmp (magic, MINIS_DB_MAGIC, 4) != 0)
    {
      msgf ("Snapshot: invalid magic signature");
      fclose (file_ptr);
      return false;
    }

  // 2. Read Expected CRC (Header)
  // We do not CRC the header itself.
  uint32_t expected_crc;
  if (!read_u32 (file_ptr, &expected_crc))
    {
      msgf ("Snapshot: failed to read CRC");
      fclose (file_ptr);
      return false;
    }

  // 3. Read Version
  uint32_t ver;
  if (!read_u32 (file_ptr, &ver))
    {
      msgf ("Snapshot: failed to read version");
      fclose (file_ptr);
      return false;
    }

  if (ver != MINIS_DB_VERSION)
    {
      msgf ("Snapshot: unsupported version %u", ver);
      fclose (file_ptr);
      return false;
    }

  // --- Start Body CRC Calculation ---
  // We start calculating CRC from this point onwards (the Payload).
  // Note: This assumes your Save function resets CRC to 0 after writing the Version.
  uint32_t running_crc = 0; 

  msgf ("Snapshot: loading data (v%u)...", ver);

  // 4. Iterate Records
  while (true)
    {
      // A. Read Metadata: [EXPIRE (8b)]
      // We read this manually to handle EOF cleanly.
      uint64_t expire_net; // Network byte order
      size_t read_count = fread(&expire_net, 1, 8, file_ptr);
      
      if (read_count == 0) 
        {
          // Clean EOF: We are done.
          break; 
        }
      if (read_count != 8) 
        {
          // Truncated file (error)
          goto err; 
        }

      // Update CRC with the expiration bytes we just read
      running_crc = crc32_update(running_crc, (uint8_t*)&expire_net, 8);
      uint64_t expire_at = ntoh_u64(expire_net);

      // B. Read Key Length
      uint32_t klen;
      if (!read_u32_crc (file_ptr, &klen, &running_crc)) goto err;

      if (klen > K_MAX_MSG)
        {
          msgf ("Snapshot: key too long (%u)", klen);
          goto err;
        }

      // C. Read Key Bytes
      char key_buf[K_MAX_MSG];
      if (!read_exact_crc (file_ptr, key_buf, klen, &running_crc)) goto err;
      key_buf[klen] = '\0';

      // D. Read Type
      uint8_t type;
      if (!read_exact_crc (file_ptr, &type, 1, &running_crc)) goto err;

      // E. Read Value based on Type
      if (type == T_STR)
        {
          // Format: [LEN] [BYTES]
          uint32_t vlen;
          if (!read_u32_crc (file_ptr, &vlen, &running_crc)) goto err;

          char *val = malloc (vlen + 1);
          if (!val) goto err;

          if (!read_exact_crc (file_ptr, val, vlen, &running_crc))
            {
              free (val);
              goto err;
            }
          val[vlen] = '\0';

          // Insert into Cache
          Entry *ent = entry_new_str (cache, key_buf, val);
          if (expire_at > 0)
            {
               // Assuming you have a way to set absolute expiration
               ent->expire_at_us = expire_at; 
            }
        }
      else if (type == T_ZSET)
        {
          // Format: [COUNT] [ (SCORE) (T_STR) (LEN) (NAME) ... ]
          uint32_t count;
          if (!read_u32_crc (file_ptr, &count, &running_crc)) goto err;

          Entry *ent = entry_new_zset (cache, key_buf);
          if (!ent) goto err;

          for (uint32_t i = 0; i < count; i++)
            {
              double score;
              if (!read_double_crc (file_ptr, &score, &running_crc)) goto err;

              // Check mandatory T_STR marker
              uint8_t sub_type;
              if (!read_exact_crc (file_ptr, &sub_type, 1, &running_crc) || sub_type != T_STR)
                {
                  msgf ("Snapshot: invalid ZSet member format");
                  goto err;
                }

              uint32_t nlen;
              if (!read_u32_crc (file_ptr, &nlen, &running_crc)) goto err;
              if (nlen > K_MAX_MSG) goto err;

              char name_buf[K_MAX_MSG];
              if (!read_exact_crc (file_ptr, name_buf, nlen, &running_crc)) goto err;
              
              zset_add (ent->zset, name_buf, nlen, score);
            }

          if (expire_at > 0)
            {
              ent->expire_at_us = expire_at;
            }
        }
      else
        {
          msgf ("Snapshot: unknown type %u for key %s", type, key_buf);
          goto err;
        }
    }

  // 5. Verify CRC
  if (running_crc != expected_crc)
    {
      msgf ("Snapshot: CRC mismatch! Expected %08x, got %08x. File corrupted.", expected_crc, running_crc);
      goto err;
    }

  fclose (file_ptr);
  msgf ("Snapshot: load complete.");
  return true;

err:
  msgf ("Snapshot: file corrupted, read error, or checksum mismatch.");
  fclose (file_ptr);
  // Optional: clear the cache if you want atomic success/failure
  return false;
}
