/*
 *============================================================================
 * Name             : proto_parser.c
 * Author           : Milos
 * Description      : Zero-copy protocol parser using in-place modification.
 *============================================================================
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <assert.h>
#include <arpa/inet.h>

#include "common/common.h"
#include "common/macros.h"
#include "proto_parser.h"

// Validate the basic request structure
// Returns error code, or VALIDATE_OK on success
ValidationResult
validate_request_header (const uint8_t *req, uint32_t reqlen,
			 uint32_t *out_arg_count)
{
  if (reqlen < 4)
    return VALIDATE_TOO_SHORT;

  uint32_t num = 0;
  memcpy (&num, &req[0], 4);
  num = ntohl (num);

  if (num > K_MAX_ARGS)
    return VALIDATE_TOO_MANY_ARGS;

  if (num < 1)
    return VALIDATE_TOO_FEW_ARGS;

  *out_arg_count = num;
  return VALIDATE_OK;
}

// Optimization: Force inline since this runs in a tight loop
static ALWAYS_INLINE void
save_and_nullterm (RestoreState *state, uint8_t *location,
		   uint8_t original_char)
{
  assert (state->restore_count < K_MAX_ARGS);
  state->restore_ptrs[state->restore_count] = location;
  state->restore_chars[state->restore_count] = original_char;
  state->restore_count++;
  *location = '\0';
}

// Parse arguments from request buffer into cmd array
// Uses in-place null-termination with automatic restoration tracking
ParseResult 
parse_arguments (ProtoRequest *proto_request)
{
  restore_state_init(proto_request->restore);
  size_t pos = 4;
  size_t cmd_size = 0;
  
  // Cache the pointer locally for speed/readability
  uint8_t* req = proto_request->req; 

  for (uint32_t i = 0; i < proto_request->arg_count; i++)
    {
      // Check for length header
      if (pos + 4 > proto_request->reqlen)
        return PARSE_MISSING_LENGTH;

      // Read argument length
      uint32_t arg_len = 0;
      memcpy (&arg_len, &req[pos], 4);
      arg_len = ntohl (arg_len);

      // Check if argument data fits
      if (pos + 4 + arg_len > proto_request->reqlen)
        return PARSE_LENGTH_OVERFLOW;

      // Get pointer to null-termination location.
      uint8_t *null_term_loc = &req[pos + 4 + arg_len];
      uint8_t original_char = *null_term_loc;

      // Save and null-terminate
      save_and_nullterm (proto_request->restore, null_term_loc, original_char);

      // Store pointer to argument string
      proto_request->cmd[cmd_size++] = (char *) &req[pos + 4];

      // Advance position
      pos += 4 + arg_len;
    }

  // Verify no trailing data
  if (pos != proto_request->reqlen)
    return PARSE_TRAILING_DATA;

  proto_request->cmd_size = cmd_size;
  return PARSE_OK;
}
