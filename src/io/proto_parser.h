#ifndef PROTO_PARSER_H
#define PROTO_PARSER_H

/*
 *============================================================================
 * Name             : proto_parser.h
 * Author           : Milos
 * Description      : Zero-copy protocol parser using in-place modification.
 *
 * This module parses the wire protocol by temporarily injecting null-terminators
 * directly into the receive buffer. This allows us to pass "C-Strings" to
 * the application without allocating new memory (Zero-Copy/Zero-Malloc).
 *
 * The RestoreState struct tracks every byte we modify so we can revert the
 * buffer to its original state after command execution.
 *============================================================================
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#include "common/common.h"
#include "common/macros.h"

typedef enum
{
  VALIDATE_OK = 0,
  VALIDATE_TOO_SHORT,
  VALIDATE_TOO_MANY_ARGS,
  VALIDATE_TOO_FEW_ARGS
} ValidationResult;

typedef enum
{
  PARSE_OK = 0,
  PARSE_MISSING_LENGTH,
  PARSE_LENGTH_OVERFLOW,
  PARSE_TRAILING_DATA
} ParseResult;

// State for in-place null-termination trick
// We track the location and original value of every byte we overwrite with '\0'
typedef struct
{
  uint8_t *restore_ptrs[K_MAX_ARGS];
  uint8_t restore_chars[K_MAX_ARGS];
  size_t restore_count;
} RestoreState;

// Initialize restoration state
static ALWAYS_INLINE void
restore_state_init (RestoreState *state)
{
  state->restore_count = 0;
}

// Restore all modified bytes
// Note: We use 'const' for state to show we don't modify the TRACKING data,
// only the raw buffer data it points to.
static ALWAYS_INLINE void
restore_all_bytes (const RestoreState *state)
{
  for (size_t i = 0; i < state->restore_count; i++)
    {
      *state->restore_ptrs[i] = state->restore_chars[i];
    }
}

// Validate the basic request structure
// Returns error code, or VALIDATE_OK on success
ValidationResult
validate_request_header (const uint8_t * req, uint32_t reqlen,
			 uint32_t * out_arg_count);


// Parse arguments from request buffer into cmd array
// Uses in-place null-termination with automatic restoration tracking
ParseResult
parse_arguments (uint8_t * req, uint32_t reqlen, uint32_t arg_count,
		 char **cmd, size_t *out_cmd_size, RestoreState * restore);

#endif // PROTO_PARSER_H
