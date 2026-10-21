#ifndef PROTO_PARSER_H
#define PROTO_PARSER_H

/*
 *============================================================================
 * Name             : proto_parser.h
 * Author           : Milos
 * Description      : Zero-copy protocol parser using in-place modification.
 *============================================================================
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#include "common/macros.h"
#include "io/proto_defs.h"

// Initialize restoration state
static ALWAYS_INLINE void
restore_state_init (RestoreState *state)
{
  state->restore_count = 0;
}

// Restore all modified bytes
static ALWAYS_INLINE void
restore_all_bytes (const RestoreState *state)
{
  for (size_t i = 0; i < state->restore_count; i++)
    {
      *state->restore_ptrs[i] = state->restore_chars[i];
    }
}

// The protoc sniffing function
HOT ProtoMessageInfo proto_identify_message (const uint8_t * buf, size_t len);

// Argument 1 of a request proto_identify_message framed (req and len are
// its payload), which is the key of most commands. The buffer is only
// read. False if there is no argument 1.
bool proto_request_key (const uint8_t * req, size_t len, ProtoType type,
			const uint8_t ** key, size_t *key_len);

// Validate the basic request structure (BINARY)
ValidationResult
validate_request_header (const uint8_t * req, uint32_t reqlen,
			 uint32_t * out_arg_count);

// Parse arguments from request buffer into cmd array (BINARY)
HOT ParseResult proto_parse_arguments (ProtoRequest * proto_request);

/**
 * @brief Frames the next RESP message: its total length, whether more
 * bytes are needed, or why it can never be valid.
 * * @param buf Pointer to the start of the buffer, at a '*'
 * @param limit Total bytes available in the buffer
 * @param out_len The message length, for PROTO_DECISION_OK
 * @param out_error Why, for PROTO_DECISION_INVALID
 */
HOT ProtoStatus scan_resp_message (const uint8_t * buf, size_t limit,
				   size_t *out_len, const char **out_error);

/**
 * @brief Parses a RESP Array message into the cmd array using Zero-Copy.
 * Handles *N\r\n$L\r\nData\r\n...
 */
HOT ParseResult parse_resp_arguments (ProtoRequest * req);

#endif // PROTO_PARSER_H
