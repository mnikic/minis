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

// Validate the basic request structure (BINARY)
ValidationResult
validate_request_header (const uint8_t * req, uint32_t reqlen,
			 uint32_t * out_arg_count);

// Parse arguments from request buffer into cmd array (BINARY)
HOT ParseResult parse_arguments (ProtoRequest * proto_request);

/**
 * @brief Scans a buffer to determine the total length of the next RESP message.
 * Used by connection_handler to frame messages.
 * * @param buf Pointer to the start of the buffer
 * @param limit Total bytes available in the buffer
 * @return ssize_t The total length of the message in bytes, or -1 if incomplete.
 */
HOT ssize_t scan_resp_message_length (const uint8_t * buf, size_t limit);

/**
 * @brief Parses a RESP Array message into the cmd array using Zero-Copy.
 * Handles *N\r\n$L\r\nData\r\n...
 */
HOT ParseResult parse_resp_arguments (ProtoRequest * req);

#endif // PROTO_PARSER_H
