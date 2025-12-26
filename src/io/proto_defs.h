/*
 * protocol_defs.h
 * Description: Central definitions for protocol types, limits, and status codes.
 */
#ifndef PROTO_DEFS_H_
#define PROTO_DEFS_H_

#include <stddef.h>
#include <stdint.h>

#include "common/common.h"

#define BIN_HEADER_SIZE 4	// Binary protocol length prefix

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
  PARSE_TRAILING_DATA,
  PARSE_BAD_PROTOCOL,
  PARSE_OUT_OF_MEMORY
} ParseResult;

// State for in-place null-termination trick
// We track the location and original value of every byte we overwrite with '\0'
typedef struct
{
  uint8_t *restore_ptrs[K_MAX_ARGS];
  uint8_t restore_chars[K_MAX_ARGS];
  size_t restore_count;
} RestoreState;

typedef struct
{
  // Inputs
  uint8_t *req;
  uint32_t reqlen;
  uint32_t arg_count;

  const char **cmd;		// Array of pointers to fill
  size_t cmd_size;

  // State Tracking
  RestoreState *restore;
} ProtoRequest;

typedef enum
{
  PROTO_RESP = 0,		// Redis Serialization Protocol
  PROTO_BIN = 1			// Custom Binary Protocol
} ProtoType;

// Result of the "Sniffing" phase
typedef enum
{
  PROTO_DECISION_OK,		// Found a valid full message
  PROTO_DECISION_INCOMPLETE,	// Need more data
  PROTO_DECISION_INVALID,	// Garbage detected
  PROTO_DECISION_TOO_BIG	// Exceeds K_MAX_MSG
} ProtoStatus;

// Result of the identification step
typedef struct
{
  ProtoStatus status;
  ProtoType type;
  uint32_t header_len;		// Bytes used by framing (4 for Bin, 0 for RESP)
  uint32_t payload_len;		// Actual data length
  uint32_t total_len;		// header_len + payload_len
} ProtoMessageInfo;

#endif /* PROTO_DEFS_H_ */
