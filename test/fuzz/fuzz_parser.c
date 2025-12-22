#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>

#include "io/proto_parser.h"
#include "common/common.h"

int
LLVMFuzzerTestOneInput (const uint8_t *data, size_t size)
{
  // Min Size Check
  // We need at least 4 bytes for the header (req len) to even make sense
  if (size < 4)
    return 0;

  // Setup Mutable Buffer (Simulate Network Buffer)
  // We allocate +1 for the safety padding, just like the real server.
  size_t alloc_size = size + 1;
  uint8_t *work_buf = malloc (alloc_size);
  memcpy (work_buf, data, size);

  // Zero out the padding byte to ensure consistent behavior
  work_buf[size] = 0;

  // Validate Header
  // We mimic do_request() logic here.
  uint32_t arg_count = 0;
  ValidationResult val_res =
    validate_request_header (work_buf, (uint32_t) size, &arg_count);

  if (val_res == VALIDATE_OK)
    {

      // Prepare Arguments
      char *cmd[K_MAX_ARGS];
      RestoreState restore;
      ProtoRequest args = {
	.req = work_buf,
	.reqlen = (uint32_t) size,
	.arg_count = arg_count,
	.cmd = cmd,
	.restore = &restore
      };

      ParseResult parse_res = parse_arguments (&args);
      (void) parse_res;
      // Verification (The "Undo" Check)
      // Regardless of success/failure, restore must work.
      restore_all_bytes (&restore);

      // INVARIANT CHECK:
      // After restoration, the buffer MUST be identical to the original input.
      // If this fails, 'undo' logic is broken.
      if (memcmp (work_buf, data, size) != 0)
	{
	  // Force a crash so the fuzzer reports it!
	  abort ();
	}
    }

  free (work_buf);
  return 0;
}
