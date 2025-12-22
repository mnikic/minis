#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include "io/out.h"
#include "io/buffer.h"

// Enum to map random bytes to function calls
enum Ops
{
  OP_NIL = 0,
  OP_STR,
  OP_INT,
  OP_DBL,
  OP_ERR,
  OP_ARR,
  OP_ARR_BEGIN_END,		// Test the dynamic array pair
  OP_COUNT			// Total number of ops
};

int
LLVMFuzzerTestOneInput (const uint8_t *data, size_t size)
{
  // 1. Setup a Destination Buffer
  // We make it relatively small (e.g., 1KB) to FORCE buffer-full conditions frequentl.y
  // We want to ensure it handles "Running out of space" gracefully (returning false),
  // rather than segfaulting.
  uint8_t dest_mem[1024];
  Buffer out_buf = buf_init (dest_mem, sizeof (dest_mem));

  // 2. Loop through the input data as a stream of instructions
  size_t cursor = 0;

  while (cursor < size)
    {
      // Read Opcode
      uint8_t op = data[cursor] % OP_COUNT;
      cursor++;

      bool res = false;

      switch (op)
	{
	case OP_NIL:
	  res = out_nil (&out_buf);
	  break;

	case OP_STR:
	  // Need at least 1 byte for length
	  if (cursor + 1 <= size)
	    {
	      uint8_t len = data[cursor];
	      cursor++;
	      // Check if we have 'len' bytes left for the string
	      if (cursor + len <= size)
		{
		  // Cast the random bytes to a char* (it doesn't need to be null-term for this API usually, 
		  // but out_str_size is safer for fuzzing raw bytes)
		  res =
		    out_str_size (&out_buf, (const char *) &data[cursor],
				  len);
		  cursor += len;
		}
	    }
	  break;

	case OP_INT:
	  // Need 8 bytes for int64
	  if (cursor + 8 <= size)
	    {
	      int64_t val;
	      memcpy (&val, &data[cursor], 8);
	      cursor += 8;
	      res = out_int (&out_buf, val);
	    }
	  break;

	case OP_DBL:
	  // Need 8 bytes for double
	  if (cursor + 8 <= size)
	    {
	      double val;
	      memcpy (&val, &data[cursor], 8);
	      cursor += 8;
	      res = out_dbl (&out_buf, val);
	    }
	  break;

	case OP_ERR:
	  // Need 4 bytes code + 1 byte len + string
	  if (cursor + 5 <= size)
	    {
	      int32_t code;
	      memcpy (&code, &data[cursor], 4);
	      cursor += 4;

	      uint8_t len = data[cursor];
	      cursor++;

	      if (cursor + len <= size)
		{
		  // We can construct a temp stack buffer for the err msg
		  char temp_msg[256];
		  size_t copy_len = len > 255 ? 255 : len;
		  memcpy (temp_msg, &data[cursor], copy_len);
		  temp_msg[copy_len] = '\0';	// Ensure valid C-string

		  res = out_err (&out_buf, code, temp_msg);
		  cursor += len;
		}
	    }
	  break;

	case OP_ARR:
	  // Small random number for array size
	  if (cursor + 1 <= size)
	    {
	      res = out_arr (&out_buf, data[cursor]);
	      cursor++;
	    }
	  break;

	case OP_ARR_BEGIN_END:
	  // This stress tests the stateful logic
	  {
	    size_t pos = out_arr_begin (&out_buf);
	    // Write some junk in between?
	    out_int (&out_buf, 123);
	    // Close it
	    if (cursor + 1 <= size)
	      {
		size_t num = data[cursor];
		cursor++;
		res = out_arr_end (&out_buf, pos, num);
	      }
	  }
	  break;
	}

      //  If buffer is full (res == false), we could break, 
      // or we can keep hammering it to make sure it doesn't crash 
      // when called on a full buffer. Hammering is better for safety testing.
    }

  return 0;
}
