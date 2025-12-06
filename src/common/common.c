/*
 * common.c
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#include <stdarg.h>
#include <stdint.h>
#include <stddef.h>
#include "common.h"
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

inline uint64_t
str_hash (const uint8_t *data, size_t len)
{
  uint32_t seed = 0x811C9DC5;
  for (size_t i = 0; i < len; i++)
    {
      seed = (seed + (data[i] ? data[i] : 0)) * 0x01000193;
    }
  return seed;
}

uint64_t
get_monotonic_usec (void)
{
  struct timespec tvs = { 0, 0 };
  clock_gettime (CLOCK_MONOTONIC, &tvs);
  return (uint64_t) ((tvs.tv_sec * 1000000) + (tvs.tv_nsec / 1000));
}


__attribute__((format (printf, 1, 2)))
     void msgf (const char *fmt, ...)
{
  va_list apl;
  va_start (apl, fmt);

  vfprintf (stderr, fmt, apl);

  va_end (apl);

  // Add newline only if the format does NOT end with one
  size_t len = strlen (fmt);
  if (len == 0 || fmt[len - 1] != '\n')
    {
      fputc ('\n', stderr);
    }
}

void
msg (const char *msg)
{
  fprintf (stderr, "%s\n", msg);
}

__attribute__((noreturn))
     void die (const char *msg)
{
  int err = errno;
  fprintf (stderr, "[%d] %s\n", err, msg);
  abort ();
}

static void
usage (const char *prog_name)
{
  fprintf (stderr, "Usage: %s [-p <port>]\n", prog_name);
  fprintf (stderr,
	   "  -p <port>  Specify the TCP port to listen on (default: %d)\n",
	   DEFAULT_PORT);
}

uint16_t
parse_port (int argc, char *argv[])
{
  int port = DEFAULT_PORT;

  for (int i = 1; i < argc; ++i)
    {
      if (strcmp (argv[i], "-p") == 0)
	{
	  if (i + 1 < argc)
	    {
	      port = (int) strtol (argv[++i], NULL, 10);
	      if (port <= 0 || port > 65535)
		{
		  usage (argv[0]);
		  die ("Error: Invalid port number.");
		}
	    }
	  else
	    {
	      usage (argv[0]);
	      die ("Error: -p requires a port number argument.");
	    }
	  goto DONE;
	}
    }
DONE:
  return (uint16_t) port;
}

uint64_t
htoll (uint64_t number)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
  // If the host is little-endian, we must swap the 32-bit halves and then swap
  // the bytes within each half.
  // Explicitly cast to uint32_t before passing to htonl to avoid -Wconversion warnings/errors.
  uint32_t low_part = (uint32_t) number;
  uint32_t high_part = (uint32_t) (number >> 32);

  return (((uint64_t) htonl (low_part)) << 32) | (uint64_t) htonl (high_part);
#else
  // Host is big-endian (network order), so no swap is needed.
  return x;
#endif
}

uint64_t
ntohll (uint64_t number)
{
  // For network-to-host, the logic is the same: swap if the host is little-endian.
  return htoll (number);
}

// Portable implementation of Host to Network 32-bit (htonl)
uint32_t
hton_u32 (uint32_t host_val)
{
  // Manual byte swap: 0x12345678 (Host) -> 0x78563412 (Network/BE)
  return ((host_val & 0xFF) << 24) |
    ((host_val & 0xFF00) << 8) |
    ((host_val & 0xFF0000) >> 8) | ((host_val & 0xFF000000) >> 24);
}

// Portable implementation of Host to Network 64-bit
uint64_t
hton_u64 (uint64_t host_val)
{
  // Manual byte swap (Host to Big-Endian)
  uint64_t val = 0;
  val |= (host_val & 0x00000000000000FFULL) << 56;
  val |= (host_val & 0x000000000000FF00ULL) << 40;
  val |= (host_val & 0x0000000000FF0000ULL) << 24;
  val |= (host_val & 0x00000000FF000000ULL) << 8;
  val |= (host_val & 0x000000FF00000000ULL) >> 8;
  val |= (host_val & 0x0000FF0000000000ULL) >> 24;
  val |= (host_val & 0x00FF000000000000ULL) >> 40;
  val |= (host_val & 0xFF00000000000000ULL) >> 56;
  return val;
}
