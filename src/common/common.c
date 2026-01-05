/*
 * common.c
 *
 *  Created on: Jun 19, 2023
 *      Author: loshmi
 */

#include <stdarg.h>
#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <endian.h>

#include "common/common.h"
#include "common/macros.h"

bool g_verbose_mode = false;

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

NORETURN void
die (const char *msg)
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
