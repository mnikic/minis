/*
 *============================================================================
 * Name          : main.c
 * Author        : Milos
 * Description   : Application entry point and command-line argument parsing.
 *============================================================================
 */
#include <stdint.h>
#include "server_loop.h"
#include "common/common.h"

int
main (int argc, char *argv[])
{
  // Start the server with the parsed or default port
  return server_run (parse_port (argc, argv));
}
