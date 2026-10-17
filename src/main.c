/*
 *============================================================================
 * Name          : main.c
 * Author        : Milos
 * Description   : Minis entry point. Handles CLI args, signals, and startup.
 *============================================================================
 */

#include "common/common.h"
#include <stdint.h>
#include <stdio.h>
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L	// For daemon(), sigaction, etc.
#endif

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <signal.h>
#include <stdbool.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <errno.h>
#include <string.h>

#include "io/server_loop.h"

#define MINIS_VERSION "0.1.0"
#define DEFAULT_PORT 1234
#define MINIS_BANNER \
    "  __  __ _       _\n" \
    " |  \\/  (_)     (_)\n" \
    " | \\  / |_ _ __  _ ___\n" \
    " | |\\/| | | '_ \\| / __|\n" \
    " | |  | | | | | | \\__ \\\n" \
    " |_|  |_|_|_| |_|_|___/  v" MINIS_VERSION "\n"

typedef struct
{
  int port;
  const char *bind_addr;	// NULL: all addresses
  bool daemonize;
  bool verbose;
} MinisConfig;

static MinisConfig config;

void
print_usage (const char *prog_name)
{
  msgf ("Usage: %s [options]\n\n", prog_name);
  msgf ("Options:\n");
  msgf ("  -p, --port <port>    Set TCP port (default: %d)\n", DEFAULT_PORT);
  msgf ("  -b, --bind <addr>    Listen on this IPv4 address only, like\n"
	"                       127.0.0.1 (default: all addresses)\n");
  msgf ("  -d, --daemonize      Run as a background daemon\n");
  msgf ("  -v, --verbose        Enable verbose logging\n");
  msgf ("  -h, --help           Show this help message\n");
  msgf ("\n");
}

void
parse_args (int argc, char *argv[], MinisConfig *cfg)
{
  // Set defaults
  cfg->port = DEFAULT_PORT;
  cfg->bind_addr = NULL;
  cfg->daemonize = false;
  cfg->verbose = false;

  static struct option long_options[] = {
    {"port", required_argument, 0, 'p'},
    {"bind", required_argument, 0, 'b'},
    {"daemonize", no_argument, 0, 'd'},
    {"verbose", no_argument, 0, 'v'},
    {"help", no_argument, 0, 'h'},
    {0, 0, 0, 0}
  };

  int opt;
  int option_index = 0;

  while ((opt =
	  getopt_long (argc, argv, "p:b:dvh", long_options,
		       &option_index)) != -1)
    {
      switch (opt)
	{
	case 'p':
	  cfg->port = atoi (optarg);
	  if (cfg->port <= 0 || cfg->port > 65535)
	    {
	      msgf ("Error: Invalid port number %s\n", optarg);
	      exit (EXIT_FAILURE);
	    }
	  break;
	case 'b':
	  cfg->bind_addr = optarg;
	  break;
	case 'd':
	  cfg->daemonize = true;
	  break;
	case 'v':
	  g_verbose_mode = true;
	  cfg->verbose = true;
	  break;
	case 'h':
	  print_usage (argv[0]);
	  exit (EXIT_SUCCESS);
	case '?':
	  // getopt_long already printed an error message
	  // FALLSTHROUGH
	default:
	  exit (EXIT_FAILURE);
	}
    }
}

// Simple Daemonizer (POSIX). Returns the write end of a pipe the daemon
// writes a byte to once it is serving; the process the user started
// waits for that byte, so it can say whether the daemon really started.
int
daemonize_process (void)
{
  int ready[2];
  if (pipe (ready) < 0)
    die ("pipe()");

  // Fork off the parent process
  pid_t pid = fork ();
  if (pid < 0)
    exit (EXIT_FAILURE);
  if (pid > 0)
    {
      // Wait for the daemon: a byte when it serves, EOF if it died
      close (ready[1]);
      char byte;
      ssize_t got;
      do
	got = read (ready[0], &byte, 1);
      while (got < 0 && errno == EINTR);
      if (got == 1)
	{
	  msg ("  > Minis started in the background.");
	  exit (EXIT_SUCCESS);
	}
      msg ("  > Minis failed to start, run it without -d to see why.");
      exit (EXIT_FAILURE);
    }
  close (ready[0]);

  // Create a new SID for the child process
  if (setsid () < 0)
    exit (EXIT_FAILURE);

  // Catch, ignore and handle signals (TODO)
  signal (SIGCHLD, SIG_IGN);
  signal (SIGHUP, SIG_IGN);

  // Fork off for the second time (prevent re-acquiring TTY)
  pid = fork ();
  if (pid < 0)
    exit (EXIT_FAILURE);
  if (pid > 0)
    {
      msgf ("  > PID:  %d\n", pid);
      fflush (stderr);
      exit (EXIT_SUCCESS);	// Parent exits
    }

  // Change file mode mask
  umask (0);

  // Change working directory to root (so we don't lock folders)
  if (chdir ("/") < 0)
    {				/* handle error */
    }

  // Close standard file descriptors
  close (STDIN_FILENO);
  close (STDOUT_FILENO);
  close (STDERR_FILENO);

  // Redirect stdio to /dev/null
  int file_desc = open ("/dev/null", O_RDWR);
  if (file_desc != -1)
    {
      dup2 (file_desc, STDIN_FILENO);
      dup2 (file_desc, STDOUT_FILENO);
      dup2 (file_desc, STDERR_FILENO);
      if (file_desc > 2)
	close (file_desc);
    }
  return ready[1];
}

void
handle_signal (int sig)
{
  if (sig == SIGINT || sig == SIGTERM)
    {
      // This is where you'd call save_data() if we add persistence!
      // For now, we just define it to catch the interrupt.
      // The server loop is usually infinite, but we can set a global flag here.
      // For raw performance, we usually just let the OS kill us unless we need to save.
      (void) sig;
    }
}

int
main (int argc, char *argv[])
{
  parse_args (argc, argv, &config);

  msg (MINIS_BANNER);
  msgf ("\n  > Port: %d\n", config.port);
  msgf ("  > Profile: %s\n", BUILD_PROFILE);
  if (config.verbose)
    msgf ("  > Verbose: Enabled\n");

  // The daemon changes to /, so a relative snapshot directory has to be
  // made absolute first
  static char db_dir[PATH_MAX];
  if (!getcwd (db_dir, sizeof (db_dir) - sizeof ("/" MINIS_DB_DIR)))
    die ("getcwd()");
  strcat (db_dir, "/" MINIS_DB_DIR);
  msgf ("  > Data: %s\n", db_dir);

  int ready_fd = -1;
  if (config.daemonize)
    {
      ready_fd = daemonize_process ();
    }
  else
    {
      msgf ("  > PID:  %d\n", getpid ());
      msgf ("  > Ready to accept connections.\n\n");
    }

  // Setup graceful shutdown hooks
  signal (SIGINT, handle_signal);
  signal (SIGTERM, handle_signal);

  // Start the Engine
  // You might need to update server_run to accept the config struct 
  // or just pass the port for now.
  return server_run (config.port, config.bind_addr, db_dir, ready_fd);
}
