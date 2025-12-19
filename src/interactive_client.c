#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>

#include "common/common.h"

// --- Configuration and Protocol Definitions ---
#define DEFAULT_SERVER_IP "127.0.0.1"
#define MAX_COMMAND_LEN 1024
#define MAX_TOKENS 32
#define MSG_BUF_SIZE (4 + K_MAX_MSG)
#define RECONNECT_DELAY_S 3

// --- Global Configuration ---
static int g_debug_mode = 0;
static const char *g_host = DEFAULT_SERVER_IP;
static uint16_t g_port = DEFAULT_PORT;


// --- Command History Globals ---
#define HISTORY_MAX_SIZE 10
// Array of strings to hold the last 10 commands
static char *g_history[HISTORY_MAX_SIZE];
// Index where the next command will be stored (circular buffer)
static int g_history_idx = 0;
// Actual number of commands stored (up to 10)
static int g_history_len = 0;


// --- Network Byte Order Helpers ---
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <libkern/OSByteOrder.h>
#define ntohll(x) OSSwapBigToHostInt64(x)
#define htonll(x) OSSwapHostToBigInt64(x)
#else
#define ntohll(x) __builtin_bswap64(x)
#define htonll(x) __builtin_bswap64(x)
#endif


// --- History Management ---

static void
history_add (const char *command)
{
  // Free the old command at this slot if it exists
  if (g_history[g_history_idx])
    {
      free (g_history[g_history_idx]);
    }

  // Allocate and store the new command
  g_history[g_history_idx] = strdup (command);

  // Advance index and wrap around (circular buffer logic)
  g_history_idx = (g_history_idx + 1) % HISTORY_MAX_SIZE;

  // Update length if we haven't filled the buffer yet
  if (g_history_len < HISTORY_MAX_SIZE)
    {
      g_history_len++;
    }
}

// Retrieves a command from history based on the index (1 = oldest, N = newest)
static char *
history_get (int index)
{
  if (index <= 0 || index > g_history_len)
    {
      return NULL;
    }

  // Convert human-readable index (1-10, 10=newest) to internal array index (0-9)
  int target_idx =
    (g_history_idx - g_history_len + index - 1) % HISTORY_MAX_SIZE;

  // Handle negative results from modulo for safety, though it shouldn't happen with the logic above
  if (target_idx < 0)
    {
      target_idx += HISTORY_MAX_SIZE;
    }

  return g_history[target_idx];
}

static void
history_print (void)
{
  if (g_history_len == 0)
    {
      printf ("History is empty.\n");
      return;
    }
  printf ("\n--- Command History (%d entries) ---\n", g_history_len);
  for (int i = 1; i <= g_history_len; ++i)
    {
      char *cmd = history_get (i);
      if (cmd)
	{
	  printf (" %2d: %s\n", i, cmd);
	}
    }
  printf ("-----------------------------------\n");
}


// --- Debug Printing ---

static void
print_raw_data (const char *label, const char *data, size_t n)
{
  if (!g_debug_mode)
    return;

  fprintf (stderr, "\n--- %s (%zu bytes) ---\n", label, n);

  // Hex Dump
  fprintf (stderr, "Hex: ");
  for (size_t i = 0; i < n; i++)
    {
      fprintf (stderr, "%02x ", (unsigned char) data[i]);
      if ((i + 1) % 16 == 0)
	fprintf (stderr, "\n     ");
    }

  // ASCII Dump
  fprintf (stderr, "\nASCII: ");
  for (size_t i = 0; i < n; i++)
    {
      char c = data[i];
      if (c >= 32 && c <= 126)
	{
	  fprintf (stderr, "%c", c);
	}
      else
	{
	  fprintf (stderr, ".");	// Replace non-printable characters
	}
    }
  fprintf (stderr, "\n-------------------------------------\n");
}


// --- I/O Helpers ---

static int32_t
read_full (int fd, char *buf, size_t n)
{
  size_t initial_n = n;
  while (n > 0)
    {
      ssize_t rv = read (fd, buf, n);
      if (rv <= 0)
	{
	  return -1;
	}
      assert ((size_t) rv <= n);
      n -= (size_t) rv;
      buf += rv;
    }

  if (g_debug_mode)
    {
      print_raw_data ("READ RESPONSE BODY (RAW)", buf - initial_n, initial_n);
    }
  return 0;
}

static int32_t
write_all (int fd, const char *buf, size_t n)
{
  if (g_debug_mode)
    {
      print_raw_data ("WRITE REQUEST (RAW)", buf, n);
    }

  while (n > 0)
    {
      ssize_t rv = write (fd, buf, n);
      if (rv <= 0)
	{
	  if (rv == -1)
	    {
	      perror ("Socket write failed (write_all)");
	    }
	  else if (rv == 0)
	    {
	      fprintf (stderr,
		       "Socket write returned 0 bytes, connection likely closed.\n");
	    }
	  return -1;		// Fatal socket error
	}
      assert ((size_t) rv <= n);
      n -= (size_t) rv;
      buf += rv;
    }
  return 0;			// Success
}


// --- Request Serialization and Deserialization ---

static int32_t
send_req_from_input (int fd, const char *input)
{
  char temp_input[MAX_COMMAND_LEN];
  strncpy (temp_input, input, MAX_COMMAND_LEN - 1);
  temp_input[MAX_COMMAND_LEN - 1] = '\0';

  char *saveptr;
  char *tokens[MAX_TOKENS];
  int cmd_size = 0;

  char *token = strtok_r (temp_input, " \t\n", &saveptr);
  while (token != NULL && cmd_size < MAX_TOKENS)
    {
      tokens[cmd_size++] = token;
      token = strtok_r (NULL, " \t\n", &saveptr);
    }

  if (cmd_size == 0)
    return 0;

  char wbuf[MSG_BUF_SIZE];

  uint32_t total_payload_len = 4;	// for the command count N
  for (int i = 0; i < cmd_size; i++)
    {
      total_payload_len += (uint32_t) strlen (tokens[i]) + 4;	// +4 for string length L
    }

  if (total_payload_len > K_MAX_MSG)
    {
      fprintf (stderr,
	       "Error: Request size (%u bytes) exceeds client/server limit (%lu bytes)\n",
	       total_payload_len, K_MAX_MSG);
      return -1;
    }

  uint32_t net_total_len = htonl (total_payload_len);
  memcpy (&wbuf[0], &net_total_len, 4);

  uint32_t net_cmd_size = htonl ((uint32_t) cmd_size);
  memcpy (&wbuf[4], &net_cmd_size, 4);

  size_t cur = 8;
  for (int i = 0; i < cmd_size; i++)
    {
      char *s = tokens[i];
      size_t cmd_len = strlen (s);
      uint32_t p = (uint32_t) cmd_len;

      if (cur + 4 + cmd_len > MSG_BUF_SIZE)
	{
	  fprintf (stderr,
		   "Internal buffer overflow during serialization.\n");
	  return -1;
	}

      uint32_t net_p = htonl (p);
      memcpy (&wbuf[cur], &net_p, 4);

      memcpy (&wbuf[cur + 4], s, cmd_len);
      cur += 4 + cmd_len;
    }

  uint32_t bytes_to_write = 4 + total_payload_len;

  if (write_all (fd, wbuf, bytes_to_write) < 0)
    {
      return -1;
    }

  return (int32_t) bytes_to_write;
}


static int32_t
on_response (const uint8_t *data, size_t size)
{
  if (size < 1)
    return -1;

  uint32_t len_net = 0;
  uint32_t len_host = 0;

  switch (data[0])
    {
    case SER_NIL:
      printf ("(nil)\n");
      return 1;

    case SER_ERR:
      if (size < 1 + 8)
	return -1;
      {
	int32_t code = 0;
	uint32_t code_net = 0;
	memcpy (&code_net, &data[1], 4);
	code = (int32_t) ntohl (code_net);

	memcpy (&len_net, &data[1 + 4], 4);
	len_host = ntohl (len_net);

	if (size < 1 + 8 + len_host)
	  return -1;
	printf ("(ERR %d) %.*s\n", code, len_host, &data[1 + 8]);
	return (int32_t) (1 + 8 + len_host);
      }

    case SER_STR:
      if (size < 1 + 4)
	return -1;
      {
	memcpy (&len_net, &data[1], 4);
	len_host = ntohl (len_net);

	if (size < 1 + 4 + len_host)
	  return -1;
	printf ("(str) %.*s\n", len_host, &data[1 + 4]);
	return (int32_t) (1 + 4 + len_host);
      }

    case SER_INT:
      if (size < 1 + 8)
	return -1;
      {
	uint64_t val_net = 0;
	memcpy (&val_net, &data[1], 8);
	int64_t val = (int64_t) ntohll (val_net);

	printf ("(int) %ld\n", val);
	return 1 + 8;
      }

    case SER_DBL:
      if (size < 1 + 8)
	return -1;
      {
	double val = 0;
	memcpy (&val, &data[1], 8);
	printf ("(dbl) %g\n", val);
	return 1 + 8;
      }

    case SER_ARR:
      if (size < 1 + 4)
	return -1;
      {
	memcpy (&len_net, &data[1], 4);
	len_host = ntohl (len_net);

	printf ("(arr) len=%u\n", len_host);
	size_t arr_bytes = 1 + 4;
	for (uint32_t i = 0; i < len_host; ++i)
	  {
	    printf ("  [%u]: ", i);
	    int32_t rv = on_response (&data[arr_bytes], size - arr_bytes);
	    if (rv < 0)
	      return rv;
	    arr_bytes += (size_t) rv;
	  }
	printf ("(arr) end\n");
	return (int32_t) arr_bytes;
      }

    default:
      fprintf (stderr, "Error: Unknown response type byte: 0x%02x\n",
	       data[0]);
      return -1;
    }
}

static int32_t
read_res (int fd)
{
  char rbuf[MSG_BUF_SIZE];

  struct pollfd pfd = {.fd = fd,.events = POLLIN };
  int timeout_ms = 2000;

  int poll_rv = poll (&pfd, 1, timeout_ms);

  if (poll_rv < 0)
    {
      perror ("poll error");
      return -1;
    }
  if (poll_rv == 0)
    {
      fprintf (stderr,
	       "\nERROR: Read timeout (2s). The server did not respond.\n");
      return -1;
    }

  if (!(pfd.revents & POLLIN))
    {
      if (pfd.revents & POLLHUP)
	{
	  fprintf (stderr, "Connection hung up or closed by server.\n");
	}
      return -1;
    }

  // 1. Read the 4-byte length header
  errno = 0;
  int32_t err = read_full (fd, rbuf, 4);
  if (err)
    {
      if (errno == 0)
	fprintf (stderr,
		 "Server closed connection (EOF) while reading response header.\n");
      else
	perror ("read() error on length prefix");
      return err;
    }

  if (g_debug_mode)
    {
      print_raw_data ("READ RESPONSE HEADER (RAW)", rbuf, 4);
    }

  uint32_t len_net = 0;
  uint32_t len_host = 0;

  memcpy (&len_net, rbuf, 4);
  len_host = ntohl (len_net);

  if (len_host > K_MAX_MSG)
    {
      fprintf (stderr, "Error: Response too long (%u > %lu). Discarding.\n",
	       len_host, K_MAX_MSG);
      return -1;
    }

  // 2. Read the reply body
  err = read_full (fd, &rbuf[4], len_host);
  if (err)
    {
      perror ("read() error on response body");
      return err;
    }

  // 3. Parse the response payload
  int32_t rv = on_response ((uint8_t *) & rbuf[4], len_host);

  if (rv < 0)
    {
      fprintf (stderr, "Error parsing response.\n");
    }
  else if ((uint32_t) rv != len_host)
    {
      fprintf (stderr,
	       "Warning: Payload size mismatch. Parsed %d bytes, expected %u bytes.\n",
	       rv, len_host);
    }

  fflush (stdout);
  fflush (stderr);
  return rv;
}

static void
get_host_and_port_and_debug (int argc, char *argv[])
{
  for (int i = 1; i < argc; ++i)
    {
      if (strcmp (argv[i], "-h") == 0 && i + 1 < argc)
	{
	  g_host = argv[++i];
	}
      else if (strcmp (argv[i], "-p") == 0 && i + 1 < argc)
	{
	  int p = atoi (argv[++i]);
	  if (p > 0 && p <= 65535)
	    {
	      g_port = (uint16_t) p;
	    }
	  else
	    {
	      fprintf (stderr, "Invalid port number. Using default %d.\n",
		       DEFAULT_PORT);
	    }
	}
      else if (strcmp (argv[i], "-d") == 0)
	{
	  g_debug_mode = 1;
	}
    }
}

static int
do_connect (void)
{
  int client_fd;
  struct sockaddr_in server_addr;

  if ((client_fd = socket (AF_INET, SOCK_STREAM, 0)) < 0)
    {
      perror ("Socket creation error");
      return -1;
    }

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons (g_port);
  if (inet_pton (AF_INET, g_host, &server_addr.sin_addr) <= 0)
    {
      perror ("Invalid address/ Address not supported");
      close (client_fd);
      return -1;
    }

  if (connect
      (client_fd, (struct sockaddr *) &server_addr, sizeof (server_addr)) < 0)
    {
      if (g_debug_mode)
	{
	  perror ("Connection attempt failed");
	}
      close (client_fd);
      return -1;
    }

  printf ("\nSuccessfully connected to %s:%d.\n", g_host, g_port);
  return client_fd;
}

// Handles special commands like !! (previous), !5 (5th command back), and H/HISTORY
// Returns 1 if a command was executed/handled (not sent to server), 0 otherwise.
static int 
process_special_command(int client_fd, char *command, char *exec_cmd) 
{
    // Check for H or HISTORY to print the history list
    if (strcasecmp(command, "H") == 0 || strcasecmp(command, "HISTORY") == 0) {
        history_print();
        return 1;
    }

    if (strcmp(command, "!!") == 0) {
        // Execute the last command (which is history_len's index)
        if (g_history_len > 0) {
            char *last_cmd = history_get(g_history_len);
            if (last_cmd) {
                // FIX: Ensure null termination to satisfy -Wstringop-truncation
                strncpy(exec_cmd, last_cmd, MAX_COMMAND_LEN - 1);
                exec_cmd[MAX_COMMAND_LEN - 1] = '\0'; 
                printf("!%d: %s\n", g_history_len, exec_cmd);
                return 0; // Command ready for execution
            }
        }
        fprintf(stderr, "History error: No previous command to repeat.\n");
        return 1;
    }

    if (command[0] == '!' && command[1] >= '1' && command[1] <= '9') {
        int index = atoi(&command[1]);
        if (index > 0 && index <= g_history_len) {
             char *hist_cmd = history_get(index);
             if (hist_cmd) {
                // FIX: Ensure null termination to satisfy -Wstringop-truncation
                strncpy(exec_cmd, hist_cmd, MAX_COMMAND_LEN - 1);
                exec_cmd[MAX_COMMAND_LEN - 1] = '\0';
                printf("!%d: %s\n", index, exec_cmd);
                return 0; // Command ready for execution
             }
        }
        fprintf(stderr, "History error: Command !%d is outside the current history window (1-%d).\n", index, g_history_len);
        return 1;
    }

    // Not a special command (default path reported by user)
    // FIX: Ensure null termination to satisfy -Wstringop-truncation
    snprintf(exec_cmd, MAX_COMMAND_LEN, "%s", command);
    return 0; 
}


int main(int argc, char *argv[]) {
    int client_fd = -1;
    char command[MAX_COMMAND_LEN];
    char exec_command[MAX_COMMAND_LEN]; // Holds command to be sent (either from input or history)
    int first_connection = 1; 

    setvbuf(stdin, NULL, _IONBF, 0); 
    setvbuf(stdout, NULL, _IONBF, 0); 
    setvbuf(stderr, NULL, _IONBF, 0); 

    get_host_and_port_and_debug(argc, argv);
    
    printf("--- Minis Interactive Native Client ---\n");
    printf("Host: %s | Port: %d | Debug Mode: %s\n", g_host, g_port, g_debug_mode ? "ON (-d)" : "OFF");
    printf("Type 'QUIT' to exit.\n");
    printf("History commands: 'H' or 'HISTORY' (list history), '!!' (repeat last), '!N' (execute Nth command).\n");


    // Outer loop for connection management
    while (1) {
        if (client_fd == -1) {
            if (!first_connection) {
                fprintf(stderr, "Connection lost. Attempting to reconnect in %d seconds...\n", RECONNECT_DELAY_S);
                sleep(RECONNECT_DELAY_S);
            }
            
            client_fd = do_connect();
            if (client_fd == -1) {
                if (first_connection) {
                    fprintf(stderr, "Initial connection failed. Retrying...\n");
                    first_connection = 0; 
                }
                continue; 
            }
            first_connection = 0;
        }

        // Inner loop for interactive session
        while (client_fd != -1) {
            printf("minis> ");
            fflush(stdout); 

            if (fgets(command, MAX_COMMAND_LEN, stdin) == NULL) {
                // EOF (Ctrl+D)
                goto cleanup; 
            }

            command[strcspn(command, "\n")] = 0; // Trim newline
            
            if (strcasecmp(command, "QUIT") == 0 || strcasecmp(command, "EXIT") == 0) {
                goto cleanup;
            }

            if (strlen(command) == 0) {
                continue;
            }
            
            // Handle history commands (H, !! or !N) and prepare exec_command
            int handled = process_special_command(client_fd, command, exec_command);

            if (handled) {
                continue; // Command was handled internally (like HISTORY)
            }

            // 1. Send the request (using exec_command which is either input or history)
            int32_t bytes_sent = send_req_from_input(client_fd, exec_command);
            
            if (bytes_sent < 0) {
                fprintf(stderr, "Request send failed. Closing socket.\n");
                close(client_fd);
                client_fd = -1; 
                break; 
            }
            
            if (bytes_sent == 0) continue; 
            
            // Add the executed command to history *only if* it was a real command sent
            history_add(exec_command);

            // 2. Read and parse the response
            int32_t err = read_res(client_fd);
            if (err < 0) {
                fprintf(stderr, "Interactive session error or server closed connection.\n");
                close(client_fd);
                client_fd = -1; 
                break;
            }
        }
    }

cleanup:
    if (client_fd != -1) {
        close(client_fd);
    }
    // Clean up history memory
    for (int i = 0; i < HISTORY_MAX_SIZE; ++i) {
        if (g_history[i]) {
            free(g_history[i]);
        }
    }
    printf("Connection closed. Exiting.\n");
    return 0;
}
