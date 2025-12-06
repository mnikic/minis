#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include "common.h"

// Note: k_max_msg has been moved to common.h as K_MAX_MSG

static int32_t
read_full (int fd, char *buf, size_t n)
{
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
  return 0;
}

static int32_t
write_all (int fd, const char *buf, size_t n)
{
  while (n > 0)
    {
      ssize_t rv = write (fd, buf, n);
      if (rv <= 0)
	{
	  return -1;
	}
      assert ((size_t) rv <= n);
      n -= (size_t) rv;
      buf += rv;
    }
  return 0;
}

static int32_t
send_req (int fd, char **cmd, size_t cmd_size)
{
  // Calculate total payload length (4 bytes for array size + 4 bytes for each command length + command strings)
  uint32_t total_len = 4;
  for (size_t i = 0; i < cmd_size; i++)
    {
      total_len += (uint32_t) strlen (cmd[i]) + 4;
    }

  // Check against the shared maximum message size
  if (total_len > K_MAX_MSG)
    {
      fprintf (stderr,
	       "Request size (%u bytes) exceeds client/server limit (%u bytes)\n",
	       total_len, K_MAX_MSG);
      return -1;
    }

  // Allocate buffer for header (4 bytes total length) + payload
  char wbuf[4 + K_MAX_MSG];

  // 1. Write total payload length (total_len)
  uint32_t net_total_len = htonl (total_len);
  memcpy (&wbuf[0], &net_total_len, 4);

  // 2. Write command count (cmd_size)
  uint32_t net_cmd_size = htonl ((uint32_t) cmd_size);
  memcpy (&wbuf[4], &net_cmd_size, 4);

  size_t cur = 8;
  // 3. Write command arguments (length + string)
  for (size_t i = 0; i < cmd_size; i++)
    {
      char *s = cmd[i];
      size_t cmd_len = strlen (s);
      uint32_t p = (uint32_t) cmd_len;

      uint32_t net_p = htonl (p);
      memcpy (&wbuf[cur], &net_p, 4);

      memcpy (&wbuf[cur + 4], s, cmd_len);
      cur += 4 + cmd_len;
    }
  // The total length written is 4 bytes (for the overall length prefix) + total_len (the calculated payload length)
  return write_all (fd, wbuf, 4 + total_len);
}

static int32_t
on_response (const uint8_t *data, size_t size)
{
  if (size < 1)
    {
      msg ("bad response");
      return -1;
    }

  uint32_t len_net = 0;
  uint32_t len_host = 0;

  switch (data[0])
    {
    case SER_NIL:
      printf ("(nil)\n");
      return 1;

    case SER_ERR:
      if (size < 1 + 8)
	{
	  msg ("bad response");
	  return -1;
	}
      {
	int32_t code = 0;

	// Deserialize Code (uint32_t)
	uint32_t code_net = 0;
	memcpy (&code_net, &data[1], 4);
	code = (int32_t) ntohl (code_net);

	// Deserialize Length (uint32_t)
	memcpy (&len_net, &data[1 + 4], 4);
	len_host = ntohl (len_net);

	if (size < 1 + 8 + len_host)
	  {
	    msg ("bad response");
	    return -1;
	  }
	printf ("(err) %d %.*s\n", code, len_host, &data[1 + 8]);
	return (int32_t) (1 + 8 + len_host);
      }

    case SER_STR:
      if (size < 1 + 4)
	{
	  msg ("bad response");
	  return -1;
	}
      {
	// Deserialize Length (uint32_t)
	memcpy (&len_net, &data[1], 4);
	len_host = ntohl (len_net);

	if (size < 1 + 4 + len_host)
	  {
	    msg ("bad response");
	    return -1;
	  }
	printf ("(str) %.*s\n", len_host, &data[1 + 4]);
	return (int32_t) (1 + 4 + len_host);
      }

    case SER_INT:
      if (size < 1 + 8)
	{
	  msg ("bad response");
	  return -1;
	}
      {
	// Deserialize 64-bit Integer
	uint64_t val_net = 0;
	memcpy (&val_net, &data[1], 8);
	int64_t val = (int64_t) ntohll (val_net);

	printf ("(int) %ld\n", val);
	return 1 + 8;
      }

    case SER_DBL:
      // NOTE: Network conversion for doubles is non-standard.
      // Floating point standards (IEEE 754) often rely on raw byte order.
      // For simplicity and common practice double is treate as raw bytes, but this is a *potential* portability issue. 
      if (size < 1 + 8)
	{
	  msg ("bad response");
	  return -1;
	}
      {
	double val = 0;
	memcpy (&val, &data[1], 8);
	printf ("(dbl) %g\n", val);
	return 1 + 8;
      }

    case SER_ARR:
      if (size < 1 + 4)
	{
	  msg ("bad response");
	  return -1;
	}
      {
	// Deserialize Array Length (uint32_t)
	memcpy (&len_net, &data[1], 4);
	len_host = ntohl (len_net);

	printf ("(arr) len=%u\n", len_host);
	size_t arr_bytes = 1 + 4;
	for (uint32_t i = 0; i < len_host; ++i)
	  {
	    int32_t rv = on_response (&data[arr_bytes], size - arr_bytes);
	    if (rv < 0)
	      {
		return rv;
	      }
	    arr_bytes += (size_t) rv;
	  }
	printf ("(arr) end\n");
	return (int32_t) arr_bytes;
      }

    default:
      msg ("bad response");
      return -1;
    }
}

static int32_t
read_res (int fd)
{
  // 4 bytes header + K_MAX_MSG (payload limit) + 1 (for NULL terminator safety)
  char rbuf[4 + K_MAX_MSG + 1];
  errno = 0;
  int32_t err = read_full (fd, rbuf, 4);
  if (err)
    {
      if (errno == 0)
	{
	  msg ("EOF");
	}
      else
	{
	  msg ("read() error");
	}
      return err;
    }

  uint32_t len_net = 0;
  uint32_t len_host = 0;

  // Deserialize Total Length (uint32_t)
  memcpy (&len_net, rbuf, 4);
  len_host = ntohl (len_net);

  if (len_host > K_MAX_MSG)
    {
      msg ("too long");
      return -1;
    }

  // reply body
  err = read_full (fd, &rbuf[4], len_host);
  if (err)
    {
      msg ("read() error");
      return err;
    }

  int32_t rv = on_response ((uint8_t *) & rbuf[4], len_host);
  if (rv > 0 && (uint32_t) rv != len_host)
    {
      msg ("bad response");
      rv = -1;
    }
  return rv;
}

int
main (int argc, char **argv)
{
  int fd = socket (AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    {
      die ("socket()");
    }

  struct sockaddr_in addr = { 0 };
  addr.sin_family = AF_INET;
  addr.sin_port = ntohs (parse_port (argc, argv));
  addr.sin_addr.s_addr = ntohl (INADDR_LOOPBACK);	// 127.0.0.1
  int rv = connect (fd, (const struct sockaddr *) &addr, sizeof (addr));
  if (rv)
    {
      die ("connect");
    }

  size_t size = 0;
  char **cmds = malloc (sizeof (char *) * (size_t) argc);
  if (!cmds)
    die ("Out of memory cmds");
  for (int i = 1; i < argc; ++i)
    {
      // Skip the -p flag and the argument that follows it (the port number)
      if (strcmp (argv[i], "-p") == 0)
	{
	  i++;
	  continue;
	}
      cmds[size++] = argv[i];
    }
  int32_t err = send_req (fd, cmds, size);
  if (err)
    {
      goto L_DONE;
    }
  err = read_res (fd);
  if (err)
    {
      goto L_DONE;
    }

L_DONE:
  free (cmds);
  close (fd);
  return 0;
}
