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
// proj
#include "common.h"

static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;  // error, or unexpected EOF
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_all(int fd, const char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0) {
            return -1;  // error
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

const size_t k_max_msg = 4096;

static int32_t send_req(int fd, char** cmd, size_t cmd_size) {
    uint32_t len = 4;
    for (size_t i = 0; i < cmd_size; i++) {
        len += (uint32_t) strlen(cmd[i]) + 4; 
    }

    if (len > k_max_msg) {
        return -1;
    }

    char wbuf[4 + k_max_msg];
    memcpy(&wbuf[0], &len, 4);  // assume little endian
    memcpy(&wbuf[4], &cmd_size, 4);
    size_t cur = 8;
    for (size_t i = 0; i < cmd_size; i++) {
        char* s = cmd[i];
        size_t cmd_len = strlen(s);
        uint32_t p = (uint32_t) cmd_len;
        memcpy(&wbuf[cur], &p, 4);
        memcpy(&wbuf[cur + 4], s, cmd_len);
        cur += 4 + cmd_len;
    }
    return write_all(fd, wbuf, 4 + len);
}

static int32_t on_response(const uint8_t *data, size_t size) {
    if (size < 1) {
        msg("bad response");
        return -1;
    }
    switch (data[0]) {
    case SER_NIL:
        printf("(nil)\n");
        return 1;
    case SER_ERR:
        if (size < 1 + 8) {
            msg("bad response");
            return -1;
        }
        {
            int32_t code = 0;
            uint32_t len = 0;
            memcpy(&code, &data[1], 4);
            memcpy(&len, &data[1 + 4], 4);
            if (size < 1 + 8 + len) {
                msg("bad response");
                return -1;
            }
            printf("(err) %d %.*s\n", code, len, &data[1 + 8]);
            return (int32_t) (1 + 8 + len);
        }
    case SER_STR:
        if (size < 1 + 4) {
            msg("bad response");
            return -1;
        }
        {
            uint32_t len = 0;
            memcpy(&len, &data[1], 4);
            if (size < 1 + 4 + len) {
                msg("bad response");
                return -1;
            }
            printf("(str) %.*s\n", len, &data[1 + 4]);
            return (int32_t) (1 + 4 + len);
        }
    case SER_INT:
        if (size < 1 + 8) {
            msg("bad response");
            return -1;
        }
        {
            int64_t val = 0;
            memcpy(&val, &data[1], 8);
            printf("(int) %ld\n", val);
            return 1 + 8;
        }
    case SER_DBL:
        if (size < 1 + 8) {
            msg("bad response");
            return -1;
        }
        {
            double val = 0;
            memcpy(&val, &data[1], 8);
            printf("(dbl) %g\n", val);
            return 1 + 8;
        }
    case SER_ARR:
        if (size < 1 + 4) {
            msg("bad response");
            return -1;
        }
        {
            uint32_t len = 0;
            memcpy(&len, &data[1], 4);
            printf("(arr) len=%u\n", len);
            size_t arr_bytes = 1 + 4;
            for (uint32_t i = 0; i < len; ++i) {
                int32_t rv = on_response(&data[arr_bytes], size - arr_bytes);
                if (rv < 0) {
                    return rv;
                }
                arr_bytes += (size_t)rv;
            }
            printf("(arr) end\n");
            return (int32_t)arr_bytes;
        }
    default:
        msg("bad response");
        return -1;
    }
}

static int32_t read_res(int fd) {
    // 4 bytes header
    char rbuf[4 + k_max_msg + 1];
    errno = 0;
    int32_t err = read_full(fd, rbuf, 4);
    if (err) {
        if (errno == 0) {
            msg("EOF");
        } else {
            msg("read() error");
        }
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, rbuf, 4);  // assume little endian
    if (len > k_max_msg) {
        msg("too long");
        return -1;
    }

    // reply body
    err = read_full(fd, &rbuf[4], len);
    if (err) {
        msg("read() error");
        return err;
    }

    // print the result
    int32_t rv = on_response((uint8_t *)&rbuf[4], len);
    if (rv > 0 && (uint32_t)rv != len) {
        msg("bad response");
        rv = -1;
    }
    return rv;
}

int main(int argc, char **argv) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(PORT);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);  // 127.0.0.1
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("connect");
    }

    size_t size = 0;
    char** cmds = malloc(sizeof(char*) * (size_t) argc);
    for (int i = 1; i < argc; ++i) 
        cmds[size++] = argv[i];
    int32_t err = send_req(fd, cmds, size);
    if (err) {
        goto L_DONE;
    }
    err = read_res(fd);
    if (err) {
        goto L_DONE;
    }

L_DONE:
    free(cmds);
    close(fd);
    return 0;
}
