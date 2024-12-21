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
#include <string>
#include <vector>

static void message(const char *message) {
    fprintf(stderr, "%s\n", message);
}

static void die(const char *message) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, message);
    abort();
}

static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;
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
            return -1;
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

const size_t k_max_message = 4096;

static int32_t send_req(int fd, const std::vector<std::string> &commands) {
    uint32_t len = 4;
    for (const std::string &s : commands) {
        len += 4 + s.size();
    }
    if (len > k_max_message) {
        return -1;
    }

    char wbuf[4 + k_max_message];
    memcpy(&wbuf[0], &len, 4);
    uint32_t n = (uint32_t)commands.size();
    memcpy(&wbuf[4], &n, 4);
    size_t cur = 8;
    for (const std::string &s : commands) {
        uint32_t p = (uint32_t)s.size();
        memcpy(&wbuf[cur], &p, 4);
        memcpy(&wbuf[cur + 4], s.data(), s.size());
        cur += 4 + s.size();
    }
    return write_all(fd, wbuf, 4 + len);
}

enum {
    TAG_NIL = 0,
    TAG_ERR = 1,
    TAG_STR = 2,
    TAG_INT = 3,
    TAG_DBL = 4,
    TAG_ARR = 5,
};

static int32_t print_response(const uint8_t *data, size_t size) {
    if (size < 1) {
        message("bad response");
        return -1;
    }
    switch (data[0]) {
    case TAG_NIL:
        printf("(nil)\n");
        return 1;
    case TAG_ERR: {
        if (size < 9) {
            message("bad response");
            return -1;
        }
        int32_t code = 0;
        uint32_t len = 0;
        memcpy(&code, &data[1], 4);
        memcpy(&len, &data[5], 4);
        if (size < 9 + len) {
            message("bad response");
            return -1;
        }
        printf("(err) %d %.*s\n", code, len, &data[9]);
        return 9 + len;
    }
    case TAG_STR: {
        if (size < 5) {
            message("bad response");
            return -1;
        }
        uint32_t len = 0;
        memcpy(&len, &data[1], 4);
        if (size < 5 + len) {
            message("bad response");
            return -1;
        }
        printf("(str) %.*s\n", len, &data[5]);
        return 5 + len;
    }
    case TAG_INT: {
        if (size < 9) {
            message("bad response");
            return -1;
        }
        int64_t val = 0;
        memcpy(&val, &data[1], 8);
        printf("(int) %ld\n", val);
        return 9;
    }
    case TAG_DBL: {
        if (size < 9) {
            message("bad response");
            return -1;
        }
        double val = 0;
        memcpy(&val, &data[1], 8);
        printf("(dbl) %g\n", val);
        return 9;
    }
    case TAG_ARR: {
        if (size < 5) {
            message("bad response");
            return -1;
        }
        uint32_t len = 0;
        memcpy(&len, &data[1], 4);
        printf("(arr) len=%u\n", len);
        size_t arr_bytes = 5;
        for (uint32_t i = 0; i < len; ++i) {
            int32_t rv = print_response(&data[arr_bytes], size - arr_bytes);
            if (rv < 0) {
                return rv;
            }
            arr_bytes += (size_t)rv;
        }
        printf("(arr) end\n");
        return (int32_t)arr_bytes;
    }
    default:
        message("bad response");
        return -1;
    }
}

static int32_t read_res(int fd) {
    char rbuf[4 + k_max_message + 1];
    errno = 0;
    int32_t err = read_full(fd, rbuf, 4);
    if (err) {
        if (errno == 0) {
            message("EOF");
        } else {
            message("read() error");
        }
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, rbuf, 4);
    if (len > k_max_message) {
        message("too long");
        return -1;
    }

    err = read_full(fd, &rbuf[4], len);
    if (err) {
        message("read() error");
        return err;
    }

    int32_t rv = print_response((uint8_t *)&rbuf[4], len);
    if (rv > 0 && (uint32_t)rv != len) {
        message("bad response");
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
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("connect");
    }

    std::vector<std::string> commands;
    for (int i = 1; i < argc; ++i) {
        commands.push_back(argv[i]);
    }
    int32_t err = send_req(fd, commands);
    if (err) {
        goto L_DONE;
    }
    err = read_res(fd);
    if (err) {
        goto L_DONE;
    }

L_DONE:
    close(fd);
    return 0;
}
