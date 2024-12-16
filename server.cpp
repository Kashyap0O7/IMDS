#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>
#include "hashtable.hpp"

#define container_of(ptr, T, member) ((T *)( (char *)ptr - offsetof(T, member) ))

static void message(const char *message) {
    fprintf(stderr, "%s\n", message);
}

static void message_errno(const char *message) {
    fprintf(stderr, "[errno:%d] %s\n", errno, message);
}

static void die(const char *message) {
    fprintf(stderr, "[%d] %s\n", errno, message);
    abort();
}

static void listen_set_nb(int fd) {
    errno = 0;
    int mark = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    mark |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, mark);
    if (errno) {
        die("fcntl error");
    }
}

const size_t k_max_message = 32 << 20;

struct Conn {
    int fd = -1;
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;
    std::vector<uint8_t> incoming;
    std::vector<uint8_t> outgoing;
};

static void buf_push_back(std::vector<uint8_t> &buf, const uint8_t *data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

static void buf_pop_front(std::vector<uint8_t> &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + n);
}

static Conn *handle_new_conn(int fd) {
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0) {
        message_errno("accept() error");
        return NULL;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
    );

    listen_set_nb(connfd);

    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    return conn;
}

const size_t k_max_args = 200 * 1000;

static bool read_int(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur + 4 > end) {
        return false;
    }
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

static bool read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out) {
    if (cur + n > end) {
        return false;
    }
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

static int32_t deserialize(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_int(data, end, nstr)) {
        return -1;
    }
    if (nstr > k_max_args) {
        return -1;
    }

    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_int(data, end, len)) {
            return -1;
        }
        out.push_back(std::string());
        if (!read_str(data, end, len, out.back())) {
            return -1;
        }
    }
    if (data != end) {
        return -1;
    }
    return 0;
}

enum {
    RESP_OK = 0,
    RESP_ERR = 1,
    RESP_NX = 2,
};

struct Response {
    uint32_t status = 0;
    std::vector<uint8_t> data;
};

static struct {
    HMap db;
} data_store;

struct Entry {
    struct HNode node;
    std::string key;
    std::string val;
};

static bool entry_eq(HNode *lhs, HNode *rhs) {
    struct Entry *le = container_of(lhs, struct Entry, node);
    struct Entry *re = container_of(rhs, struct Entry, node);
    return le->key == re->key;
}

static uint64_t str_hash(const uint8_t *data, size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}

static void do_get(std::vector<std::string> &commands, Response &out) {
    Entry key;
    key.key.swap(commands[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&data_store.db, &key.node, &entry_eq);
    if (!node) {
        out.status = RESP_NX;
        return;
    }
    const std::string &val = container_of(node, Entry, node)->val;
    assert(val.size() <= k_max_message);
    out.data.assign(val.begin(), val.end());
}

static void do_set(std::vector<std::string> &commands, Response &) {
    Entry key;
    key.key.swap(commands[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&data_store.db, &key.node, &entry_eq);
    if (node) {
        container_of(node, Entry, node)->val.swap(commands[2]);
    } else {
        Entry *ent = new Entry();
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->val.swap(commands[2]);
        hm_insert(&data_store.db, &ent->node);
    }
}

static void cmd_execute(std::vector<std::string> &commands, Response &out) {
    if (commands.size() == 2 && commands[0] == "get") {
        return do_get(commands, out);
    } else if (commands.size() == 3 && commands[0] == "set") {
        return do_set(commands, out);
    } else if (commands.size() == 2 && commands[0] == "del") {
        return do_del(commands, out);
    } else {
        out.status = RESP_ERR;
    }
}

static void make_response(const Response &resp, std::vector<uint8_t> &out) {
    uint32_t resp_len = 4 + (uint32_t)resp.data.size();
    buf_push_back(out, (const uint8_t *)&resp_len, 4);
    buf_push_back(out, (const uint8_t *)&resp.status, 4);
    buf_push_back(out, resp.data.data(), resp.data.size());
}

static bool handle_single_request(Conn *conn) {
    if (conn->incoming.size() < 4) {
        return false;
    }
    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_message) {
        message("too long");
        conn->want_close = true;
        return false;
    }
    if (4 + len > conn->incoming.size()) {
        return false;
    }
    const uint8_t *request = &conn->incoming[4];

    std::vector<std::string> commands;
    if (deserialize(request, len, commands) < 0) {
        message("bad request");
        conn->want_close = true;
        return false;
    }
    Response resp;
    cmd_execute(commands, resp);
    make_response(resp, conn->outgoing);

    buf_pop_front(conn->incoming, 4 + len);
    return true;
}

static void handle_write(Conn *conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) {
        return;
    }
    if (rv < 0) {
        message_errno("write() error");
        conn->want_close = true;
        return;
    }
    buf_pop_front(conn->outgoing, (size_t)rv);
    if (conn->outgoing.size() == 0) {
        conn->want_read = true;
        conn->want_write = false;
    }
}

static void handle_read(Conn *conn) {
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) {
        return;
    }
    if (rv < 0) {
        message_errno("read() error");
        conn->want_close = true;
        return;
    }
    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            message("client closed");
        } else {
            message("unexpected EOF");
        }
        conn->want_close = true;
        return;
    }
    buf_push_back(conn->incoming, buf, (size_t)rv);

    while (handle_single_request(conn)) {}

    if (conn->outgoing.size() > 0) {
        conn->want_read = false;
        conn->want_write = true;
        return handle_write(conn);
    }
}

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }
    listen_set_nb(fd);
    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    std::vector<Conn *> fd2connMap;
    std::vector<struct pollfd> checklist;
    while (true) {
        checklist.clear();
        struct pollfd tempollfd = {fd, POLLIN, 0};
        checklist.push_back(tempollfd);
        for (Conn *conn : fd2connMap) {
            if (!conn) {
                continue;
            }
            struct pollfd tempollfd = {conn->fd, POLLERR, 0};
            if (conn->want_read) {
                tempollfd.events |= POLLIN;
            }
            if (conn->want_write) {
                tempollfd.events |= POLLOUT;
            }
            checklist.push_back(tempollfd);
        }

        int rv = poll(checklist.data(), (nfds_t)checklist.size(), -1);
        if (rv < 0 && errno == EINTR) {
            continue;
        }
        if (rv < 0) {
            die("poll");
        }

        if (checklist[0].revents) {
            if (Conn *conn = handle_new_conn(fd)) {
                if (fd2connMap.size() <= (size_t)conn->fd) {
                    fd2connMap.resize(conn->fd + 1);
                }
                assert(!fd2connMap[conn->fd]);
                fd2connMap[conn->fd] = conn;
            }
        }

        for (size_t i = 1; i < checklist.size(); ++i) {
            uint32_t doable = checklist[i].revents;
            if (doable == 0) {
                continue;
            }

            Conn *conn = fd2connMap[checklist[i].fd];
            if (doable & POLLIN) {
                assert(conn->want_read);
                handle_read(conn);
            }
            if (doable & POLLOUT) {
                assert(conn->want_write);
                handle_write(conn);
            }

            if ((doable & POLLERR) || conn->want_close) {
                (void)close(conn->fd);
                fd2connMap[conn->fd] = NULL;
                delete conn;
            }
        }
    }
    return 0;
}
