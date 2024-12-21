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
typedef std::vector<uint8_t> Buffer;

static void buf_push_back(Buffer &buf, const uint8_t *data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

static void buf_pop_front(Buffer &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + n);
}

struct Conn {
    int fd = -1;
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;
    Buffer incoming;
    Buffer outgoing;
};

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
        ntohs(client_addr.sin_port));
    listen_set_nb(connfd);
    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    return conn;
}

const size_t k_max_args = 200 * 1000;

static bool read_int(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur + 4 > end) return false;
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

static bool read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out) {
    if (cur + n > end) return false;
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

static int32_t deserialize(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_int(data, end, nstr)) return -1;
    if (nstr > k_max_args) return -1;
    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_int(data, end, len)) return -1;
        out.push_back(std::string());
        if (!read_str(data, end, len, out.back())) return -1;
    }
    if (data != end) return -1;
    return 0;
}

enum { ERR_UNKNOWN = 1, ERR_TOO_BIG = 2 };

enum { TAG_NIL = 0, TAG_ERR = 1, TAG_STR = 2, TAG_INT = 3, TAG_DBL = 4, TAG_ARR = 5 };

static void buf_push_back_u8(Buffer &buf, uint8_t data) { buf.push_back(data); }
static void buf_push_back_u32(Buffer &buf, uint32_t data) { buf_push_back(buf, (const uint8_t *)&data, 4); }
static void buf_push_back_i64(Buffer &buf, int64_t data) { buf_push_back(buf, (const uint8_t *)&data, 8); }
static void buf_push_back_dbl(Buffer &buf, double data) { buf_push_back(buf, (const uint8_t *)&data, 8); }

static void out_nil(Buffer &out) { buf_push_back_u8(out, TAG_NIL); }
static void out_str(Buffer &out, const char *s, size_t size) {
    buf_push_back_u8(out, TAG_STR);
    buf_push_back_u32(out, (uint32_t)size);
    buf_push_back(out, (const uint8_t *)s, size);
}
static void out_int(Buffer &out, int64_t val) {
    buf_push_back_u8(out, TAG_INT);
    buf_push_back_i64(out, val);
}
static void out_dbl(Buffer &out, double val) {
    buf_push_back_u8(out, TAG_DBL);
    buf_push_back_dbl(out, val);
}
static void out_err(Buffer &out, uint32_t code, const std::string &message) {
    buf_push_back_u8(out, TAG_ERR);
    buf_push_back_u32(out, code);
    buf_push_back_u32(out, (uint32_t)message.size());
    buf_push_back(out, (const uint8_t *)message.data(), message.size());
}
static void out_arr(Buffer &out, uint32_t n) {
    buf_push_back_u8(out, TAG_ARR);
    buf_push_back_u32(out, n);
}

static struct { HMap db; } data_store;

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
    for (size_t i = 0; i < len; i++) h = (h + data[i]) * 0x01000193;
    return h;
}

static void do_get(std::vector<std::string> &commands, Buffer &out) {
    Entry key;
    key.key.swap(commands[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&data_store.db, &key.node, &entry_eq);
    if (!node) return out_nil(out);
    const std::string &val = container_of(node, Entry, node)->val;
    return out_str(out, val.data(), val.size());
}

static void do_set(std::vector<std::string> &commands, Buffer &out) {
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
    return out_nil(out);
}

static void do_del(std::vector<std::string> &commands, Buffer &out) {
    Entry key;
    key.key.swap(commands[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_delete(&data_store.db, &key.node, &entry_eq);
    if (node) delete container_of(node, Entry, node);
    return out_int(out, node ? 1 : 0);
}

static bool cb_keys(HNode *node, void *arg) {
    Buffer &out = *(Buffer *)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(std::vector<std::string> &, Buffer &out) {
    out_arr(out, (uint32_t)hm_size(&data_store.db));
    hm_foreach(&data_store.db, &cb_keys, (void *)&out);
}

static void cmd_execute(std::vector<std::string> &commands, Buffer &out) {
    if (commands.size() == 2 && commands[0] == "get") return do_get(commands, out);
    else if (commands.size() == 3 && commands[0] == "set") return do_set(commands, out);
    else if (commands.size() == 2 && commands[0] == "del") return do_del(commands, out);
    else if (commands.size() == 1 && commands[0] == "keys") return do_keys(commands, out);
    else return out_err(out, ERR_UNKNOWN, "unknown command.");
}

static void response_begin(Buffer &out, size_t *header) {
    *header = out.size();
    buf_push_back_u32(out, 0);
}

static size_t response_size(Buffer &out, size_t header) {
    return out.size() - header - 4;
}

static void response_end(Buffer &out, size_t header) {
    size_t message_size = response_size(out, header);
    if (message_size > k_max_message) {
        out.resize(header + 4);
        out_err(out, ERR_TOO_BIG, "response is too big.");
        message_size = response_size(out, header);
    }
    uint32_t len = (uint32_t)message_size;
    memcpy(&out[header], &len, 4);
}

static bool handle_single_request(Conn *conn) {
    if (conn->incoming.size() < 4) return false;
    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_message) {
        message("too long");
        conn->want_close = true;
        return false;
    }
    if (4 + len > conn->incoming.size()) return false;
    const uint8_t *request = &conn->incoming[4];
    std::vector<std::string> commands;
    if (deserialize(request, len, commands) < 0) {
        message("bad request");
        conn->want_close = true;
        return false;
    }
    size_t header_pos = 0;
    response_begin(conn->outgoing, &header_pos);
    cmd_execute(commands, conn->outgoing);
    response_end(conn->outgoing, header_pos);
    buf_pop_front(conn->incoming, 4 + len);
    return true;
}

static void handle_write(Conn *conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) return;
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
    if (rv < 0 && errno == EAGAIN) return;
    if (rv < 0) {
        message_errno("read() error");
        conn->want_close = true;
        return;
    }
    if (rv == 0) {
        if (conn->incoming.size() == 0) message("client closed");
        else message("unexpected EOF");
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
    if (fd < 0) die("socket()");
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv) die("bind()");
    listen_set_nb(fd);
    rv = listen(fd, SOMAXCONN);
    if (rv) die("listen()");
    std::vector<Conn *> fd2connMap;
    std::vector<struct pollfd> checklist;
    while (true) {
        checklist.clear();
        struct pollfd tempollfd = {fd, POLLIN, 0};
        checklist.push_back(tempollfd);
        for (Conn *conn : fd2connMap) {
            if (!conn) continue;
            struct pollfd tempollfd = {conn->fd, POLLERR, 0};
            if (conn->want_read) tempollfd.events |= POLLIN;
            if (conn->want_write) tempollfd.events |= POLLOUT;
            checklist.push_back(tempollfd);
        }
        int rv = poll(checklist.data(), (nfds_t)checklist.size(), -1);
        if (rv < 0 && errno == EINTR) continue;
        if (rv < 0) die("poll");
        if (checklist[0].revents) {
            if (Conn *conn = handle_new_conn(fd)) {
                if (fd2connMap.size() <= (size_t)conn->fd) fd2connMap.resize(conn->fd + 1);
                assert(!fd2connMap[conn->fd]);
                fd2connMap[conn->fd] = conn;
            }
        }
        for (size_t i = 1; i < checklist.size(); ++i) {
            uint32_t doable = checklist[i].revents;
            if (doable == 0) continue;
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
