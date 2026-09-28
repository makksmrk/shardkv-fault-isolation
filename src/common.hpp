#pragma once

#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace shardkv {

using Clock = std::chrono::steady_clock;

struct Request {
    std::uint64_t id{};
    std::string op;
    std::string key;
    std::string value;
};

struct Response {
    std::uint64_t id{};
    std::string status;
    std::string value;
};

inline bool parse_request(const std::string& line, Request& r) {
    std::istringstream iss(line);
    if (!(iss >> r.id >> r.op >> r.key)) return false;
    r.value.clear();
    if (r.op == "PUT") {
        if (!(iss >> r.value)) return false;
    }
    return r.op == "GET" || r.op == "PUT" || r.op == "DELETE";
}

inline bool parse_response(const std::string& line, Response& r) {
    std::istringstream iss(line);
    if (!(iss >> r.id >> r.status)) return false;
    r.value.clear();
    if (r.status == "OK") {
        iss >> r.value; // GET may return a value; PUT/DELETE may not.
    }
    return true;
}

inline std::string encode_request(const Request& r) {
    std::string out = std::to_string(r.id) + " " + r.op + " " + r.key;
    if (r.op == "PUT") out += " " + r.value;
    out += "\n";
    return out;
}

inline std::string encode_response(const Response& r) {
    std::string out = std::to_string(r.id) + " " + r.status;
    if (!r.value.empty()) out += " " + r.value;
    out += "\n";
    return out;
}

inline std::uint32_t fnv1a32(const std::string& s) {
    std::uint32_t h = 2166136261u;
    for (unsigned char c : s) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

inline std::size_t shard_of(const std::string& key, std::size_t n) {
    return static_cast<std::size_t>(fnv1a32(key) % n);
}

inline bool send_all(int fd, const std::string& data) {
    std::size_t off = 0;
    while (off < data.size()) {
        ssize_t n = ::send(fd, data.data() + off, data.size() - off, MSG_NOSIGNAL);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

class LineReader {
public:
    explicit LineReader(int fd) : fd_(fd) {}

    bool read_line(std::string& out, std::size_t max_len = 4096) {
        while (true) {
            auto p = buf_.find('\n');
            if (p != std::string::npos) {
                out.assign(buf_.data(), p);
                buf_.erase(0, p + 1);
                return true;
            }
            if (buf_.size() > max_len) return false;
            char tmp[4096];
            ssize_t n = ::recv(fd_, tmp, sizeof(tmp), 0);
            if (n > 0) {
                buf_.append(tmp, static_cast<std::size_t>(n));
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
    }

private:
    int fd_;
    std::string buf_;
};

inline int make_listener(std::uint16_t port, int backlog = 256) {
    int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int off = 0;
    ::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_any;
    addr.sin6_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    if (::listen(fd, backlog) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

inline bool split_host_port(const std::string& spec, std::string& host, std::string& port) {
    auto p = spec.rfind(':');
    if (p == std::string::npos || p == 0 || p + 1 >= spec.size()) return false;
    host = spec.substr(0, p);
    port = spec.substr(p + 1);
    return true;
}

inline int connect_with_timeout(const std::string& host, const std::string& port, int timeout_ms) {
    addrinfo hints{};
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) return -1;

    int result_fd = -1;
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        int fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;

        if (timeout_ms < 0) {
            if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
                result_fd = fd;
                break;
            }
            ::close(fd);
            continue;
        }

        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        int rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc == 0) {
            ::fcntl(fd, F_SETFL, flags);
            result_fd = fd;
            break;
        }
        if (errno != EINPROGRESS) {
            ::close(fd);
            continue;
        }

        pollfd pfd{fd, POLLOUT, 0};
        rc = ::poll(&pfd, 1, timeout_ms);
        if (rc > 0) {
            int err = 0;
            socklen_t len = sizeof(err);
            ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
            if (err == 0) {
                ::fcntl(fd, F_SETFL, flags);
                result_fd = fd;
                break;
            }
        }
        ::close(fd);
    }

    ::freeaddrinfo(res);
    return result_fd;
}

inline bool wait_readable(int fd, int timeout_ms) {
    if (timeout_ms < 0) return true;
    pollfd pfd{fd, POLLIN, 0};
    while (true) {
        int rc = ::poll(&pfd, 1, timeout_ms);
        if (rc > 0) return (pfd.revents & (POLLIN | POLLHUP)) != 0;
        if (rc == 0) return false;
        if (errno == EINTR) continue;
        return false;
    }
}

inline double ms_since(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

constexpr std::array<double, 12> LAT_BUCKETS_MS{
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000
};

inline std::size_t latency_bucket(double ms) {
    for (std::size_t i = 0; i < LAT_BUCKETS_MS.size(); ++i) {
        if (ms <= LAT_BUCKETS_MS[i]) return i;
    }
    return LAT_BUCKETS_MS.size();
}

} // namespace shardkv
