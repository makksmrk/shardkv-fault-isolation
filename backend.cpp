#include "common.hpp"

#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace shardkv;

namespace {

struct StatsSnapshot {
    std::uint64_t requests = 0;
    std::uint64_t ok = 0;
    std::uint64_t not_found = 0;
    std::uint64_t other = 0;
    std::array<std::uint64_t, LAT_BUCKETS_MS.size() + 1> buckets{};
};

struct WindowStats {
    std::mutex mu;
    std::uint64_t requests = 0;
    std::uint64_t ok = 0;
    std::uint64_t not_found = 0;
    std::uint64_t other = 0;
    std::array<std::uint64_t, LAT_BUCKETS_MS.size() + 1> buckets{};

    void record(const std::string& status, double latency_ms) {
        std::lock_guard<std::mutex> g(mu);
        ++requests;
        if (status == "OK") ++ok;
        else if (status == "NOT_FOUND") ++not_found;
        else ++other;
        ++buckets[latency_bucket(latency_ms)];
    }

    StatsSnapshot snapshot_and_reset() {
        std::lock_guard<std::mutex> g(mu);
        StatsSnapshot s;
        s.requests = requests;
        s.ok = ok;
        s.not_found = not_found;
        s.other = other;
        s.buckets = buckets;
        requests = ok = not_found = other = 0;
        buckets.fill(0);
        return s;
    }
};

std::unordered_map<std::string, std::string> kv;
std::shared_mutex kv_mu;
WindowStats stats;
std::atomic<int> open_connections{0};
std::atomic<bool> running{true};

Response handle_request(const Request& r) {
    if (r.op == "GET") {
        std::shared_lock lock(kv_mu);
        auto it = kv.find(r.key);
        if (it == kv.end()) return {r.id, "NOT_FOUND", {}};
        return {r.id, "OK", it->second};
    }
    if (r.op == "PUT") {
        std::unique_lock lock(kv_mu);
        kv[r.key] = r.value;
        return {r.id, "OK", {}};
    }
    if (r.op == "DELETE") {
        std::unique_lock lock(kv_mu);
        auto erased = kv.erase(r.key);
        return {r.id, erased ? "OK" : "NOT_FOUND", {}};
    }
    return {r.id, "BAD_REQUEST", {}};
}

void serve_client(int fd) {
    open_connections.fetch_add(1, std::memory_order_relaxed);
    LineReader reader(fd);
    std::string line;

    while (reader.read_line(line)) {
        auto t0 = Clock::now();
        Request req;
        Response resp;
        if (!parse_request(line, req)) {
            resp = {0, "BAD_REQUEST", {}};
        } else {
            resp = handle_request(req);
        }
        if (!send_all(fd, encode_response(resp))) break;
        stats.record(resp.status, ms_since(t0, Clock::now()));
    }

    ::close(fd);
    open_connections.fetch_sub(1, std::memory_order_relaxed);
}

void metrics_loop(const std::string& path) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    out << "t_s,req_s,ok,not_found,other,open_connections";
    for (double b : LAT_BUCKETS_MS) out << ",lat_le_" << static_cast<int>(b) << "ms";
    out << ",lat_gt_5000ms\n";
    out.flush();

    auto start = Clock::now();
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto s = stats.snapshot_and_reset();
        double t = std::chrono::duration<double>(Clock::now() - start).count();
        out << t << ',' << s.requests << ',' << s.ok << ',' << s.not_found << ',' << s.other
            << ',' << open_connections.load();
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < LAT_BUCKETS_MS.size(); ++i) {
            cumulative += s.buckets[i];
            out << ',' << cumulative;
        }
        out << ',' << s.buckets.back() << '\n';
        out.flush();
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <port> <metrics.csv>\n";
        return 1;
    }

    ::signal(SIGPIPE, SIG_IGN);
    std::uint16_t port = static_cast<std::uint16_t>(std::stoi(argv[1]));
    int listener = make_listener(port);
    if (listener < 0) {
        std::cerr << "Cannot listen on port " << port << ": " << std::strerror(errno) << "\n";
        return 1;
    }

    std::thread metrics(metrics_loop, argv[2]);
    metrics.detach();

    std::cout << "backend listening on port " << port << "\n";
    while (true) {
        int fd = ::accept(listener, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            std::cerr << "accept failed: " << std::strerror(errno) << "\n";
            continue;
        }
        std::thread(serve_client, fd).detach();
    }
}
