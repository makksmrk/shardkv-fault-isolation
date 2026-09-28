#include "common.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using namespace shardkv;

namespace {

constexpr std::size_t NUM_BACKENDS = 3;
constexpr int WORKERS = 16;
constexpr std::size_t QUEUE_CAPACITY = 1024;

struct StatsSnapshot {
    std::uint64_t requests = 0, ok = 0, not_found = 0, timeout = 0,
                  overloaded = 0, unavailable = 0, other = 0;
    std::array<std::uint64_t, LAT_BUCKETS_MS.size() + 1> buckets{};
    std::array<double, NUM_BACKENDS> queue_wait_ms_sum{};
    std::array<std::uint64_t, NUM_BACKENDS> queue_wait_samples{};
};

struct WindowStats {
    std::mutex mu;
    StatsSnapshot s;

    void record_response(const std::string& status, double latency_ms) {
        std::lock_guard<std::mutex> g(mu);
        ++s.requests;
        if (status == "OK") ++s.ok;
        else if (status == "NOT_FOUND") ++s.not_found;
        else if (status == "TIMEOUT") ++s.timeout;
        else if (status == "OVERLOADED") ++s.overloaded;
        else if (status == "BACKEND_UNAVAILABLE") ++s.unavailable;
        else ++s.other;
        ++s.buckets[latency_bucket(latency_ms)];
    }

    void record_queue_wait(std::size_t shard, double ms) {
        std::lock_guard<std::mutex> g(mu);
        s.queue_wait_ms_sum[shard] += ms;
        ++s.queue_wait_samples[shard];
    }

    StatsSnapshot snapshot_and_reset() {
        std::lock_guard<std::mutex> g(mu);
        StatsSnapshot out = s;
        s = {};
        return out;
    }
};

struct ClientConn {
    explicit ClientConn(int f) : fd(f) {}
    ~ClientConn() { close_now(); }

    bool send_response(const Response& r) {
        std::lock_guard<std::mutex> g(mu);
        if (fd < 0) return false;
        return send_all(fd, encode_response(r));
    }

    void close_now() {
        std::lock_guard<std::mutex> g(mu);
        if (fd >= 0) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
            fd = -1;
        }
    }

    int raw_fd() const { return fd; }

private:
    mutable std::mutex mu;
    int fd;
};

struct Task {
    std::shared_ptr<ClientConn> client;
    Request req;
    Clock::time_point arrival;
};

class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t cap) : cap_(cap) {}

    bool try_push(Task t) {
        std::lock_guard<std::mutex> g(mu_);
        if (q_.size() >= cap_) return false;
        q_.push_back(std::move(t));
        cv_.notify_one();
        return true;
    }

    Task pop() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return !q_.empty(); });
        Task t = std::move(q_.front());
        q_.pop_front();
        return t;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> g(mu_);
        return q_.size();
    }

private:
    std::size_t cap_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Task> q_;
};

class BackendClient {
public:
    BackendClient(std::string host, std::string port)
        : host_(std::move(host)), port_(std::move(port)) {}

    ~BackendClient() { reset(); }

    Response call(const Request& req) {
        // Baseline deliberately has no timeout. A stopped backend can block here.
        for (int attempt = 0; attempt < 1; ++attempt) {
            if (fd_ < 0) {
                fd_ = connect_with_timeout(host_, port_, -1);
                if (fd_ < 0) return {req.id, "BACKEND_UNAVAILABLE", {}};
                reader_ = std::make_unique<LineReader>(fd_);
            }
            if (!send_all(fd_, encode_request(req))) {
                reset();
                continue;
            }
            std::string line;
            if (!reader_->read_line(line)) {
                reset();
                continue;
            }
            Response r;
            if (!parse_response(line, r) || r.id != req.id) {
                reset();
                return {req.id, "BACKEND_UNAVAILABLE", {}};
            }
            return r;
        }
        return {req.id, "BACKEND_UNAVAILABLE", {}};
    }

private:
    void reset() {
        reader_.reset();
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

    std::string host_, port_;
    int fd_ = -1;
    std::unique_ptr<LineReader> reader_;
};

BoundedQueue queue_(QUEUE_CAPACITY);
WindowStats stats;
std::atomic<int> active_workers{0};
std::array<std::atomic<int>, NUM_BACKENDS> waiting_backend{};
std::atomic<int> open_clients{0};
std::array<std::string, NUM_BACKENDS> backend_specs;

void worker_loop() {
    std::array<std::unique_ptr<BackendClient>, NUM_BACKENDS> backends;
    for (std::size_t i = 0; i < NUM_BACKENDS; ++i) {
        std::string host, port;
        split_host_port(backend_specs[i], host, port);
        backends[i] = std::make_unique<BackendClient>(host, port);
    }

    while (true) {
        Task t = queue_.pop();
        active_workers.fetch_add(1);
        auto now = Clock::now();
        std::size_t shard = shard_of(t.req.key, NUM_BACKENDS);
        stats.record_queue_wait(shard, ms_since(t.arrival, now));

        waiting_backend[shard].fetch_add(1);
        Response resp = backends[shard]->call(t.req);
        waiting_backend[shard].fetch_sub(1);

        t.client->send_response(resp);
        stats.record_response(resp.status, ms_since(t.arrival, Clock::now()));
        active_workers.fetch_sub(1);
    }
}

void client_loop(std::shared_ptr<ClientConn> client) {
    open_clients.fetch_add(1);
    LineReader reader(client->raw_fd());
    std::string line;
    while (reader.read_line(line)) {
        Request req;
        auto arrival = Clock::now();
        if (!parse_request(line, req)) {
            Response bad{0, "BAD_REQUEST", {}};
            client->send_response(bad);
            stats.record_response(bad.status, 0.0);
            continue;
        }
        Task t{client, req, arrival};
        if (!queue_.try_push(std::move(t))) {
            Response resp{req.id, "OVERLOADED", {}};
            client->send_response(resp);
            stats.record_response(resp.status, ms_since(arrival, Clock::now()));
        }
    }
    client->close_now();
    open_clients.fetch_sub(1);
}

void metrics_loop(const std::string& path) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    out << "t_s,req_s,ok,not_found,timeout,overloaded,backend_unavailable,other,queue_len,avg_queue_wait_ms,avg_qwait_s0_ms,avg_qwait_s1_ms,avg_qwait_s2_ms,active_workers,waiting_s0,waiting_s1,waiting_s2,open_clients";
    for (double b : LAT_BUCKETS_MS) out << ",lat_le_" << static_cast<int>(b) << "ms";
    out << ",lat_gt_5000ms\n";
    out.flush();

    auto start = Clock::now();
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto s = stats.snapshot_and_reset();
        double t = std::chrono::duration<double>(Clock::now() - start).count();
        double qsum = 0.0;
        std::uint64_t qsamples = 0;
        std::array<double, NUM_BACKENDS> avgq_shard{};
        for (std::size_t i = 0; i < NUM_BACKENDS; ++i) {
            qsum += s.queue_wait_ms_sum[i];
            qsamples += s.queue_wait_samples[i];
            avgq_shard[i] = s.queue_wait_samples[i] ? s.queue_wait_ms_sum[i] / s.queue_wait_samples[i] : 0.0;
        }
        double avgq = qsamples ? qsum / qsamples : 0.0;
        out << t << ',' << s.requests << ',' << s.ok << ',' << s.not_found << ',' << s.timeout << ','
            << s.overloaded << ',' << s.unavailable << ',' << s.other << ',' << queue_.size() << ',' << avgq << ','
            << avgq_shard[0] << ',' << avgq_shard[1] << ',' << avgq_shard[2] << ','
            << active_workers.load() << ',' << waiting_backend[0].load() << ',' << waiting_backend[1].load() << ','
            << waiting_backend[2].load() << ',' << open_clients.load();
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
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <listen_port> <backend0:port> <backend1:port> <backend2:port> <metrics.csv>\n";
        return 1;
    }
    ::signal(SIGPIPE, SIG_IGN);
    std::uint16_t port = static_cast<std::uint16_t>(std::stoi(argv[1]));
    for (std::size_t i = 0; i < NUM_BACKENDS; ++i) {
        backend_specs[i] = argv[2 + i];
        std::string h, p;
        if (!split_host_port(backend_specs[i], h, p)) {
            std::cerr << "Bad backend spec: " << backend_specs[i] << "\n";
            return 1;
        }
    }

    int listener = make_listener(port, 512);
    if (listener < 0) {
        std::cerr << "Cannot listen on port " << port << ": " << std::strerror(errno) << "\n";
        return 1;
    }

    for (int i = 0; i < WORKERS; ++i) std::thread(worker_loop).detach();
    std::thread(metrics_loop, argv[5]).detach();

    std::cout << "baseline gateway: " << WORKERS << " workers, global queue=" << QUEUE_CAPACITY << "\n";
    while (true) {
        int fd = ::accept(listener, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            std::cerr << "accept failed: " << std::strerror(errno) << "\n";
            continue;
        }
        auto client = std::make_shared<ClientConn>(fd);
        std::thread(client_loop, std::move(client)).detach();
    }
}
