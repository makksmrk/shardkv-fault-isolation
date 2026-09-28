#include "common.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

using namespace shardkv;

namespace {

constexpr std::size_t NUM_BACKENDS = 3;
constexpr int WORKERS_PER_SHARD = 5;          // 15 total, baseline has 16.
constexpr std::size_t QUEUE_PER_SHARD = 341;  // 1023 total, baseline has 1024.
constexpr int BACKEND_TIMEOUT_MS = 250;
constexpr int CIRCUIT_OPEN_MS = 1000;

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

class CircuitBreaker {
public:
    // Returns true for normal requests and for exactly one probe after the
    // open interval expires. Other requests fail fast while the circuit is open.
    bool allow_request() {
        std::lock_guard<std::mutex> g(mu_);
        if (!open_) return true;

        const auto now = Clock::now();
        if (now < open_until_) return false;

        if (probe_in_progress_) return false;
        probe_in_progress_ = true;
        return true;
    }

    void on_success() {
        std::lock_guard<std::mutex> g(mu_);
        open_ = false;
        probe_in_progress_ = false;
    }

    void on_failure() {
        std::lock_guard<std::mutex> g(mu_);
        open_ = true;
        probe_in_progress_ = false;
        open_until_ = Clock::now() + std::chrono::milliseconds(CIRCUIT_OPEN_MS);
    }

    bool is_open() const {
        std::lock_guard<std::mutex> g(mu_);
        return open_ && Clock::now() < open_until_;
    }

private:
    mutable std::mutex mu_;
    bool open_ = false;
    bool probe_in_progress_ = false;
    Clock::time_point open_until_{};
};

class BackendClient {
public:
    BackendClient(std::string host, std::string port)
        : host_(std::move(host)), port_(std::move(port)) {}

    ~BackendClient() { reset(); }

    Response call(const Request& req) {
        if (fd_ < 0) {
            fd_ = connect_with_timeout(host_, port_, BACKEND_TIMEOUT_MS);
            if (fd_ < 0) return {req.id, "BACKEND_UNAVAILABLE", {}};

            timeval tv{};
            tv.tv_sec = BACKEND_TIMEOUT_MS / 1000;
            tv.tv_usec = (BACKEND_TIMEOUT_MS % 1000) * 1000;
            ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            reader_ = std::make_unique<LineReader>(fd_);
        }

        if (!send_all(fd_, encode_request(req))) {
            reset();
            return {req.id, "BACKEND_UNAVAILABLE", {}};
        }

        std::string line;
        if (!reader_->read_line(line)) {
            const int e = errno;
            reset();
            if (e == EAGAIN || e == EWOULDBLOCK) {
                return {req.id, "TIMEOUT", {}};
            }
            return {req.id, "BACKEND_UNAVAILABLE", {}};
        }

        Response r;
        if (!parse_response(line, r) || r.id != req.id) {
            reset();
            return {req.id, "BACKEND_UNAVAILABLE", {}};
        }
        return r;
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

std::array<std::unique_ptr<BoundedQueue>, NUM_BACKENDS> queues;
std::array<CircuitBreaker, NUM_BACKENDS> breakers;
WindowStats stats;
std::array<std::atomic<int>, NUM_BACKENDS> active_workers{};
std::array<std::atomic<int>, NUM_BACKENDS> waiting_backend{};
std::atomic<int> open_clients{0};
std::array<std::string, NUM_BACKENDS> backend_specs;

bool is_backend_failure(const std::string& status) {
    return status == "TIMEOUT" || status == "BACKEND_UNAVAILABLE";
}

void worker_loop(std::size_t shard) {
    std::string host, port;
    split_host_port(backend_specs[shard], host, port);
    BackendClient backend(host, port);

    while (true) {
        Task t = queues[shard]->pop();
        active_workers[shard].fetch_add(1);
        stats.record_queue_wait(shard, ms_since(t.arrival, Clock::now()));

        Response resp;
        if (!breakers[shard].allow_request()) {
            // Circuit is open: do not occupy a worker/backend connection for
            // another 250 ms. Fail this shard fast and let the client continue.
            resp = {t.req.id, "OVERLOADED", {}};
        } else {
            waiting_backend[shard].fetch_add(1);
            resp = backend.call(t.req);
            waiting_backend[shard].fetch_sub(1);

            if (is_backend_failure(resp.status)) {
                breakers[shard].on_failure();
            } else {
                breakers[shard].on_success();
            }
        }

        t.client->send_response(resp);
        stats.record_response(resp.status, ms_since(t.arrival, Clock::now()));
        active_workers[shard].fetch_sub(1);
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

        const std::size_t shard = shard_of(req.key, NUM_BACKENDS);

        // Fail fast already at ingress if this shard's circuit is currently open.
        // This keeps the per-shard queue short during a long brownout.
        if (breakers[shard].is_open()) {
            Response resp{req.id, "OVERLOADED", {}};
            client->send_response(resp);
            stats.record_response(resp.status, ms_since(arrival, Clock::now()));
            continue;
        }

        Task t{client, req, arrival};
        if (!queues[shard]->try_push(std::move(t))) {
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
    out << "t_s,req_s,ok,not_found,timeout,overloaded,backend_unavailable,other,"
           "q0,q1,q2,avg_queue_wait_ms,avg_qwait_s0_ms,avg_qwait_s1_ms,avg_qwait_s2_ms,active_s0,active_s1,active_s2,"
           "waiting_s0,waiting_s1,waiting_s2,circuit_s0,circuit_s1,circuit_s2,open_clients";
    for (double b : LAT_BUCKETS_MS) out << ",lat_le_" << static_cast<int>(b) << "ms";
    out << ",lat_gt_5000ms\n";
    out.flush();

    auto start = Clock::now();
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto s = stats.snapshot_and_reset();
        const double t = std::chrono::duration<double>(Clock::now() - start).count();
        double qsum = 0.0;
        std::uint64_t qsamples = 0;
        std::array<double, NUM_BACKENDS> avgq_shard{};
        for (std::size_t i = 0; i < NUM_BACKENDS; ++i) {
            qsum += s.queue_wait_ms_sum[i];
            qsamples += s.queue_wait_samples[i];
            avgq_shard[i] = s.queue_wait_samples[i] ? s.queue_wait_ms_sum[i] / s.queue_wait_samples[i] : 0.0;
        }
        const double avgq = qsamples ? qsum / qsamples : 0.0;

        out << t << ',' << s.requests << ',' << s.ok << ',' << s.not_found << ',' << s.timeout << ','
            << s.overloaded << ',' << s.unavailable << ',' << s.other << ','
            << queues[0]->size() << ',' << queues[1]->size() << ',' << queues[2]->size() << ',' << avgq << ','
            << avgq_shard[0] << ',' << avgq_shard[1] << ',' << avgq_shard[2] << ','
            << active_workers[0].load() << ',' << active_workers[1].load() << ',' << active_workers[2].load() << ','
            << waiting_backend[0].load() << ',' << waiting_backend[1].load() << ',' << waiting_backend[2].load() << ','
            << (breakers[0].is_open() ? 1 : 0) << ',' << (breakers[1].is_open() ? 1 : 0) << ','
            << (breakers[2].is_open() ? 1 : 0) << ',' << open_clients.load();

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
        std::cerr << "Usage: " << argv[0]
                  << " <listen_port> <backend0:port> <backend1:port> <backend2:port> <metrics.csv>\n";
        return 1;
    }

    ::signal(SIGPIPE, SIG_IGN);
    const std::uint16_t port = static_cast<std::uint16_t>(std::stoi(argv[1]));

    for (std::size_t i = 0; i < NUM_BACKENDS; ++i) {
        backend_specs[i] = argv[2 + i];
        std::string h, p;
        if (!split_host_port(backend_specs[i], h, p)) {
            std::cerr << "Bad backend spec: " << backend_specs[i] << "\n";
            return 1;
        }
        queues[i] = std::make_unique<BoundedQueue>(QUEUE_PER_SHARD);
    }

    const int listener = make_listener(port, 512);
    if (listener < 0) {
        std::cerr << "Cannot listen on port " << port << ": " << std::strerror(errno) << "\n";
        return 1;
    }

    for (std::size_t s = 0; s < NUM_BACKENDS; ++s) {
        for (int i = 0; i < WORKERS_PER_SHARD; ++i) {
            std::thread(worker_loop, s).detach();
        }
    }
    std::thread(metrics_loop, argv[5]).detach();

    std::cout << "isolated gateway: 3 x " << WORKERS_PER_SHARD
              << " workers, 3 x " << QUEUE_PER_SHARD
              << " queues, backend timeout=" << BACKEND_TIMEOUT_MS
              << " ms, circuit open=" << CIRCUIT_OPEN_MS << " ms\n";

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
