#!/usr/bin/env python3
"""Load generator for ShardKV

Adapted parts:
- NUM_BACKENDS = 3
- deterministic FNV-1a shard mapping matching the C++ gateway
- protocol parser accepts optional GET value
- optional consistency check using a reserved key not touched by the workload
"""

import argparse
import csv
import math
import random
import socket
import threading
import time

NUM_KEYS = 100000
VALUE_SIZE = 128
MIX = (("GET", 0.90), ("PUT", 0.08), ("DELETE", 0.02))
NUM_BACKENDS = 3


def key_name(i: int) -> str:
    return f"key-{i:08d}"


def fnv1a32(data: bytes) -> int:
    h = 2166136261
    for b in data:
        h ^= b
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def shard_of(key: str) -> int:
    return fnv1a32(key.encode()) % NUM_BACKENDS


def encode_request(request_id: int, op: str, key: str, value: str | None) -> bytes:
    if value is not None:
        return f"{request_id} {op} {key} {value}\n".encode()
    return f"{request_id} {op} {key}\n".encode()


def parse_response(line: bytes) -> tuple[int, str, str | None]:
    parts = line.decode().strip().split(maxsplit=2)
    if len(parts) < 2:
        raise ValueError("malformed response")
    rid = int(parts[0])
    status = parts[1]
    value = parts[2] if len(parts) == 3 else None
    return rid, status, value


def make_key_picker(rng: random.Random, hotset: bool):
    if not hotset:
        return lambda: rng.randrange(NUM_KEYS)
    lam = math.log(10) / (0.1 * NUM_KEYS)

    def pick() -> int:
        u = rng.random() or 1e-12
        return int(-math.log(u) / lam) % NUM_KEYS

    return pick


def pick_op(rng: random.Random) -> str:
    x = rng.random()
    acc = 0.0
    for op, p in MIX:
        acc += p
        if x < acc:
            return op
    return MIX[-1][0]


class Client(threading.Thread):
    def __init__(self, cid, args, t_start, t_end, samples, lock):
        super().__init__(daemon=True)
        self.cid = cid
        self.args = args
        self.t_start, self.t_end = t_start, t_end
        self.samples, self.lock = samples, lock
        self.rng = random.Random(args.seed * 100_003 + cid)
        self.pick_key = make_key_picker(self.rng, args.hotset)
        self.request_id = cid

    def run(self):
        sock = rfile = None
        local = []
        while time.monotonic() < self.t_end:
            self.request_id += self.args.clients
            op = pick_op(self.rng)
            key = key_name(self.pick_key())
            value = "x" * VALUE_SIZE if op == "PUT" else None
            t0 = time.monotonic()
            reconnect = False
            try:
                if sock is None:
                    sock = socket.create_connection(
                        (self.args.host, self.args.port), timeout=self.args.timeout
                    )
                    rfile = sock.makefile("rb")
                sock.sendall(encode_request(self.request_id, op, key, value))
                line = rfile.readline()
                if not line:
                    status = "CONNECTION_CLOSED"
                    reconnect = True
                else:
                    rid, status, _ = parse_response(line)
                    if rid != self.request_id:
                        status = "PROTOCOL_ERROR"
                        reconnect = True
            except socket.timeout:
                status = "CLIENT_TIMEOUT"
                reconnect = True
            except OSError:
                status = "CONNECTION_ERROR"
                reconnect = True
            except (ValueError, IndexError):
                status = "PROTOCOL_ERROR"
                reconnect = True

            t1 = time.monotonic()
            local.append(
                (t1 - self.t_start, (t1 - t0) * 1000.0, status, shard_of(key), op)
            )

            if reconnect:
                if rfile is not None:
                    try:
                        rfile.close()
                    except OSError:
                        pass
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass
                sock = rfile = None
                time.sleep(min(0.1, max(0.0, self.t_end - time.monotonic())))

        with self.lock:
            self.samples.extend(local)
        if rfile is not None:
            rfile.close()
        if sock is not None:
            sock.close()


def percentile(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    idx = min(
        len(sorted_vals) - 1,
        max(0, math.ceil(p / 100 * len(sorted_vals)) - 1),
    )
    return sorted_vals[idx]


def kept_samples(samples, warmup, duration):
    return [
        s
        for s in samples
        if s[0] - s[1] / 1000 >= warmup and s[0] <= duration
    ]


def report(samples, warmup, duration, label):
    kept = kept_samples(samples, warmup, duration)
    lat = sorted(s[1] for s in kept)
    window = duration - warmup
    rate = len(kept) / window if window > 0 else 0.0
    print(f"--- {label}: {len(kept)} requests in {window:.0f} s ({rate:.0f} req/s)")

    status_counts = {}
    for s in kept:
        status_counts[s[2]] = status_counts.get(s[2], 0) + 1
    for st, n in sorted(status_counts.items(), key=lambda kv: -kv[1]):
        print(f"    {st:22s} {n:8d} ({100*n/len(kept) if kept else 0:.2f} %)")

    for op, _ in MIX:
        n = sum(s[4] == op for s in kept)
        print(f"    {op:6s} {n:8d} ({100*n/len(kept) if kept else 0:.2f} %)")

    print(
        f"    latency [ms] p50={percentile(lat, 50):.2f} "
        f"p95={percentile(lat, 95):.2f} p99={percentile(lat, 99):.2f}"
    )


def one_request(host, port, timeout, rid, op, key, value=None):
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        rfile = sock.makefile("rb")
        sock.sendall(encode_request(rid, op, key, value))
        line = rfile.readline()
        if not line:
            raise RuntimeError("connection closed during verification")
        return parse_response(line)


def verify_reserved_key(args, phase):
    key = "verify-key-00000001"
    value = "known-value-aufgabe3"
    if phase == "before":
        rid, st, _ = one_request(args.host, args.port, args.timeout, 9_000_001, "PUT", key, value)
        if st != "OK":
            raise RuntimeError(f"verification PUT failed: {rid} {st}")
        print(f"verification key prepared on shard {shard_of(key)}")
    else:
        rid, st, got = one_request(args.host, args.port, args.timeout, 9_000_002, "GET", key)
        if st != "OK" or got != value:
            raise RuntimeError(f"verification GET failed: {rid} {st} value={got!r}")
        print("verification GET after run: OK")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--clients", type=int, default=64)
    ap.add_argument("--duration", type=float, default=70.0)
    ap.add_argument("--warmup", type=float, default=10.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--hotset", action="store_true")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--no-verify", action="store_true")
    args = ap.parse_args()

    if args.clients < 1:
        ap.error("--clients must be >= 1; Aufgabe 3 requires at least 64")
    if not (0 <= args.warmup < args.duration):
        ap.error("required: 0 <= warmup < duration")
    if args.timeout <= 0:
        ap.error("timeout must be positive")

    if not args.no_verify:
        verify_reserved_key(args, "before")

    samples, lock = [], threading.Lock()
    t_start = time.monotonic()
    t_end = t_start + args.duration
    clients = [Client(i, args, t_start, t_end, samples, lock) for i in range(args.clients)]
    print(
        f"start {args.clients} clients -> {args.host}:{args.port}, duration={args.duration:.0f}s, "
        f"warmup={args.warmup:.0f}s, seed={args.seed}, hotset={args.hotset}"
    )
    for c in clients:
        c.start()
    for c in clients:
        c.join()

    report(samples, args.warmup, args.duration, "all")
    for shard in range(NUM_BACKENDS):
        report([s for s in samples if s[3] == shard], args.warmup, args.duration, f"shard {shard}")

    with open(args.csv, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["t_rel_s", "lat_ms", "status", "shard", "op"])
        w.writerows(samples)
    print(f"CSV written: {args.csv}")

    if not args.no_verify:
        verify_reserved_key(args, "after")


if __name__ == "__main__":
    main()
