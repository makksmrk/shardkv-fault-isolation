#!/usr/bin/env python3
import argparse
import socket
import threading


def encode(rid, key, value):
    return f"{rid} PUT {key} {value}\n".encode()


def worker(tid, threads, host, port, nkeys, timeout, errors):
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.settimeout(timeout)
            rf = s.makefile("rb")
            for i in range(tid, nkeys, threads):
                key = f"key-{i:08d}"
                rid = 100_000_000 + i
                s.sendall(encode(rid, key, "init"))
                line = rf.readline()
                if not line:
                    raise RuntimeError("connection closed")
                parts = line.decode().strip().split(maxsplit=2)
                if len(parts) < 2 or int(parts[0]) != rid or parts[1] != "OK":
                    raise RuntimeError(f"bad response for {key}: {line!r}")
    except Exception as e:
        errors.append((tid, repr(e)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--keys", type=int, default=100000)
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--timeout", type=float, default=5.0)
    args = ap.parse_args()

    errors = []
    ts = [threading.Thread(target=worker, args=(i, args.threads, args.host, args.port, args.keys, args.timeout, errors))
          for i in range(args.threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    if errors:
        for e in errors[:10]:
            print("ERROR", e)
        raise SystemExit(1)
    print(f"seeded {args.keys} keys through gateway")


if __name__ == "__main__":
    main()
