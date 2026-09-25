#!/usr/bin/env python3
import argparse
import csv
import math
from collections import Counter, defaultdict
from statistics import mean

import matplotlib.pyplot as plt


def percentile(values, p):
    if not values:
        return float("nan")
    a = sorted(values)
    idx = min(len(a) - 1, max(0, math.ceil(p / 100.0 * len(a)) - 1))
    return a[idx]


def read_samples(path):
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            t_end = float(r["t_rel_s"])
            lat = float(r["lat_ms"])
            rows.append({
                # The load generator records the request completion timestamp.
                # Keep it as the timeline timestamp so existing measurements remain usable.
                "t": t_end,
                "t_start": t_end - lat / 1000.0,
                "lat": lat,
                "status": r["status"],
                "shard": int(r["shard"]),
                "op": r["op"],
            })
    return rows


def p99_series(rows, unhealthy_shard, start, end, bin_s=1.0):
    bins = defaultdict(list)
    for r in rows:
        # Timeline is based on completion time, exactly as t_rel_s in the
        # provided load-generator skeleton. During a complete stall there can
        # be empty bins; timeout/recovery samples can therefore be isolated.
        t = r["t"]
        if r["shard"] == unhealthy_shard or not (start <= t < end):
            continue
        b = int((t - start) // bin_s)
        bins[b].append(r["lat"])

    n_bins = int(math.ceil((end - start) / bin_s))
    xs = [start + (b + 0.5) * bin_s for b in range(n_bins)]
    ys = [percentile(bins.get(b, []), 99) for b in range(n_bins)]
    return xs, ys


def mean_series(series_list):
    if not series_list:
        return []
    out = []
    for vals in zip(*series_list):
        finite = [v for v in vals if math.isfinite(v)]
        out.append(mean(finite) if finite else float("nan"))
    return out


def summarize(rows, shard_filter, t0, t1):
    # Throughput and latency here use requests that completed in the interval.
    sel = [r for r in rows if t0 <= r["t"] < t1 and shard_filter(r["shard"])]
    lats = [r["lat"] for r in sel]
    statuses = Counter(r["status"] for r in sel)
    okish = statuses["OK"] + statuses["NOT_FOUND"]
    return {
        "requests": len(sel),
        "req_s": len(sel) / (t1 - t0) if t1 > t0 else float("nan"),
        "success_pct": 100.0 * okish / len(sel) if sel else float("nan"),
        "error_pct": 100.0 * (len(sel) - okish) / len(sel) if sel else float("nan"),
        "p50_ms": percentile(lats, 50),
        "p95_ms": percentile(lats, 95),
        "p99_ms": percentile(lats, 99),
    }


def average_dict(dicts):
    out = {}
    for k in dicts[0]:
        vals = [float(d[k]) for d in dicts if math.isfinite(float(d[k]))]
        out[k] = mean(vals) if vals else float("nan")
    return out


def peak_timeline_p99(runs, unhealthy_shard, t0, t1, bin_s):
    peaks = []
    for rows in runs:
        _, ys = p99_series(rows, unhealthy_shard, t0, t1, bin_s)
        vals = [y for y in ys if math.isfinite(y)]
        peaks.append(max(vals) if vals else float("nan"))
    vals = [p for p in peaks if math.isfinite(p)]
    return mean(vals) if vals else float("nan")


def normal_reference(runs, unhealthy_shard, warmup, end):
    healthy = lambda s, u=unhealthy_shard: s != u
    return average_dict([summarize(r, healthy, warmup, end) for r in runs])


def pre_brownout_reference(runs, unhealthy_shard, warmup, brownout_start):
    healthy = lambda s, u=unhealthy_shard: s != u
    return average_dict([summarize(r, healthy, warmup, brownout_start) for r in runs])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline-brownout", nargs="+", required=True)
    ap.add_argument("--isolated-brownout", nargs="+", required=True)
    ap.add_argument("--baseline-normal", nargs="+",
                    help="optional separate normal runs; otherwise pre-brownout part is the reference")
    ap.add_argument("--isolated-normal", nargs="+",
                    help="optional separate normal runs; otherwise pre-brownout part is the reference")
    ap.add_argument("--unhealthy-shard", type=int, default=2)
    ap.add_argument("--warmup", type=float, default=10.0)
    ap.add_argument("--brownout-start", type=float, default=30.0)
    ap.add_argument("--brownout-end", type=float, default=45.0)
    ap.add_argument("--end", type=float, default=70.0)
    ap.add_argument("--bin", type=float, default=1.0)
    ap.add_argument("--out", default="p99_healthy_timeline.png")
    ap.add_argument("--summary", default="summary.csv")
    args = ap.parse_args()

    brownout = {
        "baseline": [read_samples(p) for p in args.baseline_brownout],
        "isolated": [read_samples(p) for p in args.isolated_brownout],
    }
    separate_normal = {
        "baseline": [read_samples(p) for p in args.baseline_normal] if args.baseline_normal else None,
        "isolated": [read_samples(p) for p in args.isolated_normal] if args.isolated_normal else None,
    }

    # Central result plot: p99 time course of healthy shards.
    plt.figure(figsize=(10, 5))
    for variant, runs in brownout.items():
        per_run = []
        xs = None
        for rows in runs:
            xs, ys = p99_series(rows, args.unhealthy_shard, args.warmup, args.end, args.bin)
            per_run.append(ys)
        ys_mean = mean_series(per_run)
        # marker is important: while all workers are blocked there may be no
        # completions. A timeout second can therefore be a single finite point
        # surrounded by NaNs; without a marker matplotlib makes that p99 spike invisible.
        plt.plot(xs, ys_mean, marker="o", markersize=2.8, linewidth=1.2,
                 label=f"{variant} (mean p99, n={len(runs)})")

    plt.axvspan(args.brownout_start, args.brownout_end, alpha=0.2, label="Brownout")
    plt.xlabel("Completion time since load start [s]")
    plt.ylabel("Healthy shards p99 latency [ms]")
    plt.title("ShardKV: p99 latency of healthy shards")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.out, dpi=160)
    print(f"wrote {args.out}")

    phases = [
        ("pre_brownout", args.warmup, args.brownout_start),
        ("brownout", args.brownout_start, args.brownout_end),
        ("recovery", args.brownout_end, args.end),
    ]
    fields = ["variant", "run", "group", "phase", "requests", "req_s",
              "success_pct", "error_pct", "p50_ms", "p95_ms", "p99_ms"]

    with open(args.summary, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for variant, runs in brownout.items():
            for phase, t0, t1 in phases:
                for group, filt in [
                    ("healthy", lambda s, u=args.unhealthy_shard: s != u),
                    ("affected", lambda s, u=args.unhealthy_shard: s == u),
                ]:
                    ds = []
                    for idx, rows in enumerate(runs, 1):
                        d = summarize(rows, filt, t0, t1)
                        ds.append(d)
                        w.writerow({"variant": variant, "run": idx, "group": group, "phase": phase, **d})
                    w.writerow({"variant": variant, "run": "mean", "group": group,
                                "phase": phase, **average_dict(ds)})

            if separate_normal[variant]:
                for group, filt in [
                    ("healthy", lambda s, u=args.unhealthy_shard: s != u),
                    ("affected", lambda s, u=args.unhealthy_shard: s == u),
                ]:
                    ds = []
                    for idx, rows in enumerate(separate_normal[variant], 1):
                        d = summarize(rows, filt, args.warmup, args.end)
                        ds.append(d)
                        w.writerow({"variant": variant, "run": idx, "group": group,
                                    "phase": "normal_run", **d})
                    w.writerow({"variant": variant, "run": "mean", "group": group,
                                "phase": "normal_run", **average_dict(ds)})

    print(f"wrote {args.summary}")

    # Explicit success checks. Aggregate phase p99 is reported, but for the
    # baseline also show the peak 1-s p99 visible in the required time-series.
    healthy = lambda s, u=args.unhealthy_shard: s != u
    for variant, runs in brownout.items():
        if separate_normal[variant]:
            normal = normal_reference(separate_normal[variant], args.unhealthy_shard,
                                      args.warmup, args.end)
            normal_source = "separate normal runs"
        else:
            normal = pre_brownout_reference(runs, args.unhealthy_shard,
                                             args.warmup, args.brownout_start)
            normal_source = "pre-brownout window"

        brown = average_dict([
            summarize(r, healthy, args.brownout_start, args.brownout_end)
            for r in runs
        ])
        peak = peak_timeline_p99(runs, args.unhealthy_shard,
                                 args.brownout_start, args.brownout_end, args.bin)
        phase_ratio = brown["p99_ms"] / normal["p99_ms"] if normal["p99_ms"] > 0 else float("nan")
        peak_ratio = peak / normal["p99_ms"] if normal["p99_ms"] > 0 else float("nan")
        print(f"\n{variant} (normal reference: {normal_source})")
        print(f"  normal healthy p99:           {normal['p99_ms']:.2f} ms")
        print(f"  brownout healthy phase p99:   {brown['p99_ms']:.2f} ms ({phase_ratio:.2f}x)")
        print(f"  brownout peak {args.bin:g}s p99:      {peak:.2f} ms ({peak_ratio:.2f}x)")
        print(f"  healthy throughput ratio:     {100.0 * throughput_ratio:.1f}%")


if __name__ == "__main__":
    main()
