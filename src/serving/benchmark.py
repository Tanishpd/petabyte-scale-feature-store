#!/usr/bin/env python3
"""Phase 7 acceptance: how fast is the read path, and where does the time go?

Reports four things, because a single "p99" number would hide the interesting
part — how much of a Feast read is Redis, and how much is Feast:

  1. raw Redis HGETALL           — the floor
  2. feast.get_online_features() — what a model actually calls
  3. batch amortisation          — 1 vs 10 vs 100 entities per call
  4. bytes per entity in Redis   — checks SCALING.md's 1.8 KB estimate

Targets (PLAN.md Phase 7): p99 < 10 ms single-entity, < 25 ms for a 100-entity
batch.

    make bench
"""

from __future__ import annotations

import argparse
import statistics
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from contracts.schema import EMBEDDING_DIM, ENTITY_JOIN_KEY, FEATURES  # noqa: E402
from src.flink.sinks.feast_codec import feature_refs, redis_key  # noqa: E402

REPO = ROOT / "src" / "feast_repo"
REDIS_PORT = int(os.environ.get("REDIS_HOST_PORT", "6380"))


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))
    return xs[k]


def summarise(label: str, samples_ms: list[float], per_entity: int = 1) -> dict:
    row = {
        "label": label,
        "n": len(samples_ms),
        "p50": pct(samples_ms, 50),
        "p95": pct(samples_ms, 95),
        "p99": pct(samples_ms, 99),
        "mean": statistics.fmean(samples_ms),
        "per_entity_p50": pct(samples_ms, 50) / per_entity,
    }
    print(f"  {label:<34} p50 {row['p50']:6.2f}  p95 {row['p95']:6.2f}  "
          f"p99 {row['p99']:6.2f}  mean {row['mean']:6.2f}   (ms, n={row['n']})")
    return row


def live_users(client, candidates: range) -> list[int]:
    """Only benchmark entities that actually have features; a miss is a
    different (and faster) code path than a hit."""
    keys = [redis_key(u) for u in candidates]
    with client.pipeline(transaction=False) as pipe:
        for k in keys:
            pipe.exists(k)
        flags = pipe.execute()
    return [u for u, ok in zip(candidates, flags) if ok]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=300)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--repo", default=str(REPO))
    args = ap.parse_args()

    import redis
    from feast import FeatureStore

    client = redis.Redis(host="localhost", port=REDIS_PORT, socket_connect_timeout=3)
    client.ping()

    users = live_users(client, range(1, 400))
    if not users:
        print(f"No feature keys in redis on :{REDIS_PORT}. Run `make smoke` first.")
        return 1
    print(f"benchmarking against {len(users)} live entities\n")

    refs = feature_refs()

    # --- 1. the floor: one raw Redis round trip --------------------------
    print("1. raw Redis (no Feast in the path)")
    samples = []
    for i in range(args.warmup + args.iterations):
        uid = users[i % len(users)]
        t0 = time.perf_counter()
        client.hgetall(redis_key(uid))
        dt = (time.perf_counter() - t0) * 1000
        if i >= args.warmup:
            samples.append(dt)
    raw = summarise("HGETALL, 1 entity", samples)

    # --- 2. what a model calls -------------------------------------------
    #
    # A FRESH FeatureStore per batch size, deliberately. Reads through one
    # instance slow down as calls accumulate — measured 6.9 ms -> 28 ms over
    # ~240 calls on a 100-entity read. It is call-count driven, not elapsed
    # time (30 s idle changes nothing), it is not a connection leak (Redis
    # stays at 2 clients) and it is not a Python object leak (no growth in
    # gc.get_objects). Constructing *any* new FeatureStore in the process
    # restores speed for the existing ones too, so it is process-global rather
    # than per-instance. Cause unresolved; see docs/SCALING.md §8.
    #
    # Without this reset the benchmark measures the degradation instead of the
    # read path, which is how an earlier run reported a spurious 107 ms p99.
    print("\n2. feast.get_online_features()   (fresh store per batch size)")
    results = {}
    for size in (1, 10, 100):
        if size > len(users):
            continue
        store = FeatureStore(repo_path=args.repo)
        samples = []
        for i in range(args.warmup + args.iterations):
            batch = [{ENTITY_JOIN_KEY: users[(i * size + j) % len(users)]}
                     for j in range(size)]
            t0 = time.perf_counter()
            store.get_online_features(features=refs, entity_rows=batch).to_dict()
            dt = (time.perf_counter() - t0) * 1000
            if i >= args.warmup:
                samples.append(dt)
        results[size] = summarise(f"{size:>3} entities/call", samples, per_entity=size)

    print("\n   per-entity cost (p50 / batch size):")
    for size, row in results.items():
        print(f"     {size:>3} entities   {row['per_entity_p50']:6.3f} ms/entity")
    if 1 in results and 100 in results:
        speedup = results[1]["per_entity_p50"] / results[100]["per_entity_p50"]
        print(f"   batching 100 is {speedup:.0f}x cheaper per entity than one at a time")

    # --- 3. where the time goes ------------------------------------------
    if 1 in results:
        overhead = results[1]["p50"] - raw["p50"]
        share = 100 * overhead / results[1]["p50"]
        print(f"\n3. breakdown for a single-entity read (p50)")
        print(f"     Redis round trip   {raw['p50']:6.2f} ms")
        print(f"     Feast overhead     {overhead:6.2f} ms  ({share:.0f}% of the call)")
        print("     (proto decode, registry lookup, response assembly)")

    # --- 4. bytes per entity ---------------------------------------------
    print("\n4. Redis footprint per entity")
    usages = []
    for uid in users[:100]:
        u = client.memory_usage(redis_key(uid))
        if u:
            usages.append(u)
    if usages:
        mean_bytes = statistics.fmean(usages)
        print(f"     MEMORY USAGE mean  {mean_bytes:,.0f} B over {len(usages)} keys")
        print(f"     embedding alone    {EMBEDDING_DIM * 4:,} B "
              f"({EMBEDDING_DIM} x float32)")
        print(f"     -> 50M users       {mean_bytes * 50e6 / 1e9:,.0f} GB")
        print(f"     ({len(FEATURES)} features per entity)")

    # --- 5. does a long-lived store stay fast? ---------------------------
    print("\n5. read latency vs calls through ONE FeatureStore (100 entities)")
    store = FeatureStore(repo_path=args.repo)
    batch = [{ENTITY_JOIN_KEY: u} for u in users[:100]]
    trend = []
    for block in range(4):
        s2 = []
        for _ in range(60):
            t0 = time.perf_counter()
            store.get_online_features(features=refs, entity_rows=batch).to_dict()
            s2.append((time.perf_counter() - t0) * 1000)
        trend.append(pct(s2, 50))
        print(f"     after {60 * (block + 1):>4} calls   p50 {trend[-1]:6.2f} ms")
    if trend[0] > 0:
        print(f"     drift {trend[-1] / trend[0]:.1f}x over {60 * len(trend)} calls "
              f"— see docs/SCALING.md §8")

    # --- verdict ----------------------------------------------------------
    print()
    checks = []
    if 1 in results:
        checks.append((f"single-entity p99 < 10 ms ({results[1]['p99']:.2f})",
                       results[1]["p99"] < 10))
    if 100 in results:
        checks.append((f"100-entity batch p99 < 25 ms ({results[100]['p99']:.2f})",
                       results[100]["p99"] < 25))
    ok = True
    for label, passed in checks:
        print(f"  {'PASS' if passed else 'FAIL'}  {label}")
        ok &= passed
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
