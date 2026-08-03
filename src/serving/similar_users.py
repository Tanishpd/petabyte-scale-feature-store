#!/usr/bin/env python3
"""Find behaviourally similar users by nearest-neighbour search on the embedding.

This is the part Feast cannot do for us. Feast 0.65's Redis online store has no
vector search (ADR-004), so the pipeline maintains a parallel RediSearch HNSW
index and this script queries it directly.

Two retrieval paths, deliberately shown side by side:

    "give me user 42's features"   -> feast.get_online_features()  (the norm)
    "give me users like user 42"   -> FT.SEARCH KNN                (this file)

    make similar
    make similar USER=17 K=5
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from contracts.schema import ENTITY_JOIN_KEY  # noqa: E402
from src.flink.sinks.ann_index import INDEX_NAME, knn, unpack, vec_key  # noqa: E402

REDIS_PORT = int(os.environ.get("REDIS_HOST_PORT", "6380"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", type=int, default=1, help="query entity")
    ap.add_argument("--k", type=int, default=5)
    args = ap.parse_args()

    import redis

    client = redis.Redis(host="localhost", port=REDIS_PORT, socket_connect_timeout=3)
    try:
        info = client.execute_command("FT.INFO", INDEX_NAME)
    except Exception:
        print(f"No index `{INDEX_NAME}`. Run the pipeline first: make smoke")
        return 1

    indexed = dict(zip(
        [x.decode() if isinstance(x, bytes) else x for x in info[::2]],
        info[1::2],
    )).get("num_docs", b"0")
    print(f"index `{INDEX_NAME}`: {int(indexed):,} vectors\n")

    blob = client.hget(vec_key(args.user), "embedding")
    if blob is None:
        print(f"user {args.user} has no embedding "
              f"(never active, or the 30-min TTL expired)")
        return 1

    vector = unpack(blob)
    me = client.hget(vec_key(args.user), "behavior_summary").decode()

    t0 = time.perf_counter()
    hits = knn(client, vector, k=args.k, exclude_user=args.user)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"query user {args.user}")
    print(f"  {me}\n")
    print(f"{args.k} nearest, by cosine distance on the {len(vector)}-dim embedding "
          f"({elapsed_ms:.2f} ms):\n")
    for uid, dist, summary in hits:
        print(f"  user {uid:<8} distance {dist:.4f}")
        print(f"    {summary}")
    print(f"\n(Distance is on behaviour, not spend: the sentence embeds counts, "
          f"averages\n and category mix — see docs/ARCHITECTURE.md §3.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
