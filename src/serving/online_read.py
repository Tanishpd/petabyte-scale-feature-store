#!/usr/bin/env python3
"""What a production model server actually contains.

The whole pipeline exists so that this file can stay this short: no Kafka, no
Flink, no knowledge that a 10-minute hopping window or a sentence-transformer
was involved. A model asks for features by name and gets them in milliseconds.

    make read
    make read USERS="1 2 3"

Runs on the host venv against `localhost:6380` (the stack's Redis; the system
one on 6379 is untouched).
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from feast import FeatureStore  # noqa: E402

from contracts.schema import ENTITY_JOIN_KEY, FEATURE_VIEW_NAME, FEATURES  # noqa: E402

REPO = ROOT / "src" / "feast_repo"


def feature_refs() -> list[str]:
    """`["user_txn_stats_10m:txn_amount_avg_10m", ...]`, from the contract —
    never hand-typed, because the Redis field key is a hash of these strings."""
    return [f"{FEATURE_VIEW_NAME}:{name}" for name in FEATURES]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, nargs="+", default=[1, 2, 3],
                    help="entity ids to fetch")
    ap.add_argument("--repo", default=str(REPO))
    args = ap.parse_args()

    # A model server builds this once at startup, not per request.
    store = FeatureStore(repo_path=args.repo)

    entity_rows = [{ENTITY_JOIN_KEY: uid} for uid in args.users]

    t0 = time.perf_counter()
    features = store.get_online_features(
        features=feature_refs(),
        entity_rows=entity_rows,
    ).to_dict()
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"get_online_features: {len(args.users)} "
          f"{'entity' if len(args.users) == 1 else 'entities'} in {elapsed_ms:.2f} ms\n")

    found = 0
    for i, uid in enumerate(features[ENTITY_JOIN_KEY]):
        avg = features["txn_amount_avg_10m"][i]
        if avg is None:
            print(f"user {uid}: no features "
                  f"(never active, or the window expired — TTL is 30 min)")
            continue
        found += 1
        emb = features["embedding"][i]
        print(f"user {uid}")
        print(f"  transactions (10m)   {features['txn_count_10m'][i]}")
        print(f"  rolling average      {avg:,.2f}")
        print(f"  max                  {features['txn_amount_max_10m'][i]:,.2f}")
        print(f"  stddev               {features['txn_amount_stddev_10m'][i]:,.2f}")
        print(f"  distinct merchants   {features['distinct_merchants_10m'][i]}")
        print(f"  behaviour            {features['behavior_summary'][i]}")
        print(f"  embedding            {len(emb)}-dim  "
              f"[{emb[0]:+.4f}, {emb[1]:+.4f}, {emb[2]:+.4f}, ...]")
        print(f"  window ended         {features['window_end'][i]}")
        print()

    if not found:
        print("Nothing found. Is the pipeline running?")
        print("  make up && make apply && make smoke")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
