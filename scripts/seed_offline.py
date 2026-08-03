#!/usr/bin/env python3
"""Seed the offline parquet that backs the FeatureView's batch source.

Feast requires every FeatureView to have a batch source. Ours exists to satisfy
that requirement and to make `get_historical_features` demonstrable — the ONLINE
path never reads it, because Flink writes Redis directly (ADR-003).

The numeric aggregates are computed the same way the Flink job computes them:
a 10-minute window advancing every minute, over events from the same generator
the producer uses. So offline and online features share one definition of
"rolling 10-minute average" rather than two that drift.

⚠️ EMBEDDINGS HERE ARE PLACEHOLDERS. Real ones need sentence-transformers, which
lives only in the Flink image — putting torch in the host venv would merge two
dependency sets that CLAUDE.md deliberately keeps apart. The placeholder is a
deterministic unit vector derived from behavior_summary, so identical behaviour
still yields an identical vector, but it carries NO semantic meaning. Do not
train anything on this column. The online path (Phase 5) computes real ones.

    python scripts/seed_offline.py --hours 2 --users 5000
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from contracts.schema import (  # noqa: E402
    EMBEDDING_DIM,
    ENTITY_JOIN_KEY,
    FEATURE_VIEW_NAME,
    WINDOW_SIZE_MINUTES,
    WINDOW_SLIDE_MINUTES,
)
from src.producer.generate_stream import EventGenerator  # noqa: E402

OUT = ROOT / "src" / "feast_repo" / "data" / f"{FEATURE_VIEW_NAME}.parquet"


def placeholder_embedding(text: str, dim: int = EMBEDDING_DIM) -> list[float]:
    """Deterministic pseudo-vector. NOT semantic — see the module docstring."""
    seed = int.from_bytes(hashlib.blake2b(text.encode(), digest_size=8).digest(), "big")
    v = np.random.default_rng(seed).normal(size=dim)
    v /= np.linalg.norm(v)
    return v.astype(np.float32).tolist()


def build(hours: float, users: int, eps: int, seed: int) -> pd.DataFrame:
    gen = EventGenerator(n_users=users, seed=seed)

    # Generate a history ending "now", long enough that the first window is full.
    now = time.time()
    span_s = hours * 3600 + WINDOW_SIZE_MINUTES * 60
    n_events = int(span_s * eps)
    print(f"generating {n_events:,} events over {span_s / 60:.0f} min ...")

    events, _, _ = gen.batch(n_events, now=now)
    df = pd.DataFrame(events)
    # Spread events across the span; the generator stamps them all near `now`.
    df["ts"] = pd.to_datetime(
        now - np.random.default_rng(seed).uniform(0, span_s, len(df)), unit="s", utc=True
    )

    size = pd.Timedelta(minutes=WINDOW_SIZE_MINUTES)
    slide = pd.Timedelta(minutes=WINDOW_SLIDE_MINUTES)

    # Minute-aligned window ends, exactly like the Flink HOP.
    last = df["ts"].max().floor("min")
    first = last - pd.Timedelta(hours=hours)
    window_ends = pd.date_range(first, last, freq=slide)
    print(f"aggregating {len(window_ends)} windows "
          f"({WINDOW_SIZE_MINUTES}m size / {WINDOW_SLIDE_MINUTES}m slide) ...")

    rows = []
    for w_end in window_ends:
        # Flink window bounds are [window_start, window_end) — a record stamped
        # exactly at window_end belongs to the NEXT window. Getting this
        # backwards puts boundary events in the wrong window, so the offline
        # and online values for the same (user, window) would disagree.
        w = df[(df["ts"] >= w_end - size) & (df["ts"] < w_end)]
        if w.empty:
            continue
        g = w.groupby("user_id")
        agg = g.agg(
            txn_count_10m=("amount", "size"),
            txn_amount_sum_10m=("amount", "sum"),
            txn_amount_avg_10m=("amount", "mean"),
            txn_amount_max_10m=("amount", "max"),
            # STDDEV_POP, matching Flink. pandas defaults to ddof=1.
            txn_amount_stddev_10m=("amount", lambda s: float(s.std(ddof=0))),
            distinct_merchants_10m=("merchant_id", "nunique"),
        ).reset_index()

        # Deduplicated AND sorted, exactly as ADR-010 requires — LISTAGG's order
        # is undefined, so the Python side is what makes this deterministic.
        summary = (
            g["merchant_category"]
            .apply(lambda s: ", ".join(sorted(set(s))))
            .rename("behavior_summary")
            .reset_index()
        )
        agg = agg.merge(summary, on="user_id")
        agg["window_end"] = int(w_end.timestamp())
        agg["event_timestamp"] = w_end
        rows.append(agg)

    out = pd.concat(rows, ignore_index=True)
    out["txn_amount_stddev_10m"] = out["txn_amount_stddev_10m"].fillna(0.0)
    out["embedding"] = out["behavior_summary"].map(placeholder_embedding)
    out["created"] = pd.Timestamp.now(tz="UTC")

    out = out.astype({
        ENTITY_JOIN_KEY: "int64",
        "txn_count_10m": "int64",
        "distinct_merchants_10m": "int64",
        "window_end": "int64",
    })
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=float, default=2.0, help="history depth")
    ap.add_argument("--users", type=int, default=5_000)
    ap.add_argument("--eps", type=int, default=40, help="synthetic events/sec of history")
    ap.add_argument("--seed", type=int, default=17)
    args = ap.parse_args()

    df = build(args.hours, args.users, args.eps, args.seed)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False, compression="zstd")

    size_mb = OUT.stat().st_size / (1 << 20)
    print(f"\nwrote {OUT.relative_to(ROOT)}")
    print(f"  rows        : {len(df):,}")
    print(f"  users       : {df[ENTITY_JOIN_KEY].nunique():,}")
    print(f"  windows     : {df['event_timestamp'].nunique():,}")
    print(f"  span        : {df['event_timestamp'].min()} .. {df['event_timestamp'].max()}")
    print(f"  size        : {size_mb:.1f} MiB (zstd)")
    print(f"  columns     : {list(df.columns)}")
    print("\n  NOTE: `embedding` is a deterministic placeholder, not a real "
          "sentence-transformer\n        vector. See this script's docstring.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
