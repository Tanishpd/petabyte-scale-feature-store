#!/usr/bin/env python3
"""Phase 4 acceptance: prove Flink's rolling aggregates are arithmetically right.

Runs INSIDE the Flink container. Reads a topic twice with a **bounded** Kafka
source (`scan.bounded.mode = latest-offset`), so both reads terminate:

  1. the windowed aggregate, exactly as streaming_job.py computes it
  2. the raw events

then recomputes (2) with pandas and compares against (1) row by row.

Using two Flink reads rather than a Kafka client keeps this self-contained —
kafka-python is not installed in the Flink image, and does not need to be.

    docker compose exec jobmanager python /opt/project/src/flink/verify_windows.py \
        --topic transactions_verify
"""

from __future__ import annotations

import argparse
import sys

sys.path.insert(0, "/opt/project")

import pandas as pd  # noqa: E402

from contracts.schema import (  # noqa: E402
    WINDOW_SIZE_MINUTES,
    WINDOW_SLIDE_MINUTES,
)
from src.flink.streaming_job import (  # noqa: E402
    SOURCE_TABLE,
    build_env,
    build_source_ddl,
    windowed_aggregate_sql,
)

TOL = 1e-6


def to_utc(x):
    """Coerce to tz-aware UTC.

    The rowtime is TIMESTAMP_LTZ(3), which arrives as datetime64[ms, Etc/UTC],
    while the window bounds arrive naive. Comparing the two raises rather than
    silently mis-bucketing, but both have to be normalised before any of the
    window filtering or the join works.
    """
    s = pd.to_datetime(x)
    if isinstance(s, pd.Series):
        return s.dt.tz_localize("UTC") if s.dt.tz is None else s.dt.tz_convert("UTC")
    return s.tz_localize("UTC") if s.tz is None else s.tz_convert("UTC")


def normalise_categories(multiset: dict | None) -> str:
    """MULTISET<STRING> -> a sorted, deduplicated string.

    COLLECT gives a {category: count} dict, so dedup is structural; sorting is
    what makes it deterministic, since dict order is not defined and this
    pipeline delivers events out of order on purpose (ADR-010).
    """
    if not multiset:
        return ""
    return ", ".join(sorted(multiset))


def pandas_hop(events: pd.DataFrame, window_ends: list[pd.Timestamp]) -> pd.DataFrame:
    """Recompute the HOP aggregation independently.

    Flink window bounds are [window_start, window_end) — a record stamped
    exactly at window_end belongs to the NEXT window, not this one.
    """
    size = pd.Timedelta(minutes=WINDOW_SIZE_MINUTES)
    out = []
    for w_end in window_ends:
        w_start = w_end - size
        w = events[(events["event_timestamp"] >= w_start)
                   & (events["event_timestamp"] < w_end)]
        if w.empty:
            continue
        g = w.groupby("user_id")
        agg = g.agg(
            txn_count_10m=("amount", "size"),
            txn_amount_sum_10m=("amount", "sum"),
            txn_amount_avg_10m=("amount", "mean"),
            txn_amount_max_10m=("amount", "max"),
            # STDDEV_POP: pandas defaults to the sample stddev (ddof=1).
            txn_amount_stddev_10m=("amount", lambda s: float(s.std(ddof=0))),
            distinct_merchants_10m=("merchant_id", "nunique"),
        ).reset_index()
        cats = (g["merchant_category"]
                .apply(lambda s: ", ".join(sorted(set(s))))
                .rename("categories").reset_index())
        agg = agg.merge(cats, on="user_id")
        agg["window_end"] = w_end
        out.append(agg)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="transactions_verify")
    ap.add_argument("--parallelism", type=int, default=1,
                    help="1 keeps the bounded read deterministic")
    args = ap.parse_args()

    print(f"reading {args.topic} (bounded) ...")

    # --- 1. Flink's aggregate ---------------------------------------------
    t_env = build_env(args.parallelism)
    t_env.execute_sql(build_source_ddl(
        args.bootstrap, args.topic, "petafs-verify-agg",
        startup_mode="earliest-offset", bounded=True,
    ))
    # collect(), not to_pandas(): Arrow cannot represent MULTISET, and
    # collectAsPandasDataFrame fails on it. Materialising rows directly avoids
    # the conversion entirely.
    agg_table = t_env.sql_query(windowed_aggregate_sql())
    cols = list(agg_table.get_schema().get_field_names())
    with agg_table.execute().collect() as it:
        flink = pd.DataFrame([dict(zip(cols, row)) for row in it])
    print(f"  flink aggregate rows : {len(flink):,}")

    # --- 2. the same events, raw ------------------------------------------
    t_env2 = build_env(args.parallelism)
    t_env2.execute_sql(build_source_ddl(
        args.bootstrap, args.topic, "petafs-verify-raw",
        startup_mode="earliest-offset", bounded=True,
    ))
    events = t_env2.sql_query(
        f"SELECT `user_id`, `merchant_id`, `merchant_category`, `amount`, "
        f"`event_timestamp` FROM {SOURCE_TABLE}"
    ).to_pandas()
    print(f"  raw events           : {len(events):,}")

    if events.empty:
        print("\nFAIL: topic is empty. Backfill it first:  make verify-seed")
        return 1

    # Diagnose the specific silent failure this pipeline is prone to, rather
    # than reporting "0 rows" and leaving the cause to guesswork.
    n_null_ts = int(pd.to_datetime(events["event_timestamp"], errors="coerce").isna().sum())
    if n_null_ts:
        print(f"\nFAIL: {n_null_ts:,}/{len(events):,} rowtimes are NULL.")
        print("  The rowtime did not parse, so the watermark never advanced and")
        print("  no window fired. Check both:")
        print("    - contracts/events.py declares TIMESTAMP_LTZ(3), not TIMESTAMP(3)")
        print("      (the producer emits a UTC instant with a trailing 'Z')")
        print("    - json.timestamp-format.standard = ISO-8601, not the SQL default")
        return 1

    if flink.empty:
        print(f"\nFAIL: {len(events):,} events read but the aggregate produced no rows,")
        print("  and rowtimes parsed fine. Suspect watermark/window config.")
        return 1

    # --- 3. independent recomputation -------------------------------------
    flink["categories"] = flink["category_counts"].map(normalise_categories)
    flink["window_end"] = to_utc(flink["window_end"])
    events["event_timestamp"] = to_utc(events["event_timestamp"])

    window_ends = sorted(flink["window_end"].unique())
    print(f"  windows                : {len(window_ends)}")

    ref = pandas_hop(events, [to_utc(pd.Timestamp(w)) for w in window_ends])
    print(f"  pandas aggregate rows  : {len(ref):,}")

    # --- 4. compare --------------------------------------------------------
    key = ["user_id", "window_end"]
    ref["window_end"] = to_utc(ref["window_end"])
    merged = flink.merge(ref, on=key, how="outer",
                         suffixes=("_flink", "_pandas"), indicator=True)

    only_flink = (merged["_merge"] == "left_only").sum()
    only_pandas = (merged["_merge"] == "right_only").sum()
    both = (merged["_merge"] == "both").sum()
    print(f"\n  matched keys : {both:,}")
    print(f"  flink only   : {only_flink:,}")
    print(f"  pandas only  : {only_pandas:,}")

    failures: list[str] = []
    if only_flink or only_pandas:
        failures.append(f"key mismatch: {only_flink} flink-only, {only_pandas} pandas-only")

    b = merged[merged["_merge"] == "both"]
    numeric = [
        "txn_count_10m", "txn_amount_sum_10m", "txn_amount_avg_10m",
        "txn_amount_max_10m", "txn_amount_stddev_10m", "distinct_merchants_10m",
    ]
    print("\n  column                      max abs diff")
    for col in numeric:
        diff = (b[f"{col}_flink"].astype(float) - b[f"{col}_pandas"].astype(float)).abs()
        worst = float(diff.max()) if len(diff) else 0.0
        status = "ok" if worst <= TOL else "FAIL"
        print(f"  {col:<26} {worst:.3e}  {status}")
        if worst > TOL:
            failures.append(f"{col}: max abs diff {worst:.3e} > {TOL:.0e}")

    cat_mismatch = (b["categories_flink"] != b["categories_pandas"]).sum()
    print(f"  {'behavior_summary':<26} {cat_mismatch} mismatched  "
          f"{'ok' if cat_mismatch == 0 else 'FAIL'}")
    if cat_mismatch:
        failures.append(f"behavior_summary: {cat_mismatch} rows differ")

    if failures:
        print("\nFAIL")
        for f in failures:
            print(f"  - {f}")
        return 1

    print(f"\nPASS — Flink matches an independent pandas recomputation "
          f"across {both:,} (user, window) pairs within {TOL:.0e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
