#!/usr/bin/env python3
"""PyFlink job: Kafka -> event-time HOP window -> per-user rolling aggregates.

Phase 4 scope: source, watermarks, windowed aggregation, print sink.
Phase 5 adds the embedding UDF; Phase 6 replaces the sink with the Feast/Redis
writer. The aggregation defined here is the one both of those build on.

Runs INSIDE the Flink container — never from the host venv (CLAUDE.md
Conventions). Submitted with `make submit`:

    flink run -py /opt/project/src/flink/streaming_job.py

Feature names, window geometry and the event shape all come from contracts/,
so this file names nothing twice.
"""

from __future__ import annotations

import argparse
import sys

# The compose file mounts ../src and ../contracts under /opt/project.
sys.path.insert(0, "/opt/project")

from pyflink.table import EnvironmentSettings, TableEnvironment  # noqa: E402

from contracts.events import (  # noqa: E402
    JSON_TIMESTAMP_FORMAT,
    ROWTIME_COLUMN,
    TOPIC,
    source_ddl_columns,
    watermark_clause,
)
from contracts.schema import (  # noqa: E402
    WINDOW_SIZE_MINUTES,
    WINDOW_SLIDE_MINUTES,
)

SOURCE_TABLE = "transactions"


def build_source_ddl(
    bootstrap: str,
    topic: str,
    group_id: str,
    startup_mode: str,
    bounded: bool,
    ignore_parse_errors: bool = False,
) -> str:
    """Kafka source DDL.

    Two settings here are load-bearing, and both fail silently when wrong:

    - `json.timestamp-format.standard = ISO-8601`. The default is the SQL
      standard (`2026-08-03 06:29:02.937`), which cannot parse the producer's
      `2026-08-03T06:29:02.937Z`.
    - The rowtime column is `TIMESTAMP_LTZ(3)` (from contracts/events.py).
      `TIMESTAMP(3)` is a timestamp *without* zone and rejects the trailing `Z`.

    Either mistake yields NULL rowtimes, a watermark that never advances, and
    zero output rows with no error anywhere.

    `ignore_parse_errors` therefore defaults to **False**. Tolerating malformed
    records is the right production posture, but as a default it converts a
    schema mismatch into silent data loss — which is exactly how the
    TIMESTAMP_LTZ bug above hid.
    """
    bounded_opt = (
        "  'scan.bounded.mode' = 'latest-offset',\n" if bounded else ""
    )
    return f"""
    CREATE TABLE {SOURCE_TABLE} (
{source_ddl_columns()},
{watermark_clause()}
    ) WITH (
      'connector' = 'kafka',
      'topic' = '{topic}',
      'properties.bootstrap.servers' = '{bootstrap}',
      'properties.group.id' = '{group_id}',
      'scan.startup.mode' = '{startup_mode}',
{bounded_opt}      'format' = 'json',
      'json.timestamp-format.standard' = '{JSON_TIMESTAMP_FORMAT}',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = '{str(ignore_parse_errors).lower()}'
    )
    """


def windowed_aggregate_sql(source: str = SOURCE_TABLE) -> str:
    """The 10-minute window advancing every minute, keyed by user.

    Categories come back as `COLLECT(...)` -> `MULTISET<STRING>`, which PyFlink
    hands over as a `{{category: count}}` dict.

    ADR-010: `LISTAGG(DISTINCT ...)` fails at plan time under a HOP window
    ("This function does not support retraction") — sliced windows force
    retraction-capable codegen and ListAggFunction has none. Note the trap:
    COUNT(DISTINCT) in this same query is fine.

    Plain LISTAGG works but concatenates one entry per *transaction*: measured,
    the busiest user in a 10-minute window had 5,293 transactions across 12
    distinct categories, i.e. a ~50 KB string built only to be deduplicated
    away. COLLECT keeps a bounded multiset instead — 12 entries — and carries
    per-category counts for free. Keys still need sorting downstream, since
    neither LISTAGG's order nor a dict's is defined.

    Output is append-only: a windowed aggregate emits each window once, on
    close. That is what lets Phase 6 attach a plain writer with no retraction
    handling.
    """
    return f"""
    SELECT
      `user_id`,
      window_start,
      window_end,
      COUNT(*)                        AS txn_count_10m,
      SUM(`amount`)                   AS txn_amount_sum_10m,
      AVG(`amount`)                   AS txn_amount_avg_10m,
      MAX(`amount`)                   AS txn_amount_max_10m,
      STDDEV_POP(`amount`)            AS txn_amount_stddev_10m,
      COUNT(DISTINCT `merchant_id`)   AS distinct_merchants_10m,
      COLLECT(`merchant_category`)    AS category_counts
    FROM TABLE(
      HOP(
        TABLE {source},
        DESCRIPTOR(`{ROWTIME_COLUMN}`),
        INTERVAL '{WINDOW_SLIDE_MINUTES}' MINUTE,
        INTERVAL '{WINDOW_SIZE_MINUTES}' MINUTE
      )
    )
    GROUP BY `user_id`, window_start, window_end
    """


def build_env(parallelism: int | None = None) -> TableEnvironment:
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    cfg = t_env.get_config()
    cfg.set("pipeline.name", "petafs-user-txn-stats-10m")
    # Idle partitions must not stall the watermark. With 6 partitions and a
    # skewed key distribution some partitions go quiet, and without this the
    # global watermark is pinned to the slowest one and windows never fire.
    cfg.set("table.exec.source.idle-timeout", "15 s")
    if parallelism:
        cfg.set("parallelism.default", str(parallelism))
    return t_env


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="kafka:9092",
                    help="Invariant 4: inside the network it is kafka:9092")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--group-id", default="petafs-flink")
    ap.add_argument("--startup-mode", default="latest-offset",
                    choices=["earliest-offset", "latest-offset", "group-offsets"])
    ap.add_argument("--parallelism", type=int, default=None)
    ap.add_argument("--no-wait", dest="no_wait", action="store_true",
                    help="submit and return. Required with `flink run -d`. Named "
                         "--no-wait, not --detached: the latter collides with "
                         "flink run's own -d/--detached and is swallowed by the "
                         "launcher instead of reaching this script.")
    args = ap.parse_args()

    t_env = build_env(args.parallelism)
    t_env.execute_sql(
        build_source_ddl(args.bootstrap, args.topic, args.group_id,
                         args.startup_mode, bounded=False)
    )

    agg = t_env.sql_query(windowed_aggregate_sql())

    # Phase 4 sink. Rows land in the TaskManager stdout: `make logs S=taskmanager`.
    t_env.execute_sql("""
    CREATE TABLE window_print (
      `user_id`                BIGINT,
      window_start             TIMESTAMP(3),
      window_end               TIMESTAMP(3),
      txn_count_10m            BIGINT,
      txn_amount_sum_10m       DOUBLE,
      txn_amount_avg_10m       DOUBLE,
      txn_amount_max_10m       DOUBLE,
      txn_amount_stddev_10m    DOUBLE,
      distinct_merchants_10m   BIGINT,
      category_counts          MULTISET<STRING>
    ) WITH ('connector' = 'print')
    """)

    result = agg.execute_insert("window_print")
    if args.no_wait:
        job_id = result.get_job_client().get_job_id()
        print(f"submitted: {job_id}  ->  http://localhost:8081")
        return 0

    result.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
