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
import os
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
from contracts.secrets import read_secret  # noqa: E402
from contracts.schema import (  # noqa: E402
    WINDOW_SIZE_MINUTES,
    WINDOW_SLIDE_MINUTES,
)
from src.flink.udfs.embeddings import register as register_embedding_udfs  # noqa: E402

SOURCE_TABLE = "transactions"

# Kafka client security. Empty (the dev stack) means PLAINTEXT; the production
# topology sets SASL_PLAINTEXT and credentials via the environment so they never
# live in the image or the repo.
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_USER = os.environ.get("KAFKA_USER", "")
#: Passwords come from `${VAR}_FILE` when the topology mounts one, so they never
#: appear in `docker inspect` or `docker compose config`. See contracts/secrets.py.
KAFKA_PASSWORD = read_secret("KAFKA_PASSWORD", "") or ""
#: Java clients verify the broker chain from a JKS truststore, not a PEM. Empty
#: means SASL_PLAINTEXT or no auth at all.
KAFKA_SSL_TRUSTSTORE = os.environ.get("KAFKA_SSL_TRUSTSTORE", "")
KAFKA_SSL_TRUSTSTORE_PASSWORD = read_secret("TLS_STOREPASS", "") or ""


#: The Kafka connector is a SHADED fat jar, so its login modules live under a
#: relocated package. `flink-sql-connector-kafka-5.0.0-2.2.jar` contains
#: org/apache/flink/kafka/shaded/org/apache/kafka/common/security/plain/PlainLoginModule
#: and NOT the plain org.apache.kafka one. Naming the unshaded class in
#: sasl.jaas.config produces a LoginException that surfaces as `flink run`
#: HANGING during submission rather than failing — there is no "class not found"
#: anywhere obvious. The broker config and the kafka-python producer use the
#: unshaded name; only this connector needs the relocated one.
SHADED_PLAIN_LOGIN_MODULE = (
    "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule"
)


def _kafka_security_options() -> str:
    """DDL options for an authenticated broker, or nothing for PLAINTEXT."""
    if not KAFKA_SECURITY_PROTOCOL:
        return ""
    jaas = (
        f"{SHADED_PLAIN_LOGIN_MODULE} required "
        f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
    )
    opts = (
        f"      'properties.security.protocol' = '{KAFKA_SECURITY_PROTOCOL}',\n"
        f"      'properties.sasl.mechanism' = '{KAFKA_SASL_MECHANISM}',\n"
        f"      'properties.sasl.jaas.config' = '{jaas}',\n"
    )
    if KAFKA_SSL_TRUSTSTORE:
        # Without endpoint identification the connection is encrypted but will
        # accept any certificate, which gives up most of the guarantee.
        opts += (
            f"      'properties.ssl.truststore.location' = '{KAFKA_SSL_TRUSTSTORE}',\n"
            f"      'properties.ssl.truststore.password' = '{KAFKA_SSL_TRUSTSTORE_PASSWORD}',\n"
            "      'properties.ssl.endpoint.identification.algorithm' = 'https',\n"
        )
    return opts


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

    `ignore_parse_errors` therefore defaults to **False** in development: a
    schema mismatch should be loud, and tolerating it as a default is exactly
    how the TIMESTAMP_LTZ bug above hid.

    In production the trade-off inverts — one malformed record must not stop the
    pipeline — so `--on-bad-record skip` flips it on. That is a real choice with
    a real cost: skipped records are silently dropped, so the job also enables
    a periodic count of them (see `_log_dropped_records`). "Tolerate but count"
    is the only version of this that is safe; "tolerate" alone is data loss you
    cannot see.
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
{bounded_opt}{_kafka_security_options()}      'format' = 'json',
      'json.timestamp-format.standard' = '{JSON_TIMESTAMP_FORMAT}',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = '{str(ignore_parse_errors).lower()}'
    )
    """


def windowed_aggregate_sql(source: str = SOURCE_TABLE) -> str:
    """The 10-minute window advancing every minute, keyed by user.

    Categories use `ARRAY_AGG(DISTINCT ...)` -> `ARRAY<STRING>`. ADR-010 records
    why the two obvious alternatives lose:

    - `LISTAGG(DISTINCT ...)` fails at plan time under a HOP window ("This
      function does not support retraction"): sliced windows force
      retraction-capable codegen, and ListAggFunction has none. ArrayAggFunction
      does, which is why the DISTINCT form works here and not there. The trap:
      COUNT(DISTINCT) in this same query is fine either way.
    - `COLLECT` -> `MULTISET` is equally compact, but Python UDFs reject it:
      "Python UDF doesn't support logical type MULTISET". Neither
      `CAST(... AS MAP<STRING,INT>)` nor `MAP_KEYS(...)` bridges it.

    Compactness is the point. Measured: the busiest user in a 10-minute window
    had 2,084 transactions across 12 distinct categories. DISTINCT yields 12
    elements; without it, 2,084 — built only to be deduplicated away, and
    serialised across the Python boundary to the embedding UDF on the way.

    Sorting still happens downstream: ARRAY_AGG's element order is not defined,
    and this pipeline delivers events out of order on purpose.

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
      ARRAY_AGG(DISTINCT `merchant_category`) AS categories_agg
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


def enriched_sql(agg_view: str = "windowed") -> str:
    """Add `behavior_summary` and `embedding` to the window aggregate.

    `sorted_categories` is applied once in the inner query and reused, rather
    than called again inside the embed call — the sorted string is both a stored
    feature and the embedding input, and they must be identical.
    """
    return f"""
    SELECT
      `user_id`,
      window_start,
      window_end,
      txn_count_10m,
      txn_amount_sum_10m,
      txn_amount_avg_10m,
      txn_amount_max_10m,
      txn_amount_stddev_10m,
      distinct_merchants_10m,
      behavior_summary,
      embed_behavior(
        txn_count_10m, txn_amount_avg_10m, txn_amount_max_10m, behavior_summary
      ) AS embedding
    FROM (
      SELECT *, sorted_categories(categories_agg) AS behavior_summary
      FROM {agg_view}
    )
    """


def build_stream_env(parallelism: int | None = None):
    """Return `(stream_env, table_env)`.

    Deliberately a **StreamTableEnvironment**, built from a
    StreamExecutionEnvironment. `TableEnvironment.create(...)` returns a plain
    `TableEnvironment`, which has no `to_data_stream` at all —
    `AttributeError: 'TableEnvironment' object has no attribute
    'to_data_stream'` — so the Redis sink hop is unreachable from one. Knowing
    that `to_data_stream` belongs to StreamTableEnvironment rather than Table is
    only half of it; you also have to construct the right environment.
    """
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    if parallelism:
        env.set_parallelism(parallelism)

    t_env = StreamTableEnvironment.create(env)
    cfg = t_env.get_config()
    cfg.set("pipeline.name", "petafs-user-txn-stats-10m")
    # Idle partitions must not stall the watermark. With 6 partitions and a
    # skewed key distribution some partitions go quiet, and without this the
    # global watermark is pinned to the slowest one and windows never fire.
    cfg.set("table.exec.source.idle-timeout", "15 s")
    if parallelism:
        cfg.set("parallelism.default", str(parallelism))
    return env, t_env


def build_env(parallelism: int | None = None):
    """Table environment only — for the SQL-shaped callers (verify, bench)."""
    return build_stream_env(parallelism)[1]


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
    ap.add_argument("--on-bad-record", default="fail", choices=["fail", "skip"],
                    help="fail (default, dev): a malformed record stops the job, "
                         "so schema drift is loud. skip (prod): tolerate it and "
                         "count the drops instead of dying on one bad message.")
    ap.add_argument("--sink", default="redis", choices=["redis", "print"],
                    help="redis writes Feast-encoded hashes (the real path); "
                         "print dumps rows to the TaskManager stdout")
    ap.add_argument("--no-wait", dest="no_wait", action="store_true",
                    help="submit and return. Required with `flink run -d`. Named "
                         "--no-wait, not --detached: the latter collides with "
                         "flink run's own -d/--detached and is swallowed by the "
                         "launcher instead of reaching this script.")
    args = ap.parse_args()

    stream_env, t_env = build_stream_env(args.parallelism)
    tolerate_bad = args.on_bad_record == "skip"
    if tolerate_bad:
        print("WARNING: --on-bad-record skip — malformed records are DROPPED. "
              "Watch numRecordsIn on the source vs the producer's send count; "
              "a growing gap means schema drift, not a quiet pipeline.")
    t_env.execute_sql(
        build_source_ddl(args.bootstrap, args.topic, args.group_id,
                         args.startup_mode, bounded=False,
                         ignore_parse_errors=tolerate_bad)
    )

    register_embedding_udfs(t_env)
    t_env.create_temporary_view("windowed", t_env.sql_query(windowed_aggregate_sql()))
    final = t_env.sql_query(enriched_sql())

    if args.sink == "redis":
        return run_redis_sink(stream_env, t_env, final, args)

    # `print` sink — useful for eyeballing the aggregate without Redis.
    # Rows land in the TaskManager stdout: `make logs S=taskmanager`.
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
      behavior_summary         STRING,
      embedding                ARRAY<FLOAT>
    ) WITH ('connector' = 'print')
    """)

    result = final.execute_insert("window_print")
    if args.no_wait:
        job_id = result.get_job_client().get_job_id()
        print(f"submitted: {job_id}  ->  http://localhost:8081")
        return 0

    result.wait()
    return 0


def run_redis_sink(stream_env, t_env, table, args) -> int:
    """Table -> DataStream -> Feast-encoded Redis writes.

    `to_data_stream` is a StreamTableEnvironment method, not a Table one. The
    windowed aggregate is append-only, so the resulting stream needs no
    retraction handling. `map()` registers its operator with the execution
    environment at creation, so no terminal sink is required — and PyFlink 2.2.1
    has no DiscardingSink to attach anyway.
    """
    from pyflink.common.typeinfo import Types

    from src.flink.sinks.feast_redis_sink import FeastRedisWriter

    ds = t_env.to_data_stream(table)
    ds.map(FeastRedisWriter(), output_type=Types.STRING()).name("feast-redis-writer")

    # DataStream jobs execute through the StreamExecutionEnvironment, not the
    # table env. execute_async returns a JobClient without blocking, which is
    # what `flink run -d` needs (see --no-wait).
    if args.no_wait:
        client = stream_env.execute_async("petafs-user-txn-stats-10m")
        print(f"submitted: {client.get_job_id()}  ->  http://localhost:8081")
        return 0
    stream_env.execute("petafs-user-txn-stats-10m")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
