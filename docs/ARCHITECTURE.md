# Architecture

## 1. Data flow

```
┌──────────────┐   JSON over TCP    ┌──────────────────────────┐
│  producer    │ ─────────────────► │  Kafka  topic: transactions
│ kafka-python │  localhost:29092   │  apache/kafka:4.3.1 KRaft │
│ Zipf users   │                    │  partitions: 6            │
│ + anomalies  │                    └────────────┬─────────────┘
│ + late events│                                 │ kafka:9092 (internal)
└──────────────┘                                 ▼
                              ┌──────────────────────────────────────────┐
                              │  Flink 2.2.1  (JobManager + TaskManager) │
                              │                                          │
                              │  1. Kafka source, JSON format            │
                              │  2. watermark: event_time - 30s          │
                              │  3. HOP window  size 10m / slide 1m      │
                              │     GROUP BY user_id                     │
                              │  4. render behaviour sentence            │
                              │  5. pandas UDF → MiniLM → ARRAY<FLOAT>   │
                              │  6. t_env.to_data_stream(table)          │
                              │  7. FeastRedisWriter (MapFunction)       │
                              └──────────────────┬───────────────────────┘
                                                 │ pipelined HSET
                                                 ▼
                              ┌──────────────────────────────────────────┐
                              │  Redis Stack 7.4                         │
                              │  one hash per (project, user_id)         │
                              │  + optional RediSearch HNSW index        │
                              └──────────────────┬───────────────────────┘
                                                 │ localhost:6379
                                                 ▼
                              ┌──────────────────────────────────────────┐
                              │  Feast 0.65  →  get_online_features()    │
                              │  "the model", p99 target < 10 ms         │
                              └──────────────────────────────────────────┘
```

Offline path (for `get_historical_features` and point-in-time-correct training sets): a seeded parquet
file registered as the feature view's batch source. At real scale this is object storage; see
[SCALING.md](SCALING.md).

## 2. Event schema — Kafka topic `transactions`

```json
{
  "event_id": "9f0c...",
  "user_id": 10432,
  "merchant_id": 88,
  "merchant_name": "Blue Bottle Coffee",
  "merchant_category": "coffee_shop",
  "amount": 12.75,
  "currency": "USD",
  "country": "US",
  "device": "ios",
  "is_card_present": false,
  "event_timestamp": "2026-07-27T12:00:00.123Z"
}
```

~350 bytes serialized. Keyed by `user_id` so a user's events land on one partition and ordering is
per-user consistent.

## 3. Feature view — `user_txn_stats_10m`

Entity: `user_id` (Int64). `ttl=30 min`.

**`ttl` does not expire anything in Redis.** In Feast 0.65, `FeatureView.ttl` bounds materialization
windows and point-in-time joins on the *offline* path only. The online read path performs no staleness
check whatsoever — a value written an hour ago comes back with status `PRESENT` exactly like a fresh one.
Online expiry is a separate knob, `online_store.key_ttl_seconds`, which defaults to `None`.

Because our sink bypasses Feast's `online_write_batch` (see [ADR-003](DECISIONS.md)), setting that yaml
key alone would do nothing: **the sink must issue `EXPIRE` itself** alongside each `HSET`. The TTL value
lives in `contracts/schema.py` so `feature_store.yaml` and the sink cannot drift apart.

| Field | Type | Derivation |
|---|---|---|
| `txn_count_10m` | Int64 | `COUNT(*)` |
| `txn_amount_sum_10m` | Float64 | `SUM(amount)` |
| `txn_amount_avg_10m` | Float64 | `AVG(amount)` — the rolling average from the brief |
| `txn_amount_max_10m` | Float64 | `MAX(amount)` |
| `txn_amount_stddev_10m` | Float64 | `STDDEV_POP(amount)` |
| `distinct_merchants_10m` | Int64 | `COUNT(DISTINCT merchant_id)` |
| `behavior_summary` | String | `LISTAGG(merchant_category)`, then deduped + sorted in Python — see §4 |
| `embedding` | Array(Float32), 384 | MiniLM over a rendered sentence |
| `window_end` | UnixTimestamp | window boundary, doubles as feature freshness |

The embedding input sentence is rendered from the aggregate, e.g.:

> `"user made 7 transactions averaging $23.40, max $91.00, across coffee_shop, grocery, rideshare"`

This gives the vector actual semantics: users with similar behaviour profiles end up close together,
which is what makes the vector useful for similarity search or as a model input.

The category list is **deduplicated and sorted** before rendering. That is not cosmetic: `LISTAGG`
concatenates in arrival order, which this pipeline deliberately perturbs (the producer injects
out-of-order events), so without normalisation two windows with identical content would render
`"coffee_shop, rideshare"` and `"rideshare, coffee_shop"` and embed to different vectors — destroying the
similarity property above and making round-trip comparisons irreproducible. The same normalised string is
what gets stored as `behavior_summary`, so the feature and the embedding input never disagree.

## 4. Windowing

`HOP(TABLE txns, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE)` — a 10-minute
window advancing every minute. Each user therefore gets a fresh feature row every minute reflecting their
last 10 minutes of activity.

The source DDL declares the rowtime column as `event_time TIMESTAMP(3)`, mapped from the JSON field
`event_timestamp`, with the `WATERMARK` clause on it. Everything downstream — the TVF above, the diagram
in §1 — refers to `event_time`; `event_timestamp` only ever names the wire field.

Three properties matter downstream:
- **Append-only output.** Windowed aggregation emits each window once, on close. That means
  `to_data_stream()` yields an append stream, and the sink can be a plain writer with no retraction
  handling. A non-windowed `GROUP BY` would produce a changelog and require the sink to process
  retract/update-before messages.
- **No `DISTINCT` inside `LISTAGG`.** `LISTAGG(DISTINCT ...)` under a HOP window **fails at plan time** on
  Flink 2.2.1 with `TableException: This function does not support retraction`. Sliced hopping windows
  force retraction-capable codegen for distinct aggregates, and `ListAggFunction` has no
  `retractExpressions`. Note the trap: `COUNT(DISTINCT ...)` in the *same query* is fine, plain `LISTAGG`
  in a HOP window is fine, and `LISTAGG(DISTINCT ...)` under `TUMBLE` is fine — only this exact
  combination breaks, so it invites being "fixed" back. Dedup happens in Python instead (§3).
- **Event-time correctness.** Watermarks are `event_time - 30s`, matching the producer's injected
  lateness. Events later than that are dropped by the windowing operator. Surfacing a count of them takes
  deliberate work — Table API has no built-in late-record metric on window TVFs — so treat "we measure
  lateness" as a task, not a property we get for free.

## 5. How the Redis layout works

Feast stores each entity as **one Redis hash**:

```python
KEY    = _redis_key(project, entity_key, entity_key_serialization_version=3)   # binary blob
#                   ↑ e.g. "petafs"
#          entity_key = EntityKeyProto(join_keys=["user_id"],
#                                      entity_values=[ValueProto(int64_val=42)])

FIELDS = {
    _mmh3("user_txn_stats_10m:txn_amount_avg_10m"): ValueProto(double_val=23.4).SerializeToString(),
    _mmh3("user_txn_stats_10m:embedding")         : ValueProto(
                                                        float_list_val=FloatList(val=[...])
                                                    ).SerializeToString(),
    ...
    "_ts:user_txn_stats_10m"                      : Timestamp().SerializeToString(),
}
```

Note the two easy-to-miss constructor shapes: `float_list_val` takes a `FloatList` *message*, not a bare
Python list, and `EntityKeyProto`'s value field is `entity_values` holding `ValueProto`s, not `values`
holding ints. Feast's `Array(Float32)` maps to `float_list_val`.

Three consequences drive the design:

1. **Field names are hashes**, so a mismatch between writer and reader is undetectable at runtime — hence
   the shared contract module and the round-trip test.
2. **One hash per entity** means a read for N features of one user is a single `HMGET`. Reading many
   features costs barely more than reading one; reading many *users* is what costs.
3. **The `_ts:` field is per-feature-view**, so freshness is tracked per view, not per feature.

## 6. Why the sink is a `MapFunction`

PyFlink has no public interface for writing a custom sink in Python, so the idiomatic route is a map
operator that writes as a side effect:

```python
ds = t_env.to_data_stream(table)              # method lives on StreamTableEnvironment, not Table
ds.map(FeastRedisWriter(), output_type=Types.STRING()) \
  .name("feast-redis-writer")
```

`FeastRedisWriter.open(runtime_context)` builds the Redis connection pool and the Feast encoder once per
task slot; `map()` buffers into a pipeline and flushes at 500 rows or 100 ms, whichever comes first.

Three PyFlink-specific details, each of which is a plausible-looking mistake in Java idiom:

- **`to_data_stream` is a `StreamTableEnvironment` method**, called as `t_env.to_data_stream(table)`.
  `Table` has no such attribute — its only conversion method is `to_pandas()`.
- **There is no `RichMapFunction` in PyFlink.** That name is Java/Scala. The Python class is
  `MapFunction`, and it is *already* rich: the base `Function` defines `open(runtime_context)` and
  `close()`, which `DataStream.map()` wires through. So `class FeastRedisWriter(MapFunction)` with an
  `open()` is the correct and complete shape.
- **No terminal sink is required.** `map()` registers its operator with the execution environment at
  creation time, so a side-effect-only branch runs on `env.execute()` with nothing attached downstream.
  (`DiscardingSink` would be the instinctive choice here and does not exist in PyFlink 2.2.1 — the
  `SinkFunction`-based one was removed in Flink 2.0 and the Sink V2 replacement was never wrapped for
  Python.)

## 7. Container topology

| Service | Image | Ports (host) | Role |
|---|---|---|---|
| `kafka` | `apache/kafka:4.3.1` | 29092 | broker + controller (KRaft) |
| `jobmanager` | custom `flink:2.2.1` + PyFlink | 8081 | Flink UI, job submission |
| `taskmanager` | same image | — | execution slots; holds the model in memory |
| `redis` | `redis/redis-stack-server:7.4.0-v8` | **6380** | online store + RediSearch |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui` | 8080 | topic inspection (dev convenience) |

(`kafbat` is the maintained fork of the abandoned `provectuslabs/kafka-ui`, whose last image predates
Kafka 4.x by two years. It sits behind a compose profile — `make up UI=1` — so it costs nothing normally.)

**Redis uses host port 6380**, not 6379, so the stack coexists with a system or Homebrew redis. This is
the same shape as the Kafka dual-listener split: only the *host* side moves. Inside the compose network
it is still `redis:6379`, so the Flink sink is unaffected — but `feature_store.yaml` and the host-side
serving scripts must say `localhost:6380`. Override with `REDIS_HOST_PORT=6379 make up` if 6379 is free.

The custom Flink image carries PyTorch, sentence-transformers, `feast[redis]`, the Kafka SQL connector
JAR, and a **pre-downloaded** copy of `all-MiniLM-L6-v2`. It runs with `HF_HUB_OFFLINE=1`: a UDF that
reaches for the network on first invocation turns a cold start into a task timeout.

Its base is `eclipse-temurin:17-jre-noble` — Ubuntu 24.04, which ships **python3.12** and has no
python3.11 package at all. The container therefore runs 3.12 while the host venv runs 3.11; only the
`apache-flink` *version* has to match the cluster, not the interpreter version, and per the conventions in
[CLAUDE.md](../CLAUDE.md) the host never runs PyFlink anyway.

The base is also JRE-only, with no compiler — which matters more than it sounds. See
[ADR-009](DECISIONS.md) for why `pip install apache-flink` needs `build-essential` and `python3.12-dev`
on this platform.
