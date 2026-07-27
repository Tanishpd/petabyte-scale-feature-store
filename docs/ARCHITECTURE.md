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
                              │  6. to_data_stream()                     │
                              │  7. FeastRedisWriter (RichMapFunction)   │
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

Entity: `user_id` (Int64). TTL: 30 minutes.

| Field | Type | Derivation |
|---|---|---|
| `txn_count_10m` | Int64 | `COUNT(*)` |
| `txn_amount_sum_10m` | Float64 | `SUM(amount)` |
| `txn_amount_avg_10m` | Float64 | `AVG(amount)` — the rolling average from the brief |
| `txn_amount_max_10m` | Float64 | `MAX(amount)` |
| `txn_amount_stddev_10m` | Float64 | `STDDEV_POP(amount)` |
| `distinct_merchants_10m` | Int64 | `COUNT(DISTINCT merchant_id)` |
| `behavior_summary` | String | `LISTAGG(DISTINCT merchant_category)` |
| `embedding` | Array(Float32), 384 | MiniLM over a rendered sentence |
| `window_end` | UnixTimestamp | window boundary, doubles as feature freshness |

The embedding input sentence is rendered from the aggregate, e.g.:

> `"user made 7 transactions averaging $23.40, max $91.00, across coffee_shop, rideshare, grocery on ios in US"`

This gives the vector actual semantics: users with similar behaviour profiles end up close together,
which is what makes the vector useful for similarity search or as a model input.

## 4. Windowing

`HOP(TABLE txns, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE)` — a 10-minute
window advancing every minute. Each user therefore gets a fresh feature row every minute reflecting their
last 10 minutes of activity.

Two properties matter downstream:
- **Append-only output.** Windowed aggregation emits each window once, on close. That means
  `to_data_stream()` yields an append stream, and the sink can be a plain writer with no retraction
  handling. A non-windowed `GROUP BY` would produce a changelog and require the sink to process
  retract/update-before messages.
- **Event-time correctness.** Watermarks are `event_timestamp - 30s`, matching the producer's injected
  lateness. Events later than that are dropped and counted — the count is surfaced as a metric rather
  than silently ignored.

## 5. How the Redis layout works

Feast stores each entity as **one Redis hash**:

```
KEY    = _redis_key(project, entity_key, entity_key_serialization_version=3)   # binary blob
                    ↑ e.g. "petafs"      ↑ EntityKeyProto{join_keys:["user_id"], values:[42]}

FIELDS = {
    mmh3("user_txn_stats_10m:txn_amount_avg_10m")  : ValueProto(double_val=23.4).SerializeToString(),
    mmh3("user_txn_stats_10m:embedding")           : ValueProto(float_list_val=[...]).SerializeToString(),
    ...
    "_ts:user_txn_stats_10m"                        : Timestamp.SerializeToString(),
}
```

Three consequences drive the design:

1. **Field names are hashes**, so a mismatch between writer and reader is undetectable at runtime — hence
   the shared contract module and the round-trip test.
2. **One hash per entity** means a read for N features of one user is a single `HMGET`. Reading many
   features costs barely more than reading one; reading many *users* is what costs.
3. **The `_ts:` field is per-feature-view**, so freshness is tracked per view, not per feature.

## 6. Why the sink is a `RichMapFunction`

PyFlink has no public interface for writing a custom sink in Python. The idiomatic pattern is:

```python
ds = table.to_data_stream()
ds.map(FeastRedisWriter(), output_type=Types.STRING()) \
  .name("feast-redis-writer") \
  .sink_to(DiscardingSink())
```

`FeastRedisWriter.open()` builds the Redis connection pool and the Feast encoder once per task slot;
`map()` buffers into a pipeline and flushes at 500 rows or 100 ms, whichever comes first. The
`DiscardingSink` exists only because a `map` must terminate in a sink.

## 7. Container topology

| Service | Image | Ports (host) | Role |
|---|---|---|---|
| `kafka` | `apache/kafka:4.3.1` | 29092 | broker + controller (KRaft) |
| `jobmanager` | custom `flink:2.2.1` + PyFlink | 8081 | Flink UI, job submission |
| `taskmanager` | same image | — | execution slots; holds the model in memory |
| `redis` | `redis/redis-stack-server:7.4.0-v8` | 6379, 8001 | online store + RediSearch |
| `kafka-ui` | `provectuslabs/kafka-ui` | 8080 | topic inspection (dev convenience) |

The custom Flink image carries PyTorch, sentence-transformers, `feast[redis]`, the Kafka SQL connector
JAR, and a **pre-downloaded** copy of `all-MiniLM-L6-v2`. It runs with `HF_HUB_OFFLINE=1`: a UDF that
reaches for the network on first invocation turns a cold start into a task timeout.
