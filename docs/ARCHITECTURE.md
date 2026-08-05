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
                              │  2. watermark: event_timestamp - 30s     │
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
                                                 │ localhost:6380
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

**258 bytes serialized** (measured over 545k events; compact JSON, no whitespace). Keyed by `user_id` so
a user's events land on one partition and ordering is per-user consistent.

`country`/`currency` follow the **user**, not the event — each user has one home market, assigned once.
Drawing per event would let a single user transact in USD then BRL, making `txn_amount_avg_10m` an
average over mixed currencies. Amounts are FX-scaled so a coffee reads as ~R$15 rather than ~R$4. Per-user
aggregates are therefore coherent in the user's own currency; a production feature would additionally
carry a USD-normalised amount so cross-user comparisons work.

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
| `behavior_summary` | String | `ARRAY_AGG(DISTINCT merchant_category)`, sorted in Python — see §4 |
| `embedding` | Array(Float32), 384 | MiniLM over a rendered sentence |
| `window_end` | UnixTimestamp | window boundary, doubles as feature freshness |

The embedding input sentence is rendered from the aggregate, e.g.:

> `"user made 7 transactions averaging 23.40, max 91.00, across coffee_shop, grocery, rideshare"`

No currency symbol: amounts are in each user's local currency (§2), so a "$" would be wrong for most
rows, and the signal worth embedding is the spending *pattern* rather than the unit.

This gives the vector actual semantics: users with similar behaviour profiles end up close together,
which is what makes the vector useful for similarity search or as a model input.

The category list is **sorted** before rendering. That is not cosmetic: `ARRAY_AGG`'s element order is
undefined, and this pipeline deliberately perturbs arrival order (the producer injects out-of-order
events), so without normalisation two windows with identical content would render
`"coffee_shop, rideshare"` and `"rideshare, coffee_shop"` and embed to different vectors — destroying the
similarity property above and making round-trip comparisons irreproducible. The same normalised string is
what gets stored as `behavior_summary`, so the feature and the embedding input never disagree.

## 4. Windowing

`HOP(TABLE txns, DESCRIPTOR(event_timestamp), INTERVAL '1' MINUTE, INTERVAL '10' MINUTE)` — a 10-minute
window advancing every minute. Each user therefore gets a fresh feature row every minute reflecting their
last 10 minutes of activity.

The rowtime column is `event_timestamp TIMESTAMP_LTZ(3)` — one name end to end, since Flink's JSON format
matches columns to JSON keys by name and has no field-mapping option.

**`TIMESTAMP_LTZ(3)`, not `TIMESTAMP(3)`**, and this is not cosmetic. The producer emits a UTC instant
with an explicit `Z` (`2026-08-03T06:30:13.695Z`). `TIMESTAMP(3)` is a timestamp *without* time zone, and
Flink's ISO-8601 JSON parser rejects the trailing `Z` for it. Combined with `json.ignore-parse-errors`,
that rejection becomes a NULL rather than an error: the watermark never advances, no window ever fires,
and the job reports success while emitting nothing. Measured during Phase 4 — all 48,000 events parsed to
`NaT` before the type was corrected. The source therefore defaults to
`json.ignore-parse-errors = false`, so a schema mismatch is loud instead of silent.

Three properties matter downstream:
- **Append-only output.** Windowed aggregation emits each window once, on close. That means
  `to_data_stream()` yields an append stream, and the sink can be a plain writer with no retraction
  handling. A non-windowed `GROUP BY` would produce a changelog and require the sink to process
  retract/update-before messages.
- **Categories use `ARRAY_AGG(DISTINCT ...)`.** `LISTAGG(DISTINCT ...)` under a HOP window **fails at plan
  time** on Flink 2.2.1 with `TableException: This function does not support retraction` — sliced hopping
  windows force retraction-capable codegen for distinct aggregates, and `ListAggFunction` has no
  `retractExpressions`. Note the trap: `COUNT(DISTINCT ...)` in the *same query* is fine, plain `LISTAGG`
  in a HOP window is fine, and `LISTAGG(DISTINCT ...)` under `TUMBLE` is fine — only this exact
  combination breaks, so it invites being "fixed" back.
  `ARRAY_AGG(DISTINCT ...)` *does* work under HOP — `ArrayAggFunction` implements retraction where
  `ListAggFunction` does not — and returns 12 elements rather than the 2,084 plain `LISTAGG` would emit
  for the busiest measured user. `COLLECT` is equally compact but Python UDFs reject `MULTISET`
  outright. Full comparison in [ADR-010](DECISIONS.md).
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
  `Table` has no such attribute — its only conversion method is `to_pandas()`. Knowing that is only half
  of it: `TableEnvironment.create(...)` returns a *plain* `TableEnvironment` which has no
  `to_data_stream` either, so the environment must be built as
  `StreamTableEnvironment.create(StreamExecutionEnvironment.get_execution_environment())`.
- **A DataStream job executes through the stream environment**, not the table one:
  `stream_env.execute_async(name)` (or `.execute(name)` to block). `t_env.execute(...)` will not run it.
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

## 8. Observability

None of this runs in the dev stack. `docker-compose.yml` deliberately carries no
metrics stack — the point of the laptop stack is to start fast — so everything
below appears only in the topologies that claim to be production-shaped.

| Service | Image | Port (host) | Role | prod | multihost | multihost-ha |
|---|---|---|---|---|---|---|
| `prometheus` | `prom/prometheus:v3.2.1` | 9090 | scrape + rule evaluation | ✓ | opt-in | ✓ |
| `alertmanager` | `prom/alertmanager:v0.28.0` | 9093 | grouping, inhibition, delivery | ✓ | opt-in | ✓ |
| `alert-sink` | `python:3.12-slim` | 9099 | webhook receiver, so delivery is assertable | ✓ | opt-in | ✓ |
| `grafana` | `grafana/grafana:11.6.0` | 3000 | dashboards | ✓ | — | — |

The HA topology used to be the glaring hole here: the most production-like
setup in the repo — three nodes, ZooKeeper, a standby JobManager, distributed
object storage, per-component authorization — was the one with nothing
collecting metrics at all. Its Flink processes had always run the Prometheus
reporter, so the endpoints were exposed and scraped by no one; the drills proved
the topology recovered, and nothing would have told you it had needed to. It now
runs the same three services as prod, against the same `alerts.yml`, verified
by the same `make alerts-verify` (9/9).

`multihost` carries all three too, but **behind an `obs` profile** — off unless
you ask for it:

```bash
docker compose -f docker/docker-compose.multihost.yml --env-file docker/multihost/sim.env \
  -p petafs-a --profile a --profile obs up -d
```

That topology exists to demonstrate cross-node addressing and node-loss
survival, and its drills assert both without any monitoring; the HA file is the
one you would actually deploy. Keeping it opt-in is a scope decision rather than
a memory one — the three services together measure **81 MiB** (Prometheus 31,
Alertmanager 27, sink 22), not the "~200 MB" the original comment claimed.

**In `multihost`, losing node A takes the monitoring with it.** Node A holds the
JobManager, the object store *and* the metrics stack, because that topology has
no Flink-free node to put them on — so the one node whose loss stops the
pipeline is also the one whose loss blinds you to it. The HA file does not have
this problem, and it is one more reason it is the deployable one.

**Monitoring lives on node A, which runs no Flink.** A JobManager and the
scraper watching it dying together is the blind spot that makes an outage look
like silence, and the drills kill node B or C — so the monitoring stays up to
observe the failover it is meant to record. The cost is measured rather than
assumed: three extra containers take the topology from 18 to 21 and the VM from
~3.9 to 4.5 GiB used, leaving ~3.2 GiB free. Retention is 2 h here rather than
prod's 6 h, because this topology already fills the box (see the sizing note at
the top of the compose file).

### What is scraped

Flink's Prometheus reporter exposes **one endpoint per process** on `:9250`,
which is why there are two scrape jobs rather than one: the JobManager carries
job-level series (checkpoints, restarts) and the TaskManagers carry throughput
and backpressure. An alert written against the wrong one silently matches
nothing — see below for why that failure is easy to ship.

Retention is 6 h. This is a drill rig, not a monitoring system: long enough to
see a failover and its recovery in the same graph, short enough not to compete
with Kafka for disk on an 8 GiB VM.

### The alerts

Six rules, in `docker/observability/alerts.yml`. Five describe the pipeline; the
sixth describes the alerting stack itself.

| Alert | Fires when | Why it is in the set |
|---|---|---|
| `KafkaConsumerLagGrowing` | event-time lag > 2 min for 5 m | the measured ceiling is ~8,500 eps ([SCALING §5](SCALING.md)); sustained lag means the embedding tier is saturated |
| `CheckpointsFailing` | > 2 failures in 15 m | without checkpoints a restart replays from source and loses window state ([ADR-011](DECISIONS.md)) |
| `JobRestarting` | > 3 restarts in 10 m | flapping rather than running; threshold set above one ordinary recovery, which the drills cause deliberately |
| `SustainedBackpressure` | mean backpressure > 500 ms/s for 10 m | averaged on purpose — one hot operator is normal here, the embedding UDF is the expensive stage |
| `NoFeaturesWritten` | no rows written for 15 m | **the failure this design fears most**: the job is up, Feast still answers `PRESENT`, and the values get staler every second |
| `Watchdog` | always | the dead man's switch — see below |

### Two things worth understanding

**`NoFeaturesWritten` needs `or absent(...)`, and this is not a detail.** The
obvious expression is `sum(rate(numRecordsOut[10m])) == 0`. But when a
TaskManager *dies* its metrics do not fall to zero — they stop existing.
Prometheus marks the series stale, `sum(rate(...))` over nothing is an empty
vector, and `empty == 0` is empty, not true. The alert covering "the pipeline
writes nothing" was therefore silent in the most likely way for a pipeline to
stop writing. See [ADR-017](DECISIONS.md).

**`Watchdog` fires unconditionally, and its silence is the alarm.** Every other
rule above assumes Prometheus is running, the rules are loaded, and Alertmanager
is reachable. If any of that is false they all go quiet — and a dead alerting
stack looks exactly like a healthy system. `Watchdog` is `vector(1)`, so it
needs no metric to exist and keeps firing even when the pipeline is gone
entirely. In a real deployment it feeds an external heartbeat monitor that pages
when the ping **stops**. See [ADR-018](DECISIONS.md).

### How it is verified

Two commands, because they are blind to opposite failures and neither alone is
evidence:

```bash
make alerts-test     # promtool, no stack needed — runs in CI
make alerts-verify   # against a running topology
```

| | catches | blind to |
|---|---|---|
| `alerts-test` | wrong threshold, wrong `for:`, wrong comparison, a rule that can never become true | a metric name that does not exist — it feeds synthetic series named by hand |
| `alerts-verify` | typo'd or renamed metrics, rules that failed to load, an `alerting:` block that points nowhere, a receiver that never gets anything | whether a rule fires on the condition it is meant to catch |

Every alert is tested in **both directions** — fires on its condition, silent
below the threshold. The silent half is what makes the first half mean
something: an expression stuck permanently true passes a fires-only suite while
telling you nothing.

`alerts-verify` extracts metric names *from the rules file* rather than
restating them, so a rule that starts watching something new is covered without
anyone remembering to update a list. It also asserts delivery from the
**receiving end** — Prometheus discovered an Alertmanager, Alertmanager is up,
and the sink confirms it actually received the Watchdog. "The rule fires" and
"somebody finds out" are different claims, and until [ADR-018](DECISIONS.md)
only the first was true here: `prometheus.yml` had no `alerting:` block at all.

### What is not covered

**Silently dropped records.** Both production topologies pass
`--on-bad-record skip`, because the measured alternative is one unparseable byte
putting the job in a permanent restart loop. Flink's JSON format discards those
records without incrementing any metric and without logging — measured: 5
malformed records into a running pipeline left `numRecordsInErrors` at 0 and no
line in any TaskManager log. An alert for this was written and then **deleted**:
conservation between Kafka records-consumed and source records-out is ~4x apart
on a healthy pipeline, so a realistic drop rate sits below the noise, and a rule
that loads cleanly but can never fire makes the gap look covered. The honest fix
is a dead-letter topic, which is a change to the job rather than to a rule file.

**Delivery stops at a local receiver.** `alert-sink` stands in for Slack or
PagerDuty, which this repo has no account for. It proves the whole path — rule →
Prometheus → Alertmanager → receiver — and in a real deployment the receiver is
swapped and nothing else changes. There is no on-call rotation, no escalation,
and no silencing workflow.
