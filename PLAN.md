# Build Plan — Petabyte-Scale Feature Store & Streaming Pipeline

> Status: **Phases 0–1 complete** (2026-08-01). Next: Phase 2, the mock stream.
> Update the status column as phases land.
> Every phase has a *verifiable* acceptance criterion. A phase is not done until its check passes.

## 0. What we're building, in one paragraph

A high-velocity event stream (mock user transactions) lands in **Kafka**. **Flink** consumes it, computes
sliding-window behavioural aggregates per user (rolling 10-minute average spend, counts, distinct
merchants, etc.), renders each user's recent behaviour into a short natural-language sentence, and embeds
that sentence with **sentence-transformers** into a 384-dim vector. Flink writes both the numeric features
and the vector **directly into Redis using Feast's own on-disk encoding**, so a production model can read
them back in single-digit milliseconds via `feast.get_online_features()` — with no batch job in between.

## 1. Phase table

| # | Phase | Deliverable | Acceptance check | Status |
|---|-------|-------------|------------------|--------|
| 0 | Foundations | Repo skeleton, py3.11 venv, pinned `requirements*.txt`, `Makefile` | `make bootstrap` → feast 0.65.0, redis 7.4.1 (<8), kafka-python 3.0.9 | ☑ |
| 1 | Infrastructure | `docker-compose.yml`: Kafka (KRaft), Flink JM+TM (custom image), Redis Stack, Kafka UI | `make up` → 4 services healthy, TM registered with 2 slots, Invariant-4 round-trip on `localhost:29092` | ☑ |
| 2 | Mock stream | `producer/generate_stream.py` — Zipf users, anomalies, late events, tunable EPS | `make produce EPS=5000` sustains ≥5k msg/s; `make tail-topic` shows valid JSON | ☐ |
| 3 | Feature contract + Feast repo | `contracts/schema.py` (single source of truth), Feast entity/FV, seeded offline parquet | `feast apply` clean; `feast feature-views list` shows `user_txn_stats_10m` | ☐ |
| 4 | Flink aggregation | PyFlink job: Kafka → watermarks → HOP(10min/1min) → print | Printed rolling avg matches a pandas recomputation of the same window (±1e-6) | ☐ |
| 5 | Embedding UDF | Pandas UDF (`func_type="pandas"`), model baked into image, offline mode | Job emits `ARRAY<FLOAT>` len 384; ≥500 embeddings/s/slot measured | ☐ |
| 6 | **Feast-compatible Redis sink** | `sinks/feast_redis_sink.py` using Feast's own key codec | **Round-trip test**: Flink writes → `get_online_features()` returns identical values | ☐ |
| 7 | Serving | `serving/online_read.py` + `serving/benchmark.py` | p99 single-entity read < 10 ms local; 100-entity batch < 25 ms | ☐ |
| 8 | Scale story | Load test, real numbers in `docs/SCALING.md`, ANN index, tests, README polish | `make test` green; SCALING.md contains *measured* not guessed numbers | ☐ |

## 2. Phase detail

### Phase 0 — Foundations
- Host: Python **3.11** venv (`.venv`). Container: Python **3.12** (forced by the base image — see
  [ADR-001](docs/DECISIONS.md)). These deliberately differ; only the `apache-flink` version must match
  the cluster.
- Two requirement files: `requirements-host.txt` (feast, kafka-python, redis, pandas, pyarrow) and
  `requirements-flink.txt` (apache-flink, sentence-transformers, torch, feast[redis]).
  They are installed into *different* interpreters and must not be merged.
- **Torch must come from the CPU index**: `torch==<pin>+cpu` with
  `--extra-index-url https://download.pytorch.org/whl/cpu`. There is no `torch-cpu` package (PyPI 404),
  and plain `torch` on Linux pulls the CUDA stack — several GB — blowing both the image estimate and the
  disk budget.
- `Makefile` is the only UX. Every operation gets a target; the README never tells the user to type a
  raw `docker compose` command.

### Phase 1 — Infrastructure
- **Kafka** `apache/kafka:4.3.1` in **KRaft** mode (no Zookeeper). Dual listeners are mandatory:
  `INTERNAL://kafka:9092` for containers, `EXTERNAL://localhost:29092` for host scripts. Getting this
  wrong is the single most common failure in this stack — the producer connects, then hangs on metadata.
- **Flink** custom image `FROM flink:2.2.1-scala_2.12-java17`, adding **python3.12** (the base is
  `eclipse-temurin:17-jre-noble` = Ubuntu 24.04, which has no python3.11 package at all),
  `build-essential` + `python3.12-dev` (mandatory — see [ADR-009](docs/DECISIONS.md); PyFlink has no
  Linux aarch64 wheel, so pip compiles from sdist), `apache-flink==2.2.1`, `sentence-transformers`,
  `feast[redis]`, the Kafka SQL connector JAR, and the **pre-downloaded MiniLM model** baked into the
  image layer with `HF_HUB_OFFLINE=1` at runtime.
- **Redis** `redis/redis-stack-server:7.4.0-v8` — plain Redis plus RediSearch, which we use for the
  optional ANN index in Phase 8. Feast itself only needs plain Redis semantics.
- **Kafka UI** (`ghcr.io/kafbat/kafka-ui`, the maintained fork of the abandoned `provectuslabs` image)
  for eyeballing the topic. Nice for demos, zero production meaning.
- Set `retention.bytes` / `retention.ms` on the `transactions` topic at creation. Without caps, the
  load tests in Phase 8 fill the disk — at `EPS=20000` and ~350 B/event that is ~25 GB/hour.
- Health checks on every service; `depends_on: condition: service_healthy` so `make up` is deterministic.

### Phase 2 — Mock stream
Realism is what makes the downstream numbers interesting. The generator does four things a naive
`while True: send(random())` loop does not:
1. **Zipf-distributed user IDs** — creates genuine hot keys, so Flink's keyed state is skewed like production.
2. **Anomaly injection** (~0.5% of events at 10–30× normal amount) — makes rolling averages visibly move.
3. **Out-of-order events** (a slice delayed by up to 30 s) — exercises watermarks instead of pretending
   event time equals processing time.
4. **Closed-loop rate control** — `--eps` targets a real rate and reports actual achieved throughput.

### Phase 3 — Feature contract
`contracts/schema.py` defines the feature-view name, field names, and dtypes **once**. Both the Feast
definitions and the Flink sink import it. This is not ceremony: the Redis field key is
`mmh3(f"{feature_view_name}:{feature_name}")`, so a one-character drift between the writer and the
registry produces a silent miss — features write successfully and read back as `None`. The shared
contract makes that class of bug unrepresentable.

### Phase 4 — Aggregation
Table API, `HOP` window TVF: 10-minute size, 1-minute slide, keyed by `user_id`. Emits
`txn_count_10m`, `txn_amount_sum_10m`, `txn_amount_avg_10m`, `txn_amount_max_10m`,
`txn_amount_stddev_10m`, `distinct_merchants_10m`, and `behavior_summary` — a plain
`LISTAGG(merchant_category)` that is **deduplicated and sorted in Python**, not with SQL `DISTINCT`.

That last point is load-bearing: `LISTAGG(DISTINCT ...)` under a HOP window fails at plan time on Flink
2.2.1 (`TableException: This function does not support retraction`). `COUNT(DISTINCT)` in the same query
works, and `LISTAGG(DISTINCT)` works under `TUMBLE` — so the failure is specific enough to invite being
"fixed" back. Normalising in Python also makes the embedding input deterministic, which `LISTAGG`'s
arrival-order output otherwise is not. The normalised string is what gets stored *and* embedded.

HOP output is **append-only**, which is what lets us convert to a DataStream and write from a plain map
operator in Phase 6 without dealing with retractions.

### Phase 5 — Embedding
Two decisions carry this phase:
- **Embed on window close, not per event.** One embedding per user per slide interval, not one per
  transaction. At 1M events/s with 50k active users this is ~830 embeddings/s instead of 1,000,000/s —
  a ~1200× reduction that is the difference between "works" and "impossible". See [SCALING.md](docs/SCALING.md).
- **Pandas UDF, not scalar UDF.** `@udf(func_type="pandas")` hands the function a whole Series, so we call
  `model.encode(texts, batch_size=64)` once per batch. Row-at-a-time encoding wastes ~10× on Python
  overhead and forfeits intra-batch tensor parallelism.

Model loads lazily in `open()` and is cached per task slot.

### Phase 6 — The Feast-compatible sink *(highest risk, highest value)*
Feast stores each entity as one Redis hash:
- **key** = `_redis_key(project, entity_key, entity_key_serialization_version)` → binary blob
- **field** = `_mmh3(f"{fv_name}:{feature_name}")` → 4 bytes
- **value** = `ValueProto.SerializeToString()`
- plus a `_ts:{fv_name}` field holding a serialized protobuf `Timestamp`

We do **not** reimplement that. The Flink image installs `feast[redis]`, and the sink imports
`feast.infra.online_stores.helpers._redis_key/_mmh3` and `feast.protos...ValueProto` directly. That makes
the *encoding* correct by construction — but not the *name* it encodes: keep
`registry.enable_online_feature_view_versioning` at its default `false`, or Feast will hash a `_v{N}`
suffix that our sink knows nothing about. Writes are pipelined (flush at 500 rows or 100 ms, whichever
first), with an `EXPIRE` alongside each `HSET` since `FeatureView.ttl` does not expire Redis keys.

Implementation shape: `t_env.to_data_stream(table) → MapFunction("FeastRedisWriter")`. PyFlink has no
generic Python sink interface, so a map that writes as a side effect is the idiomatic route — and it needs
no terminal sink, because `map()` registers its operator with the execution environment at creation.
Note `to_data_stream` is a `StreamTableEnvironment` method, and PyFlink's class is `MapFunction` (already
rich — `open()`/`close()` live on the base `Function`); `RichMapFunction` and `DiscardingSink` are Java
names that do not exist in PyFlink.

**Acceptance test is non-negotiable**: `tests/test_feast_codec_roundtrip.py` writes a row through the
exact sink code path, then reads it with a real `FeatureStore.get_online_features()` and asserts every
field matches, embedding included. If this test passes, the whole architecture is proven.

### Phase 7 — Serving
`online_read.py` is deliberately dumb — it is what a model server would actually contain:

```python
store.get_online_features(features=[...], entity_rows=[{"user_id": 42}]).to_dict()
```

`benchmark.py` reports p50/p95/p99 for single-entity and 100-entity batch reads, plus end-to-end
freshness (event timestamp → readable in Redis).

### Phase 8 — Scale story
Load-test at increasing EPS until something breaks, record *what* broke first and at what number, and
write the extrapolation to petabyte scale from measured constants rather than vibes. Optional: a parallel
RediSearch HNSW index written by the same sink, giving true ANN retrieval (Feast's Redis store has no
vector search — see [ADR-004](docs/DECISIONS.md)).

## 3. Risk register

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Feast internal helper API changes between versions | Med | Pin `feast==0.65.0` exactly; round-trip test catches it immediately |
| PyFlink 2.2 Table→DataStream quirks with `ARRAY<FLOAT>` | Med | Fallback: serialize embedding to `BYTES` in Flink, decode in sink |
| Flink image is large (~3 GB with torch) | High | Accepted — user has confirmed disk is not a constraint |
| Kafka dual-listener misconfiguration | High | Explicit smoke test in Phase 1 before anything else is built |
| Embedding UDF starves task slots | Med | Measured in Phase 5; mitigation is embed-on-window-close (already designed in) |
| **PyFlink sdist compile in the arm64 image is slow or fails** | **High** | No Linux aarch64 wheel exists for any Flink version ([ADR-009](docs/DECISIONS.md)). Install `build-essential` + `python3.12-dev`; if the build proves unworkable, fall back to `platform: linux/amd64` under emulation — at a real runtime cost |
| Disk exhaustion during Phase 8 load tests | Med | Topic retention caps set at creation in Phase 1 |

## 4. Out of scope (deliberately)

Real petabyte storage, a managed Kafka/Flink cluster, model training, authentication, and multi-region
replication. The repo demonstrates the *architecture* and proves the hot path; `docs/SCALING.md` carries
the argument for what changes at real scale.
