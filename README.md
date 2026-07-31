# Petabyte-Scale Feature Store & Streaming Pipeline

A real-time feature pipeline: high-velocity events in, model-ready numeric features **and vector
embeddings** out, served from a feature store in single-digit milliseconds.

> **Status: design phase — documentation only, no code yet.** The Quickstart below describes the target
> interface, not something you can run today. Build progress is tracked in [PLAN.md](PLAN.md).

```
  producer            Kafka              Flink                      Redis            model
 ─────────────  →  ───────────  →  ────────────────────  →  ─────────────────  →  ──────────
  mock txns        transactions     10-min sliding windows     Feast encoding      get_online_
  ~50k users        (KRaft)         + MiniLM embeddings        (hash per user)     features()
```

The interesting part isn't that the boxes connect — it's *how*. Flink writes into Redis using **Feast's
own binary encoding**, so there is no batch materialisation step between computing a feature and a model
being able to read it. Event → readable feature is one hop.

---

## Quickstart

**Prerequisites:** Docker Desktop running, Python 3.11, **~20 GB free disk**. The Flink image bundles
PyTorch and the embedding model (~3–4 GB), and sustained producing adds Kafka log segments fast — at
`EPS=20000` that's roughly 25 GB/hour, which is why the `transactions` topic gets retention caps at
creation.

```bash
make bootstrap     # python3.11 venv + pinned deps
make up            # Kafka + Flink + Redis, waits until healthy  (first run: ~5 min image build)
make apply         # register the feature definitions with Feast
make produce       # start the mock transaction stream  (leave running)
make submit        # submit the PyFlink job              (leave running)
make read          # a "model" pulls features for a user — this is the payoff
```

Then:

```bash
make bench         # p50/p95/p99 read latency, single + batched
make demo          # scripted end-to-end walkthrough
make down          # stop everything
```

---

## What each piece does

**`src/producer/generate_stream.py`** — Generates mock transactions and pushes them to Kafka with
`kafka-python`. It's deliberately not uniform random: user IDs follow a **Zipf** distribution so hot keys
exist, ~0.5% of events are **anomalies** (10–30× normal spend) so the rolling averages actually move, and
a slice of events arrives **out of order** by up to 30 s so watermarking is exercised rather than assumed.
Rate is controlled with `make produce EPS=20000`.

**`src/flink/streaming_job.py`** — A PyFlink job that reads the topic with event-time watermarks and
computes a **10-minute sliding window, advancing every minute**, keyed by user:

| Feature | Meaning |
|---|---|
| `txn_count_10m` | transactions in the window |
| `txn_amount_avg_10m` | **the rolling average** |
| `txn_amount_sum_10m` / `_max_10m` / `_stddev_10m` | spend shape |
| `distinct_merchants_10m` | breadth of activity |
| `behavior_summary` | e.g. `"coffee_shop, rideshare, grocery"` |
| `embedding` | 384-dim vector from `all-MiniLM-L6-v2` |

The embedding is computed from a rendered sentence describing the user's recent behaviour — so two users
with similar spending patterns land near each other in vector space.

**`src/flink/sinks/feast_redis_sink.py`** — Writes each window result straight into Redis in the exact
layout Feast expects (one hash per user; fields keyed by a murmur3 hash of
`feature_view:feature_name`; values are serialized protobufs). It imports Feast's own encoder rather than
reimplementing it, so reads are correct by construction.

**`src/serving/online_read.py`** — What a production model server would actually contain:

```python
store.get_online_features(
    features=["user_txn_stats_10m:txn_amount_avg_10m", "user_txn_stats_10m:embedding"],
    entity_rows=[{"user_id": 42}],
).to_dict()
```

---

## Two design decisions worth knowing

**Embeddings are computed when a window closes, not per event.** A naive design embeds every transaction.
At 1M events/s that's 1M embeddings/s — impossible on CPU, expensive on GPU. Embedding on window close
instead means one vector per *active user per minute*: with 50k active users, ~830/s. Same freshness for
the model, ~1200× less compute. This is the load-bearing idea in the whole design.

**The online store is small; the offline store is the petabyte.** Raw events at 1M/s are ~30 TB/day and
cross a petabyte in about a month. But the *serving* layer only holds current state — 50M users × ~1.8 KB
≈ 90 GB, which fits comfortably in a modest Redis cluster. Understanding that split is what makes
"petabyte-scale" tractable rather than terrifying. The full argument, with measured numbers, is in
[docs/SCALING.md](docs/SCALING.md).

---

## Documentation

| Doc | What's in it |
|---|---|
| [PLAN.md](PLAN.md) | Phased build plan with acceptance criteria and risk register |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Data flow, schemas, how the Feast/Redis encoding works |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Why these versions, why direct-write, what was rejected |
| [docs/SCALING.md](docs/SCALING.md) | The petabyte math, bottleneck ordering, what breaks at each 10× |
| [CLAUDE.md](CLAUDE.md) | Working context for AI coding sessions |

## Troubleshooting

| Symptom | Cause |
|---|---|
| Producer hangs, no error | Using `kafka:9092` from the host. Host scripts must use `localhost:29092`. |
| `get_online_features` returns all `None` | Feature-view or feature name drift between the Flink sink and the Feast registry — the Redis field is a hash of both. Check `contracts/schema.py` is the only definition. |
| Flink job fails on submit | Version skew between the cluster image and the `apache-flink` pip package. Both must be 2.2.1. |
| First `make up` looks stuck | It's building a ~3 GB image with PyTorch, downloading the model, and — on Apple Silicon — compiling PyFlink from source, since no Linux aarch64 wheel exists. Once only. |
| Image build fails compiling PyFlink | Missing `build-essential` / `python3.12-dev` in the Flink Dockerfile. See [ADR-009](docs/DECISIONS.md). |
| `Cannot connect to the Docker daemon` | Docker Desktop isn't running. |
