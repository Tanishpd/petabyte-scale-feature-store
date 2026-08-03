# Scaling — what "petabyte-scale" actually means here

This repo runs on one laptop. This document is the argument for what the same architecture does at real
scale, which constants govern it, and what breaks first.

> **Numbers marked ⓔ are estimates from published characteristics of the components.**
> **Numbers marked ⓜ are measured in this repo.** Nothing here should stay ⓔ if it can be measured;
> several ⓔ estimates have already been corrected by 2–3× once measured.

---

## 1. Where the petabyte is

The instinctive reading of "petabyte-scale feature store" is a petabyte of Redis. That would cost a
fortune and is not what happens. The volume splits sharply:

| Layer | What it holds | Size at 1M events/s, 50M users |
|---|---|---|
| **Raw event log** (Kafka → object storage) | every event, retained | ⓔ ~30 TB/day raw, ~3 TB/day as zstd parquet → **1 PB in ~5 weeks raw** |
| **Offline store** (training sets, backfills) | historical feature values, point-in-time correct | ⓔ grows with retention — this is the petabyte |
| **Online store** (Redis) | *current* feature vector per user only | ⓜ **~204 GB** |

The online store holds one row per entity, not one row per event. That's the whole trick:

```
per-user row = 384 float32 embedding (1536 B)
             + 8 protobuf-encoded scalars
             + Redis hash + Feast key overhead
             = 4,073 B measured   (MEMORY USAGE, mean over 100 real keys)

50M users × 4.07 KB ≈ 204 GB    → a 6-node Redis cluster, still comfortably
```

An earlier draft of this section estimated 1.8 KB and 90 GB from first principles. The measurement
(§8) came out **2.3× larger** — the embedding is only 38% of the row; Redis hash overhead and protobuf
framing are the rest. The conclusion is unchanged and the arithmetic is now real.

So: **the offline store scales with events × retention; the online store scales with entities.** A
petabyte offline and 204 GB online are the same system.

## 2. Event-rate arithmetic

ⓜ **258 bytes/event**, measured over 545k generated events (compact JSON, no whitespace). An earlier
draft of this document assumed 350 B; every figure below is recomputed from the measurement.

| Rate | Bytes/s | Per day | Time to 1 PB (raw) |
|---|---|---|---|
| 10k/s | 2.6 MB/s | 223 GB | ~12 years |
| 100k/s | 25.8 MB/s | 2.2 TB | ~15 months |
| 1M/s | 258 MB/s | 22.3 TB | **~6.5 weeks** |

Compressed parquet at ⓔ ~10:1 on this schema (low-cardinality categoricals compress well) pushes 1 PB out
to well over a year at 1M/s.

## 3. The embedding bottleneck, and why the design sidesteps it

This is the single most important number in the system.

`all-MiniLM-L6-v2` on CPU costs roughly ⓔ 1–3 ms per text unbatched, ⓔ ~0.3 ms per text at batch 64.
Take the optimistic 0.3 ms → ~3,300 texts/s/core.

**If you embed every event** at 1M events/s you need ~300 CPU cores doing nothing but MiniLM — before any
Flink work. That design does not survive contact with production.

**Embedding on window close** changes the denominator from *events* to *active entities × slide rate*:

```
50k active users ÷ 60 s slide  ≈  830 embeddings/s  ≈  well under one core
```

The 50k is an assumption worth stating: **~0.1% of the 50M total users are active within any given
10-minute window.** (Coincidentally the local mock stream also has a ~50k user population — that is a
different 50k, and the two are unrelated.)

A ~1200× reduction, and the consumer loses nothing — a model reading `user_txn_stats_10m` reads the
window aggregate regardless. The embedding describes recent behaviour, which is the more useful semantic
anyway.

**The governing formula:**

```
embeddings/sec = active_entities / slide_interval_seconds
```

Note what's absent: event rate. Embedding cost is decoupled from stream volume entirely. Ingest can 10×
without the embedding tier moving.

## 4. Component sizing at 1M events/s

| Component | Sizing | Reasoning |
|---|---|---|
| **Kafka** | ⓔ 24 partitions, RF=3, ~12 brokers | ~10–40 MB/s sustained per partition; 350 MB/s ingress plus replication |
| **Flink** | ⓔ parallelism 24 (= partitions), RocksDB state backend, incremental checkpoints to S3 | keyed state = active users × 10-min window contents |
| **Flink state** | ⓔ ~50k users × ~10 KB window buffer ≈ 500 MB/slot-group | HOP windows retain 10 slide-panes per key; same 0.1%-active assumption as §3 |
| **Redis** | ⓔ 6–10 shards | ~100k ops/s/shard; 1M feature reads/s needs ~10 |
| **Embedding tier** | ⓔ 1–2 cores | see §3 |

Parallelism is capped by partition count — that's why partitions are the first thing to get right.

## 5. What breaks first, in order

Raise load 10× at a time and the failures arrive in this sequence:

1. ⓜ **~8,500 events/s — the Flink pipeline**, on 2 task slots sharing 8 cores. Measured by saturating
   it and timing the recovery: 3 producers at 9k each sustained **26,995 eps aggregate** for 120 s, which
   built **2.9M events of consumer lag**; with producers stopped, the backlog drained at
   **8,526 events/s**, with TaskManager CPU pinned at 590% of the 800% available. At 5,000 eps the same
   pipeline holds **zero lag on all 6 partitions**, so the ceiling sits between the two.
   *What saturates:* the embedding UDF — §3 measures MiniLM at 450–475 emb/s per process and it is
   memory-bandwidth bound, so more slots (or a GPU, or fewer embeddings via a longer slide) is the fix.
2. ⓜ **~30k events/s — a single Python producer.** One producer tracks its target exactly to 5k and tops
   out at 29.6k (74% of a 40k ask); two in parallel each hold 20k for an aggregate of 39,972 eps, so the
   broker was never the constraint — the limit is CPU-bound generation plus `kafka-python`'s pure-Python
   protocol. *Fix:* run N producers, or move to `confluent-kafka` (librdkafka).

   **This ordering is a correction.** An earlier draft of this section put the producer first, at ~30k,
   with Flink "not yet reached" — true only because nothing had pushed past the producer's ceiling. Once
   three producers ran together, Flink turned out to bend more than 3× earlier. Before that, this list
   blamed single-broker Kafka at ~10k, which was wrong too. Both corrections came from measuring.
3. **Checkpoint duration** as keyed state grows. ⓔ — not reached; the embedding tier saturates first.
   *Fix:* RocksDB backend, incremental and unaligned checkpoints.
4. **~100k reads/s** — single Redis node hits its ceiling. *Fix:* Redis Cluster; Feast supports
   `redis_type: redis_cluster` natively.

Note what this does *not* contradict. Embedding on window close is still what makes the design viable:
the ~8.5k ceiling is set by embedding **windows** at this laptop's 2-slot capacity, not by embedding
events. A per-event design would have to embed 8,500 vectors/s at the same point instead of ~441 — about
19× more work for identical output. §3's argument holds; this section just says how far two slots get.

## 6. Hot keys

The producer draws user IDs from a Zipf distribution deliberately: uniform-random users produce a
perfectly balanced keyspace that no real workload has. With Zipf, a few users generate a
disproportionate share of events, and their Flink keyed-state partitions become hot. This is what
surfaces in the Flink UI as one busy subtask among many.

Standard mitigations, in escalating order: more partitions than parallelism, a two-stage
pre-aggregation (local combine before keyed shuffle), or salted keys with a final merge. Worth
demonstrating at Phase 8 because it's the difference between a benchmark and a system.

## 7. Freshness budget

End-to-end, event → readable by a model:

| Stage | Budget |
|---|---|
| producer → Kafka ack | ⓔ ~1–5 ms |
| Kafka → Flink consume | ⓔ ~5–20 ms |
| watermark wait (out-of-order allowance) | **30 s** — configured, dominates |
| window close → embedding → HSET | ⓔ ~5–50 ms |
| `get_online_features()` | ⓜ 2.2 ms p99 (single entity) — see §8 |

The 30-second watermark delay dwarfs everything else, and it's a deliberate correctness-vs-latency dial,
not overhead. Drop it to 1 s and features are near-instant but late events get dropped. This tradeoff is
the real content of "real-time" — the pipeline machinery is milliseconds; the *semantics* cost seconds.

### The parallel vector index

Feast cannot search vectors on Redis (ADR-004), so the sink writes each embedding a **second** time as a
raw float32 blob under `petafs:vec:{user_id}` for a RediSearch HNSW index. Costs measured at ~1.6 KB per
entity on top of the ~4 KB Feast row — call it 40% more online storage to buy nearest-neighbour
retrieval. `make similar` returns 4 neighbours in ⓜ ~57 ms cold on a 150-vector index.

Two failure modes worth knowing, both found by testing rather than reasoning:

- **`FT.CREATE` races across task slots.** Every slot calls it from `open()`; the losers get "index
  already exists". Treating that as an error made the sink disable indexing *for that slot only*, so
  exactly half the keyspace (74 of 150 entities) was searchable while everything looked healthy.
- **`FT.SEARCH` defaults to `LIMIT 0 10`** and truncates the response regardless of the KNN `k`, so
  asking for 20 neighbours silently returns 10.

## 8. Online read path — measured

`make bench`, idle cluster, 150 live entities, host venv → Redis on :6380.

| | p50 | p99 |
|---|---|---|
| raw Redis `HGETALL`, 1 entity | ⓜ 0.19 ms | ⓜ 1.7 ms |
| `get_online_features`, 1 entity | ⓜ 0.93 ms | ⓜ **2.17 ms** |
| `get_online_features`, 10 entities | ⓜ 3.49 ms | ⓜ 5.37 ms |
| `get_online_features`, 100 entities | ⓜ 15.72 ms | ⓜ **22.55 ms** |

Both PLAN targets hold: p99 < 10 ms single, < 25 ms for 100.

**Redis is not the cost.** A single-entity read is 0.19 ms of Redis and 0.74 ms of Feast — proto decode,
registry lookup, response assembly — so ~79% of the call is Python. 100 keys fetched in one Redis
pipeline take ⓜ 1.6 ms, versus ⓜ 15.8 ms issued sequentially, so the connection is not the bottleneck
either. Batching still pays: per-entity cost falls from 0.93 ms to 0.157 ms, ~6× cheaper at 100.

**Per-entity row size: ⓜ 4,073 B** (`MEMORY USAGE`, mean over 100 keys) — more than double §1's 1.8 KB
estimate. The 384-float embedding is 1,536 B of it; the rest is Redis hash overhead plus 9 protobuf-
encoded values. Revised: 50M users ≈ **204 GB**, not 90 GB. Still small next to the offline petabyte, and
still the point of §1 — but the estimate was optimistic by 2.3×.

### A read-path degradation, and how to avoid it

Reads through a **single long-lived `FeatureStore` slow down as calls accumulate**: ⓜ 6.9 ms → 18.4 ms
p50 over 200 calls on a 100-entity read, a 2.7× drift.

**Mitigation — touch the registry periodically.** Calling `store.get_feature_view(...)` between batches
removes the drift entirely: ⓜ 1.1× over the same 200 calls, versus 2.7× without. A long-lived model
server should refresh the registry on an interval rather than only at startup.

| | 40 | 80 | 120 | 160 | 200 calls |
|---|---|---|---|---|---|
| reads only | 6.91 | 9.99 | 14.08 | 16.10 | **18.38 ms** |
| + `get_feature_view()` between blocks | 6.41 | 6.95 | 6.64 | 6.65 | **7.09 ms** |

What the profile shows: `feast/utils.py:_populate_response_from_feature_data` self-time grows 8.7×
(0.061 s → 0.530 s for the same 40 calls) and total Python calls per batch go 798k → 2,005k, so real work
is increasing rather than the process merely being descheduled. What is ruled out:

- not elapsed time — 30 s idle changes nothing
- not a connection leak — Redis stays at 2 `connected_clients`
- not a Python object leak — no growth in `gc.get_objects()` counts
- not Prometheus metrics — `online_features` tracking is disabled by default and the multiprocess mmap
  files stay a constant 196,608 B
- not a growing feature projection — `n_features` and `output_len` stay 9 and 100
- not per-instance — constructing any new `FeatureStore` restores speed for *existing* instances too

The common factor between the two things that fix it — a new `FeatureStore` and `get_feature_view()` — is
that both touch the registry. Exact mechanism inside Feast unidentified.

It silently corrupted this benchmark before it was noticed: an early run reported a 107 ms p99 that was
mostly drift plus contention with a running Flink job. `make bench` now uses a fresh store per batch size
and reports the drift separately as section 5.

Replace every ⓔ above with a measurement:

- Sustained producer throughput vs. target EPS (find where the closed loop can't keep up)
- Flink records/s per subtask, backpressure ratio, checkpoint duration vs. state size
- Embeddings/s per task slot, batched vs. unbatched (proves the pandas-UDF decision)
- `get_online_features()` p50/p95/p99, 1 entity vs. 100 entities per call
- Redis memory per entity, measured with `MEMORY USAGE` on a real key — validates the 1.8 KB estimate
- End-to-end freshness: event timestamp → readable, sampled continuously
