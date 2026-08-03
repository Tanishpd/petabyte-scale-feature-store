# Architecture Decision Records

Each record states the decision, the evidence behind it, and what we rejected. Evidence gathered
2026-07-27 against live package indexes — not from memory.

---

## ADR-001 — Host Python 3.11, container Python 3.12

**Decision.** The host venv runs 3.11. The Flink container runs 3.12. They deliberately differ.

**Evidence.** The container's version is not a choice: `flink:2.2.1-*` images are built on
`eclipse-temurin:17-jre-noble`, i.e. Ubuntu 24.04, whose archive contains **no python3.11 package at
all** — only 3.12. Getting 3.11 in would require the deadsnakes PPA or a source build, for no benefit:
`apache-flink` 2.2.1 supports cp312.

On the host, `feast` 0.65.0 requires `>=3.10.0` and `sentence-transformers` 5.6.1 requires `>=3.10`;
3.11 has the broadest third-party wheel coverage in that range.

**Why they need not match.** Only the `apache-flink` *version* must equal the cluster's. The interpreter
version is per-process. And per the conventions in [CLAUDE.md](../CLAUDE.md), the host never runs PyFlink
at all — jobs are submitted *inside* the container with `flink run -py`, so the host's Python is
constrained by Feast, not by Flink.

**Rejected.** Adding the deadsnakes PPA to force 3.11 in the container (extra moving part, no gain).
Host 3.13/3.14 — present on this machine, but outside Feast's tested range.

---

## ADR-002 — Flink 2.2.1, not the latest 2.3.0

**Decision.** Pin the cluster and the `apache-flink` pip package to 2.2.1.

**Evidence.** Flink 2.3.0 is released and has arm64 images, but the newest published
`flink-sql-connector-kafka` on Maven Central is `5.0.0-2.2` — built for Flink 2.2. Connector artifacts
are versioned `<connector-version>-<flink-minor>` and lag Flink releases. Since the Kafka connector is
non-optional here, connector availability sets the version ceiling, not Flink's release page.

**Consequence.** When `*-2.3` ships, upgrading is a three-line change (image tag + pip pin + JAR URL).

**Fallback.** If PyFlink 2.x proves awkward, drop to Flink 1.20.3 + connector 3.4.0-1.20 — the last 1.x
LTS, and the version nearly all PyFlink documentation targets. Amend this ADR if taken. Note this
fallback does **not** avoid the aarch64 build problem in ADR-009 — no Flink version does.

---

## ADR-003 — Flink writes to Redis directly, using Feast's own encoder

**Decision.** The Flink sink opens a Redis connection and writes Feast-encoded hashes itself. It
**imports** `_redis_key` and `_mmh3` from `feast.infra.online_stores.helpers` and `ValueProto` from
`feast.protos.feast.types.Value_pb2` rather than reimplementing the format.

**Evidence.** Reading `feast/infra/online_stores/redis.py` at v0.65.0, `online_write_batch` builds each
record as:

```python
feature_view  = _versioned_fv_name(table, config)      # ← see "The name is not encoded" below
redis_key_bin = _redis_key(project, entity_key, entity_key_serialization_version=...)
ts_key        = f"_ts:{feature_view}"
entity_hset   = {ts_key: Timestamp_proto_bytes}
for feature_name, val in values.items():
    entity_hset[_mmh3(f"{feature_view}:{feature_name}")] = val.SerializeToString()
pipe.hset(redis_key_bin, mapping=entity_hset)
```

(That is the `skip_dedup=True` fast path. Feast's **default** path first pipelines an `HMGET` of
`_ts:{fv}` for every key and skips any write whose timestamp is older than what is stored — it will not
regress a newer value to an older one. Our sink gives that up. Acceptable, because HOP windows fire per
key in `window_end` order; but a replay after a Flink restart can transiently regress values, so the sink
compares `window_end` against the stored `_ts` before writing.)

That is a small, fully legible format — but it is *internal* and has already changed across versions
(entity-key serialization is on v3, with an explicit v2→v3 re-serializer in the codebase).

**Why import instead of reimplement.** A hand-rolled codec is ~40 lines and would work today. It would
also break silently on upgrade, and "silently" is the operative word: a wrong hash means writes succeed
and reads return `None`, with no exception on either side. Importing makes correctness structural.

**Rejected alternative:** Flink → a second Kafka topic → a Python service calling
`store.write_to_online_store()`. This is the more conventional Feast pattern and is arguably safer, but
it adds a network hop and a service to the hot path, and the brief explicitly calls for Flink to stream
features *directly* into Redis. The round-trip test (see ADR-005) buys back the safety.

**The name is not encoded — and that is the residual risk.** Importing Feast's helpers guarantees the
*encoding*; it does not guarantee the *string* being encoded. Note the first line of the snippet:
`online_write_batch` resolves the feature-view name through `_versioned_fv_name(table, config)` before
hashing. With `registry.enable_online_feature_view_versioning: true`, Feast hashes
`{fv_name}_v{N}:{feature}` and reads `_ts:{fv_name}_v{N}`, while our sink hashes the raw name from
`contracts/schema.py`.

The trap is the timing: a fresh feature view is v0 and unsuffixed, so the pipeline works perfectly until
the first schema change promotes it to v1 — after which reads silently return `None`. So: **keep that
setting at its default `false`**, and treat "correct by construction" as applying to the codec only.

**Guardrail.** `feast==0.65.0` is pinned exactly, and the round-trip test fails loudly if the internal
format shifts.

---

## ADR-004 — The embedding is a stored feature, not a Feast vector index

**Decision.** Store the 384-dim vector as an `Array(Float32)` field on the feature view, retrieved by
entity key. Any true nearest-neighbour search is served by a **separate RediSearch HNSW index** that our
sink maintains alongside the Feast hash.

**Evidence.** Feast 0.65.0's Redis online store implements no vector search — grepping `redis.py` for
`vector`, `knn`, `retrieve_online_documents` returns nothing. Feast's document-retrieval API is
implemented for Milvus, Qdrant, FAISS, pgvector, Elasticsearch, MongoDB and sqlite-vec; Redis is not
among them.

**Consequence.** "Fetch this user's embedding" is a normal Feast read and works today. "Find similar
users" is a bonus path outside Feast. We use `redis/redis-stack-server` (which bundles RediSearch) so the
option stays open at zero extra cost.

**Rejected.** Swapping the online store to Milvus/Qdrant to get Feast-native ANN — the brief specifies
Redis, and key-based retrieval is the actual serving pattern for a feature store.

---

## ADR-005 — A round-trip test is the acceptance criterion for the write path

**Decision.** `tests/test_feast_codec_roundtrip.py` writes a row through the *production sink code path*,
then reads it back with a real `FeatureStore.get_online_features()` and asserts field-level equality
including the embedding array.

**Rationale.** Every risk introduced by ADR-003 collapses into one question: does what Flink writes equal
what Feast reads? A single test answers it, and answers it again automatically on every dependency bump.
Without it, the failure mode is a pipeline that looks healthy end to end while serving `None`.

---

## ADR-006 — Embed on window close, not per event

**Decision.** The embedding UDF runs on the aggregated window output — one vector per user per slide
interval — never on raw events.

**Rationale.** MiniLM costs roughly 0.3–3 ms per text on CPU depending on batching. Per-event embedding at
1M events/s needs on the order of 1M embeddings/s. Per-window embedding with 50k active users and a
1-minute slide needs ~830/s (50,000 ÷ 60). That is a ~1200× reduction with **no loss of freshness for the
consumer**, because the model reads the window aggregate anyway.

**Consequence.** The embedding describes a user's *recent behaviour*, not a single transaction — which is
also the more useful semantic for user-level features.

---

## ADR-007 — A shared feature contract module

**Decision.** `contracts/schema.py` holds feature-view name, field names, and dtypes. Feast definitions
and the Flink sink both import it. Nothing else names a feature.

**Rationale.** The Redis field key is `mmh3(f"{feature_view}:{feature_name}")`. Two independent string
literals that must stay byte-identical, in two different processes, in two different interpreters, is a
latent bug with no error path. One import removes the class of failure entirely.

---

## ADR-008 — Kafka in KRaft mode with dual listeners

**Decision.** `apache/kafka:4.3.1`, KRaft (no Zookeeper), advertising `INTERNAL://kafka:9092` to
containers and `EXTERNAL://localhost:29092` to the host.

**Rationale.** Zookeeper is removed in Kafka 4.x. The dual-listener setup is required because Flink
connects from inside the Compose network while the producer and Feast scripts run on the host — a
single advertised listener cannot satisfy both, and the failure mode (client connects to the broker,
receives unreachable metadata, then hangs without erroring) is notoriously opaque.

---

## ADR-009 — Accept compiling PyFlink from source in the arm64 image

**Decision.** The Flink Dockerfile installs `build-essential` and `python3.12-dev` before
`pip install apache-flink==2.2.1`, and we accept a slow first image build.

**Evidence.** `apache-flink` has **never published a Linux aarch64 wheel** — checked across 1.20.0,
1.20.1, 1.20.3, 2.0.0, 2.1.3, 2.2.1 and 2.3.0; every Linux wheel is `manylinux..._x86_64`. The macOS
`arm64` wheels that do exist are irrelevant here, because the install that matters happens inside a
`linux/arm64` container on Apple Silicon.

So pip falls back to the sdist. That sdist ships seven pre-generated `.c` files (`coder_impl_fast.c`
alone is 4.7 MB), so `setup.py`'s no-Cython branch still builds C extensions from them — the pure-Python
`extensions = []` path at `setup.py:170` is unreachable whenever those files are present, which for a
released sdist is always. Cython itself is *not* the blocker: it is only a build requirement under
`sys_platform == 'darwin' and python_version == '3.8'`. A C compiler and Python headers are.

The base image `eclipse-temurin:17-jre-noble` is JRE-only and ships neither.

**Confirmed by building it** (2026-08-01, M-series, 8 cores, Docker 8 GB):

```
Arch=arm64 OS=linux            # native, not emulated
machine    : aarch64
py         : 3.12.3
fast coder : .../pyflink/fn_execution/coder_impl_fast.cpython-312-aarch64-linux-gnu.so
```

That `.so` is the proof: pip did fall back to the sdist and did compile the C extensions for aarch64,
exactly as predicted. Without `build-essential` + `python3.12-dev` the layer fails.

**Consequence — measured, and milder than expected.** The whole `pip install` layer took **242 s**, with
the compile a minority of that (the rest is downloading torch, transformers and feast). Total image build
≈ 5 min cold, 3.48 GB. An earlier estimate here said 10–40 minutes; that was guesswork and it was wrong.
Note pip buffers compiler output unless the build fails, so a successful run shows *no* gcc lines — the
absence of compile chatter is not evidence that no compile happened. Subsequent builds hit the layer
cache. `apache-beam` — PyFlink's own dependency — does publish `manylinux_2_17_aarch64` wheels, so it is
not affected.

**Rejected.** (a) Switching Flink versions to find a wheel — none exists, at any version; this is a
platform constraint, not a pin. (b) Running the Flink services as `platform: linux/amd64` under
emulation: the x86_64 wheel would install cleanly, but the whole TaskManager — including torch
inference — would then run under QEMU, which is the wrong trade for a throughput demo. Keep it as the
escape hatch if the compile proves unworkable.

---

## ADR-010 — Deduplicate the category list in Python, not with SQL `DISTINCT`

**Decision.** `behavior_summary` is computed as a plain `LISTAGG(merchant_category)`, then deduplicated
and sorted in Python. The normalised string is what gets both stored as the feature and embedded.

**Evidence.** `LISTAGG(DISTINCT ...)` inside a HOP window TVF **fails at plan time** on Flink 2.2.1:

```
TableException: This function does not support retraction.
  at ListAggFunction.retractExpressions(ListAggFunction.java:98)
  ... StreamExecLocalWindowAggregate.translateToPlanInternal
```

Hopping windows are sliced and merge per-slice accumulators, so distinct aggregates require
retraction-capable codegen; `ListAggFunction` has no `retractExpressions`. The `ONE_PHASE` aggregation
strategy does not help. Reproduced on the exact pin against a datagen source.

**Why this is worth an ADR.** The failure is narrow enough to look like a mistake: `COUNT(DISTINCT ...)`
in the *same query* plans fine, plain `LISTAGG` in the *same window* plans fine, and
`LISTAGG(DISTINCT ...)` under `TUMBLE` plans fine. A future editor will be tempted to "restore" the
`DISTINCT`. This record exists to stop that.

**Bonus.** Python-side normalisation also fixes a correctness problem SQL `DISTINCT` would not have:
`LISTAGG` concatenates in arrival order, and this pipeline deliberately injects out-of-order events, so
identical window contents could otherwise render different sentences and embed to different vectors.
Sorting makes the embedding input deterministic.
