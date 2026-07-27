# Architecture Decision Records

Each record states the decision, the evidence behind it, and what we rejected. Evidence gathered
2026-07-27 against live package indexes — not from memory.

---

## ADR-001 — Target Python 3.11

**Decision.** All host and container Python is 3.11.

**Evidence.** `apache-flink` 2.2.1 publishes `macosx_11_0_arm64` wheels for cp39/cp310/cp311/cp312 only.
`feast` 0.65.0 requires `>=3.10.0`. `sentence-transformers` 5.6.1 requires `>=3.10`. The intersection is
3.10–3.12; 3.11 sits in the middle with the broadest third-party wheel coverage (notably torch).

**Rejected.** 3.13/3.14 (present on this machine, but no PyFlink wheels). 3.10 (works, but narrower
ecosystem support than 3.11 for no benefit).

---

## ADR-002 — Flink 2.2.1, not the latest 2.3.0

**Decision.** Pin the cluster and the `apache-flink` pip package to 2.2.1.

**Evidence.** Flink 2.3.0 is released and has arm64 images, but the newest published
`flink-sql-connector-kafka` on Maven Central is `5.0.0-2.2` — built for Flink 2.2. Connector artifacts
are versioned `<connector-version>-<flink-minor>` and lag Flink releases. Since the Kafka connector is
non-optional here, connector availability sets the version ceiling, not Flink's release page.

**Consequence.** When `*-2.3` ships, upgrading is a two-line change (image tag + pip pin + JAR URL).

**Fallback.** If PyFlink 2.x proves awkward, drop to Flink 1.20.3 + connector 3.4.0-1.20 — the last 1.x
LTS, and the version nearly all PyFlink documentation targets. Amend this ADR if taken.

---

## ADR-003 — Flink writes to Redis directly, using Feast's own encoder

**Decision.** The Flink sink opens a Redis connection and writes Feast-encoded hashes itself. It
**imports** `_redis_key` and `_mmh3` from `feast.infra.online_stores.helpers` and `ValueProto` from
`feast.protos.feast.types.Value_pb2` rather than reimplementing the format.

**Evidence.** Reading `feast/infra/online_stores/redis.py` at v0.65.0, `online_write_batch` builds each
record as:

```python
redis_key_bin = _redis_key(project, entity_key, entity_key_serialization_version=...)
ts_key        = f"_ts:{feature_view}"
entity_hset   = {ts_key: Timestamp_proto_bytes}
for feature_name, val in values.items():
    entity_hset[_mmh3(f"{feature_view}:{feature_name}")] = val.SerializeToString()
pipe.hset(redis_key_bin, mapping=entity_hset)
```

That is a small, fully legible format — but it is *internal* and has already changed across versions
(entity-key serialization is on v3, with an explicit v2→v3 re-serializer in the codebase).

**Why import instead of reimplement.** A hand-rolled codec is ~40 lines and would work today. It would
also break silently on upgrade, and "silently" is the operative word: a wrong hash means writes succeed
and reads return `None`, with no exception on either side. Importing makes correctness structural.

**Rejected alternative:** Flink → a second Kafka topic → a Python service calling
`store.write_to_online_store()`. This is the more conventional Feast pattern and is arguably safer, but
it adds a network hop and a service to the hot path, and the brief explicitly calls for Flink to stream
features *directly* into Redis. The round-trip test (see ADR-005) buys back the safety.

**Guardrail.** `feast==0.65.0` is pinned exactly, and the round-trip test fails loudly if the internal
format shifts.

---

## ADR-004 — The embedding is a stored feature, not a Feast vector index

**Decision.** Store the 384-dim vector as an `Array(Float32)` field on the feature view, retrieved by
entity key. Any true nearest-neighbour search is served by a **separate RediSearch HNSW index** that our
sink maintains alongside the Feast hash.

**Evidence.** Feast 0.65.0's Redis online store implements no vector search — grepping `redis.py` for
`vector`, `knn`, `retrieve_online_documents` returns nothing. Feast's document-retrieval API is
implemented for Milvus, Qdrant, FAISS, pgvector, Elasticsearch and sqlite-vec; Redis is not among them.

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
1-minute slide needs ~830/s. That is a ~1200× reduction with **no loss of freshness for the consumer**,
because the model reads the window aggregate anyway.

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
