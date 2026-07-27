# CLAUDE.md — agent working context

Context for Claude Code sessions on this repo. Read this before touching anything.
Human-facing overview lives in [README.md](README.md); the roadmap is [PLAN.md](PLAN.md).

## Project

Streaming feature pipeline: Kafka → Flink (windowed aggregates + sentence-transformer embeddings) →
Redis in Feast's native encoding → `feast.get_online_features()` for millisecond model reads.
Local, single-node, Docker-based. macOS arm64 (Apple Silicon).

## Version matrix — VERIFIED 2026-07-27, do not "upgrade" casually

| Component | Pin | Why this exact pin |
|-----------|-----|--------------------|
| Python | **3.11** | PyFlink 2.2.1 ships cp39–cp312 arm64 wheels; Feast needs ≥3.10; 3.11 has the widest torch/wheel coverage. Host has 3.10–3.14 installed; **use 3.11**. |
| Flink | **2.2.1** | Latest is 2.3.0, but **no `flink-sql-connector-kafka` exists for 2.3 yet** (newest is `5.0.0-2.2`). Connector availability, not Flink version, sets the ceiling. |
| flink-sql-connector-kafka | **5.0.0-2.2** | `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar` |
| apache-flink (pip) | **2.2.1** | Must equal the cluster version exactly. cp311 arm64 wheel confirmed present. |
| Kafka | **apache/kafka:4.3.1** | KRaft mode, no Zookeeper. arm64 ✓ |
| Redis | **redis/redis-stack-server:7.4.0-v8** | arm64 ✓. Stack (not plain) for the optional RediSearch ANN index. |
| redis (pip client) | **>=4.2.2,<8** | Hard constraint from `feast[redis]`. Do not install redis-py 8.x. |
| feast | **0.65.0** | We import its private helpers — exact pin is load-bearing, see Invariant 2. |
| kafka-python | **3.0.9** | The 3.x line is maintained again; works with Kafka 4.x. |
| sentence-transformers | **5.6.1** | Model: `all-MiniLM-L6-v2`, 384-dim, ~90 MB. |

Fallback if Flink 2.x causes trouble: Flink **1.20.3** + connector **3.4.0-1.20** + `apache-flink==1.20.3`
(last 1.x LTS, most PyFlink examples target it). Record the switch in `docs/DECISIONS.md` if taken.

## Invariants — violating these produces silent, hard-to-debug failure

1. **Feature names are hashed.** Redis field = `mmh3(f"{feature_view_name}:{feature_name}")`. If the Flink
   sink and the Feast registry disagree by one character, writes succeed and reads return `None` with no
   error anywhere. → Both sides import `contracts/schema.py`. Never hardcode a feature name twice.
2. **Never reimplement Feast's Redis codec.** Import `_redis_key` and `_mmh3` from
   `feast.infra.online_stores.helpers`, and `ValueProto` from `feast.protos.feast.types.Value_pb2`.
   The Flink image installs `feast[redis]` for exactly this reason.
3. **`entity_key_serialization_version: 3`** must match between `feature_store.yaml` and the sink's
   `_redis_key(...)` call. v2 and v3 encode entity keys differently; mismatch = permanent cache miss.
4. **Kafka has two listeners.** Containers use `kafka:9092`, host scripts use `localhost:29092`.
   Producer/consumer code on the host that uses `kafka:9092` will hang on metadata fetch, not error.
5. **Embeddings are computed on window close, not per event.** This is an architectural decision, not an
   optimisation — reverting it makes the pipeline ~1200× more expensive. See `docs/SCALING.md`.
6. **The model is baked into the Docker image** and runs with `HF_HUB_OFFLINE=1`. A Flink UDF must never
   hit the network on first call; that turns a cold start into a task-manager timeout.

## Layout (target)

```
docker/          docker-compose.yml, flink/Dockerfile
contracts/       schema.py            ← single source of truth for feature names/dtypes
src/producer/    generate_stream.py
src/flink/       streaming_job.py, udfs/embeddings.py, sinks/feast_redis_sink.py
src/feast_repo/  feature_store.yaml, definitions.py, data/*.parquet
src/serving/     online_read.py, benchmark.py
scripts/         bootstrap.sh, seed_offline.py, smoke_test.sh
tests/           test_feast_codec_roundtrip.py  ← the load-bearing test
docs/            ARCHITECTURE.md, DECISIONS.md, SCALING.md
Makefile
```

## Conventions

- **The Makefile is the interface.** Anything a human might run twice gets a target. Docs cite `make x`,
  never raw `docker compose ...`.
- Two separate dependency sets — host venv vs Flink image. Do not merge `requirements-host.txt` and
  `requirements-flink.txt`; they target different interpreters.
- The Flink job is submitted *into the container* (`flink run -py ...`), never run from the host venv.
  Host Python only ever runs the producer, Feast CLI, and serving scripts.
- Prefer Table API for the SQL-shaped work, DataStream only for the sink hop.
- Every doc claim about performance must come from a measurement in this repo, not from memory.

## Verify commands

```bash
make up            # bring up infra, wait for healthy
make smoke         # end-to-end: produce → flink → redis → feast read
make test          # pytest, includes the codec round-trip
docker compose -f docker/docker-compose.yml ps        # container health
redis-cli -p 6379 --scan --pattern '*' | head         # raw Feast keys (binary)
open http://localhost:8081                            # Flink UI: watch backpressure + watermarks
```

## Environment facts

- macOS 26.5.1, arm64. Docker 29.5.3 / Compose v5.1.4 — **daemon was NOT running** at setup time; start
  Docker Desktop before `make up`.
- Java 21 present (only needed if running Flink outside Docker — we don't).
- `uv` not installed; `poetry` is, but the plan uses plain venv + pip for lowest friction.
- Not a git repo yet. `git init` before the first substantial commit.

## Gotchas already paid for (don't rediscover)

- Feast 0.65's Redis online store has **no vector-search support** (`retrieve_online_documents` is
  implemented for Milvus/Qdrant/FAISS/pgvector/sqlite-vec, not Redis). The embedding is stored as an
  `Array(Float32)` feature and retrieved by key. True ANN needs a parallel RediSearch index we maintain
  ourselves. Do not go looking for a Feast Redis vector config — it isn't there.
- `flink-sql-connector-kafka` versioning is `<connector>-<flink-minor>`; that suffix is what must match
  the cluster, and it lags Flink releases.
- HOP window output is append-only → `to_data_stream()` works without retraction handling. If the
  aggregation is ever changed to a non-windowed `GROUP BY`, the stream becomes a changelog and the sink
  must handle retract messages.
