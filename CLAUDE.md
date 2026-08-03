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
| Python (host) | **3.11** | Feast needs ≥3.10; 3.11 has the widest wheel coverage. Host has 3.10–3.14 installed; **use 3.11**. The host never runs PyFlink (see Conventions), so PyFlink's wheel matrix does not constrain this choice. |
| Python (container) | **3.12** | Forced: the Flink image is `eclipse-temurin:17-jre-noble` (Ubuntu 24.04), whose archive has **no python3.11 package** — only 3.12. apache-flink 2.2.1 supports cp312. Host and container interpreters need not match. |
| Flink | **2.2.1** | Latest is 2.3.0, but **no `flink-sql-connector-kafka` exists for 2.3 yet** (newest is `5.0.0-2.2`). Connector availability, not Flink version, sets the ceiling. |
| flink-sql-connector-kafka | **5.0.0-2.2** | `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar` |
| apache-flink (pip) | **2.2.1** | Must equal the cluster version exactly. ⚠️ **No Linux aarch64 wheel exists — for any version.** See Invariant 7. |
| Kafka | **apache/kafka:4.3.1** | KRaft mode, no Zookeeper. arm64 ✓ |
| Redis | **redis/redis-stack-server:7.4.0-v8** | arm64 ✓. Stack (not plain) for the optional RediSearch ANN index. |
| redis (pip client) | **>=4.2.2,<8** | Hard constraint from `feast[redis]`. Do not install redis-py 8.x. |
| feast | **0.65.0** | We import its private helpers — exact pin is load-bearing, see Invariant 2. |
| kafka-python | **3.0.9** | The 3.x line is maintained again; works with Kafka 4.x. |
| sentence-transformers | **5.6.1** | Model: `all-MiniLM-L6-v2`, 384-dim, ~90 MB. |

Fallback if Flink 2.x causes trouble: Flink **1.20.3** + connector **3.4.0-1.20** + `apache-flink==1.20.3`
(last 1.x LTS, most PyFlink examples target it). Record the switch in `docs/DECISIONS.md` if taken.
Note this fallback does **not** escape the aarch64 problem in Invariant 7 — no Flink version ever has.

## Invariants — violating these produces silent, hard-to-debug failure

1. **Feature names are hashed.** Redis field = `mmh3(f"{feature_view_name}:{feature_name}")`. If the Flink
   sink and the Feast registry disagree by one character, writes succeed and reads return `None` with no
   error anywhere. → Both sides import `contracts/schema.py`. Never hardcode a feature name twice.
2. **Never reimplement Feast's Redis codec.** Import `_redis_key` and `_mmh3` from
   `feast.infra.online_stores.helpers`, and `ValueProto` from `feast.protos.feast.types.Value_pb2`.
   The Flink image installs `feast[redis]` for exactly this reason.
3. **`entity_key_serialization_version: 3`** must match between `feature_store.yaml` and the sink's
   `_redis_key(...)` call. v2 and v3 encode entity keys differently; mismatch = permanent cache miss.
   3 is already the default in 0.65.0 (both `RepoConfig` and `_redis_key` default to it), so this guards
   against someone *explicitly* setting 2 — e.g. copying an old tutorial — not against omitting the key.
4. **Kafka has two listeners.** Containers use `kafka:9092`, host scripts use `localhost:29092`.
   Producer/consumer code on the host that uses `kafka:9092` will hang on metadata fetch, not error.
5. **Embeddings are computed on window close, not per event.** This is an architectural decision, not an
   optimisation — reverting it makes the pipeline ~1200× more expensive. See `docs/SCALING.md`.
6. **The model is baked into the Docker image** and runs with `HF_HUB_OFFLINE=1`. A Flink UDF must never
   hit the network on first call; that turns a cold start into a task-manager timeout.
7. **PyFlink has no Linux aarch64 wheel — in any version, ever.** Verified across 1.20.0 → 2.3.0: every
   Linux wheel is x86_64-only. On Apple Silicon the Flink container is `linux/arm64`, so
   `pip install apache-flink==2.2.1` falls back to the sdist. That sdist ships pre-generated `.c` files
   (7 of them; `coder_impl_fast.c` alone is 4.7 MB), so `setup.py`'s no-Cython branch still compiles C
   extensions — the pure-Python `extensions = []` path is unreachable. The JRE-only base has no compiler.
   → **The Dockerfile must `apt-get install build-essential python3.12-dev` before pip.** Expect a slow
   first build. Do not "fix" this by switching Flink versions; it is a platform constraint, not a pin.
8. **`registry.enable_online_feature_view_versioning` must stay at its default (`false`).** Feast resolves
   the FV name through `_versioned_fv_name()` *before* hashing, appending `_v{N}` inside both the mmh3
   field key and the `_ts:` key. Our sink hashes the raw name from `contracts/schema.py`. The trap: a
   fresh FV is v0 and unsuffixed, so everything works until the first schema change promotes it to v1 —
   after which reads silently return `None`.

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
make up            # bring up infra, wait for healthy (prints a health summary)
make health        # re-check: container status + kafka/redis/flink reachability
make topics        # create `transactions` with retention caps
make smoke         # end-to-end: produce → flink → redis → feast read
make test          # pytest, includes the codec round-trip
redis-cli -p 6380 --scan --pattern '*' | head         # raw Feast keys (binary) — note 6380
open http://localhost:8081                            # Flink UI: watch backpressure + watermarks
```

**Redis is on host port 6380, not 6379** — this machine runs a Homebrew redis on 6379 with real data in
it. Only the host mapping moved; inside the compose network it is still `redis:6379`, so the Flink sink
is unaffected while `feature_store.yaml` and host serving scripts need `localhost:6380`. Same shape as
Invariant 4. Override with `REDIS_HOST_PORT=6379`.

## Environment facts

- macOS 26.5.1, arm64. Docker 29.5.3 / Compose v5.1.4 — **daemon was NOT running** at setup time; start
  Docker Desktop before `make up`.
- Java 21 present (only needed if running Flink outside Docker — we don't).
- `uv` not installed; `poetry` is, but the plan uses plain venv + pip for lowest friction.
- Git repo on branch `main`; remote `origin` = `github.com/Tanishpd/petabyte-scale-feature-store` (private).

## Gotchas already paid for (don't rediscover)

- Feast 0.65's Redis online store has **no vector-search support**. `retrieve_online_documents` is
  implemented for Milvus, Qdrant, FAISS, pgvector, Elasticsearch, MongoDB and sqlite-vec — Redis is not
  among them. The embedding is stored as an `Array(Float32)` feature and retrieved by key. True ANN needs
  a parallel RediSearch index we maintain ourselves. Do not go looking for a Feast Redis vector config —
  it isn't there.
- **UDF workers need `PYTHONPATH=/opt/project`.** PyFlink cloudpickles UDFs and unpickles them in a
  *separate* Python worker process that does not inherit the driver's `sys.path`. A
  `sys.path.insert(...)` at the top of the job script is not enough. Symptom: the job submits cleanly,
  then sits in a restart loop with `ModuleNotFoundError: No module named 'src'` buried in the SDK-harness
  traceback. Set on both flink services in compose.
- **`COLLECT`/`MULTISET` cannot reach a Python UDF** — `Python UDF doesn't support logical type MULTISET`,
  and neither `CAST(... AS MAP<STRING,INT>)` nor `MAP_KEYS()` bridges it. Use `ARRAY_AGG(DISTINCT ...)`,
  which is equally compact and yields `ARRAY<STRING>`. See ADR-010.
- **Never benchmark a pipeline by `collect()`ing an `ARRAY<FLOAT>` to the client.** Shipping 4,403 × 384
  floats over gRPC/Arrow cost 225 s — ~10× the actual pipeline work — and made the embedding stage look
  25× slower than it is. Project to a scalar (`CARDINALITY(...)`) or sink to `blackhole` instead.
- **`SELECT COUNT(*)` in streaming mode emits a changelog** (1, 2, 3, …), so reading its first row gives
  1, not the total. Count rows in the same pass you are already making.
- **PyFlink API names differ from Java.** There is no `RichMapFunction` (use `MapFunction`, which already
  has `open()`/`close()` on its base `Function`), no `DiscardingSink` (removed in Flink 2.0, never
  wrapped for Python — and unnecessary, since a `map` runs without a terminal sink), and
  `to_data_stream` is a method on `StreamTableEnvironment`, not on `Table`.
- **`LISTAGG(DISTINCT ...)` cannot be used under a HOP window** — plan-time `TableException: This function
  does not support retraction`. `COUNT(DISTINCT)` in the same query is fine, as is `LISTAGG(DISTINCT)`
  under `TUMBLE`. Dedup in the Python UDF instead.
- **`FeatureView.ttl` does not expire Redis keys.** It bounds offline point-in-time joins only; the online
  read path never checks staleness and returns old values as `PRESENT`. Online expiry is
  `online_store.key_ttl_seconds`, and because our sink bypasses `online_write_batch`, the sink itself must
  issue `EXPIRE`.
- `torch-cpu` is not a package (404 on PyPI). CPU-only torch is `torch==<ver>+cpu` with
  `--extra-index-url https://download.pytorch.org/whl/cpu`; plain `torch` on Linux drags in multi-GB CUDA
  deps.
- `flink-sql-connector-kafka` versioning is `<connector>-<flink-minor>`; that suffix is what must match
  the cluster, and it lags Flink releases.
- HOP window output is append-only → `to_data_stream()` works without retraction handling. If the
  aggregation is ever changed to a non-windowed `GROUP BY`, the stream becomes a changelog and the sink
  must handle retract messages.
