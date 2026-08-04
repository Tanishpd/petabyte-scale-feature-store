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

**Measured** (`make bench-embed`, 8-core container, MiniLM, 2 task slots):

| | rate |
|---|---|
| model, one sentence per `encode()` call | ⓜ 50–95 emb/s |
| model, batched `encode()` — what the pandas UDF does | ⓜ **450–475 emb/s** (5–7× faster) |
| embedding stage inside Flink, 2 slots | ⓜ ~250 emb/s aggregate |
| whole pipeline end-to-end (Kafka read + window + embed) | ⓜ 167 rows/s |
| cold model load, once per task slot in `open()` | ⓜ 0.2–0.3 s |

The 5–7× gap is the evidence for the pandas-UDF decision. Note also what *doesn't* help: throughput is
flat at 410–480 emb/s across batch sizes 32–512 and 1/4/8 torch threads, so MiniLM here is bound by
memory bandwidth, not by batch width or thread count.

At 167 rows/s on two slots, reaching §3's 830 emb/s target needs roughly 5× this hardware — which is the
honest scaling statement, and far better than the ~300 cores per-event embedding would demand.

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

## ADR-010 — Aggregate categories with `ARRAY_AGG(DISTINCT ...)`, sort in Python

**Decision.** `behavior_summary` comes from `ARRAY_AGG(DISTINCT merchant_category)` — an `ARRAY<STRING>` —
sorted in Python. The normalised string is what gets both stored as the feature and embedded.

**This ADR has been revised twice, each time by measurement.** Recorded in full because the three
candidates fail in different, non-obvious ways:

| candidate | verdict |
|---|---|
| `LISTAGG(DISTINCT ...)` | **Fails at plan time** under HOP: `This function does not support retraction`. Sliced windows force retraction-capable codegen, and `ListAggFunction` has no `retractExpressions`. |
| plain `LISTAGG(...)` | Works, but emits one entry per *transaction* — the busiest measured user had 2,084 transactions across 12 distinct categories, so a ~20 KB string was built only to be deduplicated away, and serialised to the embedding UDF on the way. |
| `COLLECT(...)` → `MULTISET` | Compact and plans fine, but **Python UDFs reject it**: `Python UDF doesn't support logical type MULTISET`. Neither `CAST(... AS MAP<STRING,INT>)` nor `MAP_KEYS(...)` bridges it. `MULTISET` also cannot cross Arrow, so `to_pandas()` fails with an opaque `collectAsPandasDataFrame` error. |
| **`ARRAY_AGG(DISTINCT ...)`** | **Chosen.** Compact (12 elements, not 2,084), and `ARRAY<STRING>` is a type Python UDFs accept. |

The instructive detail: `ARRAY_AGG(DISTINCT ...)` works under HOP where `LISTAGG(DISTINCT ...)` does not,
because `ArrayAggFunction` implements retraction and `ListAggFunction` does not. Same window, same
`DISTINCT`, opposite outcome. `COUNT(DISTINCT ...)` in the same query is fine throughout.

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
neither `LISTAGG`'s concatenation order nor a dict's key order is defined, and this pipeline deliberately
injects out-of-order events, so identical window contents could otherwise render different sentences and
embed to different vectors. Sorting makes the embedding input deterministic.

**Verified.** `make verify-windows` compares Flink against an independent pandas recomputation over 4,433
(user, window) pairs: `behavior_summary` matches on every row, and the rolling average agrees to 3.6e-12.

---

## ADR-011 — Checkpoints go to a shared volume, not the JobManager

**Decision.** `execution.checkpointing.dir` points at `file:///flink-state/checkpoints` on a Docker named
volume mounted into **both** the JobManager and the TaskManager. Externalized checkpoints are retained on
cancellation, three deep, with an exponential-delay restart strategy.

**Why it was a bug, not a nicety.** The compose file already set
`execution.checkpointing.interval: 30s` and `state.backend.type: rocksdb`, which *looks* like working
fault tolerance. It was not: with no checkpoint directory configured, storage falls back to the
JobManager, so in-flight window state did not survive a restart. Nothing warns about this — the job runs,
checkpoints "succeed", and the gap only shows up the first time something dies. It was found by being
asked whether the project was production-ready, not by any test.

**Two things that bite when fixing it:**

1. The directory must be reachable from both processes. The TaskManager writes the state data and the
   JobManager writes the checkpoint metadata, so a bind mount on one service alone silently half-works.
2. Docker seeds a **fresh named volume's ownership from the image's directory**. The image runs as
   `flink`, the volume is created root-owned, and the job dies at submission with
   `FlinkRuntimeException: Failed to create checkpoint storage at checkpoint coordinator side ... Failed
   to create directory for shared state`. The fix is `mkdir -p` + `chown flink:flink` in the Dockerfile,
   *before* the volume is first mounted — an existing root-owned volume must be removed, not just
   remounted.

**Verified by killing it.** `docker kill petafs-taskmanager` mid-job, then restart:

```
CheckpointCoordinator - Restoring job fc99819e... from Checkpoint 16
SourceCoordinator     - Recovering subtask 0 to checkpoint 16 for source
REST /checkpoints     - restored: {id: 16, external_path: file:/flink-state/checkpoints/.../chk-16}
```

Kafka source offsets recover with the operator state, so the job resumes rather than replaying. ⓜ 16
checkpoints completed before the kill, 778 KB state, 105 ms each.

**Production.** `file://` on a shared volume is correct for a single-node local stack and wrong for a
cluster — there, this becomes `s3://` or `gs://`, which is a one-line change to the same key.

---

## ADR-012 — A separate multi-host compose file, verified by isolated Compose projects

**Decision.** Multi-host deployment lives in `docker-compose.multihost.yml` as a third topology
alongside the dev and prod stacks, with one profile per node, and is verified locally by running the
three profiles as **three Compose projects with three separate Docker networks**.

**Why a separate file rather than an overlay of `docker-compose.prod.yml`.** The difference is not a
handful of patched values. Losing Compose DNS re-points four independent discovery mechanisms — Kafka
advertised listeners and the KRaft quorum, Redis `replicaof` plus Sentinel announcement, Flink's
JobManager/TaskManager RPC addressing, and TLS SANs. Three of the four fail *silently* when left at
their single-host settings: clients bootstrap and then hang, Sentinel publishes an address that does not
route, TaskManagers retry registration with nothing logged on the JobManager side. An overlay that
patched them piecemeal would read as a small delta over a file whose comments assert the opposite, and
those comments are load-bearing. See [MULTIHOST.md](MULTIHOST.md) §2.

**Why three Compose projects is a fair test.** Separate projects get separate networks, so there is no
shared DNS: a container on node B cannot resolve `kafka1`, exactly as it could not across machines.
Every cross-node connection leaves through a published port and returns over an address that resolves
off-node — which is the property that actually breaks multi-host deployments. Drill 0 asserts the
isolation before anything else runs; if the projects ever shared a network the suite would be measuring
a single-host stack, and a green run would be worse than a red one.

**What it does not test**: network partitions, cross-datacentre latency, hardware failure. Killing every
container in a project with `down -t 0` is SIGKILL with no grace period and leaves that node's volumes
intact — close to losing a machine, not identical to it.

**The controller listener changed.** `docker-compose.prod.yml` leaves KRaft's CONTROLLER as PLAINTEXT,
justified by it never leaving the Compose network. Across hosts that is no longer true, so here it is SSL
with `ssl.client.auth=required` — mTLS between controllers. This avoids the bootstrap-ordering problem
that ruled out SCRAM for the quorum, because certificates are files rather than credentials living in
the metadata log the quorum is still forming. Asserted by probe, not by config: a client with no
certificate gets `TLSV13_ALERT_CERTIFICATE_REQUIRED`.

**Deferred at the time: JobManager HA.** ~~a three-node ensemble plus a standby needs roughly 1.5 GiB
more than this 7.75 GiB Docker VM has~~ — **that estimate was wrong and ADR-013 supersedes it.** The
whole HA topology measures 5.4 GiB. The figure had been reasoned rather than measured; what actually
constrains this box is Kafka's retention setting, not the HA services. Node A remains the single
JobManager in *this* file, and drill 3 asserts the resubmission so it stays a procedure rather than a
surprise; `docker-compose.multihost-ha.yml` removes the need for it entirely.

**Verified.** `make mh-drill`, 24/24 from a cold start. Killing the whole node the job was executing on:
rescheduled onto a surviving node and restored from `s3://flink-checkpoints/petafs/<job>/chk-4`, no data
lost (48,985 → 60,389 records), features still being written (max TTL 1778 s of 1800). Killing node A:
Sentinel promoted the Redis master across hosts, Kafka degraded to ISR 2 on all 6 partitions and stayed
readable, and no split brain when A returned. The drill picks its victim by asking Flink where the job
actually is, so it has killed node B (job **and** Redis master together) and node C on different runs.

---

## ADR-013 — JobManager HA via ZooKeeper, and why the object store stays a named gap

**Decision.** `docker-compose.multihost-ha.yml` adds a three-node ZooKeeper ensemble and a second
JobManager to the multi-host topology, with the layout rearranged so the leading JobManager never shares
a node with the checkpoint store. Verified by `make mh-ha-drill`: **14/14**, run in both failover
directions.

**Supersedes ADR-012's rejection.** That ADR deferred HA on the grounds that an ensemble plus a standby
needed "roughly 1.5 GiB more than this 7.75 GiB Docker VM has". The figure was reasoned, not measured,
and it was wrong: the entire HA topology — 3 brokers, 3 ZooKeepers, 2 JobManagers, 2 TaskManagers, Redis
HA, MinIO — measures **5.4 GiB with the pipeline running**. The real constraint on this machine turned
out to be Kafka's *retention* setting, not the HA services: a broker's resident size is dominated by
mmap'd log segments, so an uncapped topic grew the three brokers from ~390 MiB to ~1.4 GiB each and
starved Flink into `Could not create actor system`. Capping retention fixed it and the second
TaskManager then registered in 30 s.

**No extra jar.** Flink 2.2.1 bundles shaded ZooKeeper 3 and Curator 5 inside `flink-dist-2.2.1.jar`. In
1.x these were a separate artifact to copy into `lib/`, which is what most surviving documentation says.

**Two settings that are silent traps:**

1. **`high-availability.jobmanager.port`.** In HA mode `jobmanager.rpc.port` is ignored and this one
   takes over — defaulting to `0`, a random port. The JobManager publishes an ephemeral port into
   ZooKeeper, the TaskManager dials a port Docker never published, and it retries forever with
   `Could not resolve ResourceManager address ...:42335`. On one host it works by accident.
2. **The leader is not "whichever REST answers 200".** Both JobManagers answer 200 in steady state with
   identical job lists, because a non-leader proxies to the leader. A 503 appears only during startup —
   long enough for a check written then to look right and never discriminate again. The authority is
   ZooKeeper: `/flink/<cluster-id>/leader/rest_server/connection_info`.

**Rejected: distributed MinIO.** It would close the durability half of the object-store gap and not the
other half. Flink takes a single `s3.endpoint`, so the endpoint itself must be highly available — a load
balancer or DNS failover in front of the cluster. That is precisely what a managed object store already
is; S3 and GCS hand you a regional endpoint with the availability solved. Swapping MinIO for either is a
credentials-and-URL change, already how `s3.endpoint` is wired. Building an LB tier on a laptop to
simulate something whose real answer is "use the managed service" would be theatre, so the object store
stays a **named** gap rather than a half-closed one.

**Also rejected: an overlay on docker-compose.multihost.yml.** HA needs a different node layout, and
Compose cannot express that as an overlay — `profiles` MERGES rather than replaces, so an overlay
declaring `profiles: [b]` on a service the base file put in `a` lands it in both. Verified with
`docker compose config`, not assumed. Same reasoning that produced a separate file in ADR-012.

---

## ADR-014 — Distributed object storage, and node-local S3 endpoints instead of a load balancer

**Decision.** The HA topology runs a MinIO member on every node, forming one erasure-coded pool, and
each Flink process points `s3.endpoint` at the member on **its own node**. There is no load-balancer
tier. Verified by `make mh-ha-drill`: **18/18**, including that the pool still accepts writes and Flink
still completes checkpoints after a whole node is killed.

**Supersedes ADR-013's rejection.** ADR-013 declined distributed MinIO on the reasoning that it would
close durability but not endpoint availability, since Flink takes a single `s3.endpoint` that would then
need an LB or DNS failover — "and building that on a laptop to simulate something whose real answer is
'use the managed service' would be theatre". The reasoning had a hole: it assumed the client and the
endpoint fail independently. They do not. If every Flink process uses the member co-located with it,
then the only event that removes an endpoint — losing its node — also removes the clients that were
using it. There is nothing left to orphan, so there is nothing for a load balancer to do. An LB tier
would only cover "MinIO died but its node did not", which is a process failure that
`restart: unless-stopped` already handles.

**Erasure sets must span nodes, and that is not automatic.** MinIO assigns drives to sets in flat
endpoint order. Three drives per node produced three sets of three, each set entirely on one node, so
killing a node destroyed a whole set and the cluster went to `503` — durable-looking configuration,
zero node tolerance. One drive per node forces the single set to span all three. Real deployments avoid
the trap by writing `http://minio-{a...c}:9000/d{1...N}`, where MinIO interleaves across hosts itself;
the host ellipsis needs identical ports on every node, which a one-machine simulation cannot provide.

**Three more requirements, each found by hitting it:** `hostname:` must be set, because MinIO decides
which endpoint is itself by resolving each one and checking the IP is on a local interface — pointing the
name at 127.0.0.1 is explicitly refused ("resolves to localhost in a containerized deployment"); the
listen port must equal the published port, because MinIO compares ports as well as addresses; and drives
must be real mounts, not directories on the container root filesystem.

**What this does not buy.** Parity 1 is the most a three-node pool offers: losing a node leaves no
redundancy until it returns, so a second failure in that window loses data. Four nodes at EC:2 is the
first comfortable configuration. A real deployment should still prefer S3/GCS — not for any property
demonstrated here, but because running an object store is operational work that an API call avoids.
`s3.endpoint` keeps that a credentials-and-URL change.

---

## ADR-015 — Credentials are mounted files, not environment variables

**Decision.** The HA topology takes every credential from a file mounted at `/run/secrets/<name>`.
`make secrets` generates them into `docker/secrets/` (gitignored, mode 600); nothing has a dev default
any more. Asserted by `make mh-ha-drill`: **34/34**, three of which are about the credentials themselves.

**Why files.** An environment variable holding a password is readable by anything that can ask the
Docker daemon about the container — `docker inspect`, `docker compose config`, `/proc/1/environ` — and
the middle one is exactly what people paste into bug reports. Removing the *dev defaults* matters at
least as much: `${REDIS_PASSWORD:-petafs-dev-secret}` means an unconfigured deployment comes up
working, with a password that is in the repository.

**The drill asserts the negative AND the positive.** Grepping every running container's `docker inspect`
output for each secret value, and `docker compose config` likewise, proves the credential is not
exposed. On its own that check would also pass against a stack where nothing was wired up at all, so it
is paired with an assertion that the file *is* present inside the container that needs it.

**Four different mechanisms, because four different programs:**

| | How it reads the secret |
|---|---|
| MinIO | `MINIO_ROOT_PASSWORD_FILE` — native `_FILE` support |
| Redis / Sentinel | the entrypoint already writes a config file; it now reads the password first |
| Kafka | `command` reads the files and exports the JAAS string before handing off to `/etc/kafka/docker/run` |
| Flink / producer | `contracts/secrets.py` resolves `${VAR}_FILE` before `${VAR}`; `AWS_SECRET_ACCESS_KEY` is exported by an entrypoint wrapper because Flink's S3 filesystem only reads the process environment |

**Scope, stated honestly.** This is done for `docker-compose.multihost-ha.yml` — the topology you would
actually deploy — and NOT for `docker-compose.prod.yml` or `docker-compose.multihost.yml`, which still
use environment variables with dev defaults. Those two are separately verified stacks (17/17 and 24/24),
and converting their credential plumbing without re-running both suites would mean shipping unverified
changes to working topologies. The mechanism is shared, so the conversion is mechanical when it happens;
what is not shared is the evidence.

**What it is not.** Nothing here rotates, expires, audits or seals anything. The file sits on a host
filesystem and any process in the container can read it. It is the *interface* a real secret manager
plugs into: point the same paths at a Kubernetes projected secret or a Vault agent's tmpfs and no
service configuration changes. `make secrets-rotate` exists but is deliberately blunt — Redis persists
its password into its own config file on first boot, so rotating means dropping the Redis volumes too.

---

## ADR-016 — Authentication is not authorization: per-component identities on Kafka and Redis

**Decision.** Every component gets its own credential and a rule set naming exactly what it may do.
Kafka runs `StandardAuthorizer` with `allow.everyone.if.no.acl.found=false`; Redis runs with
`user default off` and one ACL user per component. Asserted by `make mh-ha-drill`: **34/34**,
thirteen of them about authorization specifically.

**The gap this closes.** ADR-015 made credentials unreadable through the orchestrator, and the earlier
TLS/SASL work made them unforgeable on the wire. Neither did anything about *scope*. Before this, the
whole topology shared two secrets — one Kafka password, one Redis password — and each was effectively
root:

- Anything that could produce to `transactions` could also **delete** it.
- Anything that could write a feature could `FLUSHALL` the online store, and Redis recorded nothing
  about which client did it.

That is a lock on a door with no rooms behind it. Authentication answers *who are you*; without
authorization the answer does not constrain anything.

**Kafka.** Four principals — `petafs-broker` (inter-broker), `petafs-admin` (operator, a super user),
`petafs-producer`, `petafs-flink` — each with its own password file. The rules are one per thing a
principal actually does: the producer may `Write` and `Describe` the topic; Flink may `Read` and
`Describe` the topic *and its own consumer group*. A group ACL of `*` was rejected deliberately — it
would let any consumer hijack another's committed offsets.

The controller listener's mTLS principals (`CN=kafka1…3`) are super users. They have to be: KRaft's
`Vote`/`BeginQuorumEpoch`/metadata-log RPCs are `CLUSTER_ACTION`, and a controller that cannot
authorize them cannot elect a leader.

**Redis.** Five users, in `docker/redis/write-acl.sh`, written by **both** data nodes because roles
swap: after a failover the promoted replica must already know every user, and the demoted master must
be able to authenticate *outward* as `petafs-repl`. A per-role rule set is correct until the first
failover and wrong from then on — the one moment the topology exists to survive.

| user | may do | notably may not |
|------|--------|-----------------|
| `default` | **off** | anything — this is the point |
| `petafs-admin` | `+@all` | — (break-glass; what the runbook uses) |
| `petafs-flink` | `HGET` `HSET` `EXPIRE` | `SCAN`, `DEL`, `FLUSHALL`, `CONFIG` |
| `petafs-reader` | hash reads | any write |
| `petafs-repl` | `PSYNC` `REPLCONF` `PING` | everything else |
| `petafs-sentinel` | the set from the Sentinel docs | read or write any key |

`petafs-flink`'s three commands are not a category grant; they are what the sink actually issues —
`HGET` for the staleness guard, `HSET`, `EXPIRE`. `~*` on the key pattern is honest rather than lazy:
Feast's Redis keys are **binary** mmh3 hashes, so no printable prefix exists for a glob to match. The
restriction carrying weight here is the command list.

**Sentinel needs two different credentials** and conflating them breaks failover silently. `requirepass`
on the Sentinel is what a *client* presents to talk to it; `sentinel auth-user`/`auth-pass` is the far
narrower identity Sentinel presents to the monitored instances. Likewise a replica needs `masteruser`,
not just `masterauth` — `masterauth` alone authenticates as `default`, which is now off, so a replica
configured the old way fails to sync with `NOAUTH` and Sentinel never sees it return.

**Verified by probing, not by reading config.** Listing the ACLs back only proves the file says what we
wrote in it. The drill asserts each boundary by attempting the operation with the real credential —
and asserts the *positive* control first, because a rule set that denies everything passes every
negative assertion while leaving the pipeline dead.

**What it is not.** These are static rules in files on disk; nothing rotates or audits. There is no
per-client-IP restriction (`host=*` throughout), and the dev stack is deliberately untouched — it keeps
one `requirepass`, because the point of the dev stack is to be startable.
