# Petabyte-Scale Feature Store & Streaming Pipeline

A real-time feature pipeline: high-velocity events in, model-ready numeric features **and vector
embeddings** out, served from a feature store in single-digit milliseconds.

> **Status: built and measured.** All 8 phases complete — `make smoke` runs the whole path end to end.
> Every performance number below is measured in this repo, not estimated. See [PLAN.md](PLAN.md).

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
`EPS=20000` that's roughly 19 GB/hour, which is why the `transactions` topic gets retention caps at
creation.

```bash
make bootstrap     # python3.11 venv + pinned deps
make build         # custom Flink image (~5 min: compiles PyFlink, bakes the model)
make up            # Kafka + Flink + Redis, waits until healthy
make seed          # generate the offline parquet the FeatureView needs
make apply         # register the feature definitions with Feast
make smoke         # THE demo: produce -> flink -> redis -> feast read, end to end
```

Then:

```bash
make read          # a "model" pulls features for a user — the payoff
make similar       # find behaviourally similar users (RediSearch KNN)
make bench         # p50/p95/p99 read latency + Redis footprint
make verify-windows  # prove the aggregates match an independent pandas recomputation
make test          # 53 tests, including the Feast codec round-trip
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

**`src/serving/similar_users.py`** — Nearest-neighbour search over the embeddings. Feast 0.65 has no
vector search on Redis, so the sink maintains a parallel RediSearch HNSW index and this queries it —
the one retrieval path Feast can't serve. `make similar USER=1`.

**`src/serving/online_read.py`** — What a production model server would actually contain:

```python
store.get_online_features(
    features=["user_txn_stats_10m:txn_amount_avg_10m", "user_txn_stats_10m:embedding"],
    entity_rows=[{"user_id": 42}],
).to_dict()
```

---

## Measured

Every number here comes from `make bench`, `make bench-embed` or `make verify-windows` on one laptop
(8-core Apple Silicon, Docker with 8 GB), not from estimates.

| | |
|---|---|
| `get_online_features`, 1 entity | **p99 2.17 ms** (0.19 ms Redis + 0.74 ms Feast) |
| `get_online_features`, 100 entities | **p99 22.55 ms** — 6× cheaper per entity than one-at-a-time |
| Redis footprint per user | 4,073 B → ~204 GB for 50M users |
| Pipeline ceiling | **~8,500 eps** sustained (zero lag at 5k; 2.9M lag at 27k) |
| Producer throughput | 4,998 eps against a 5,000 target; ceiling ~30k single-threaded |
| Embedding, batched vs not | 450–475 vs 50–95 emb/s — **5–7× faster batched** |
| Aggregate correctness | 4,433 (user, window) pairs match pandas to 3.6e-12 |
| Read-path caveat | a long-lived `FeatureStore` drifts 2.7× over 200 calls — refresh the registry ([SCALING §8](docs/SCALING.md)) |

## Two design decisions worth knowing

**Embeddings are computed when a window closes, not per event.** A naive design embeds every transaction.
At 1M events/s that's 1M embeddings/s — impossible on CPU, expensive on GPU. Embedding on window close
instead means one vector per *active user per minute*: with 50k active users, ~830/s. Same freshness for
the model, ~1200× less compute. This is the load-bearing idea in the whole design.

**The online store is small; the offline store is the petabyte.** Raw events at 1M/s are ~22 TB/day and
cross a petabyte in about six weeks. But the *serving* layer only holds current state — 50M users ×
4,073 B ≈ 204 GB, which fits comfortably in a modest Redis cluster. Understanding that split is what makes
"petabyte-scale" tractable rather than terrifying. (That per-user figure is measured; the first estimate
of 1.8 KB was 2.3× optimistic.) The full argument is in [docs/SCALING.md](docs/SCALING.md).

---

## Is this production ready?

Four stacks, each one closing what the previous could not. `docker-compose.yml`
is the laptop demo. **`docker-compose.prod.yml`** is a real topology —
`make prod-up`, and `make prod-drill` kills things to prove the claims below.
**`docker-compose.multihost.yml`** spreads that across three independent nodes
(`make mh-up`, `make mh-drill` kills a whole node). **`docker-compose.multihost-ha.yml`**
adds a ZooKeeper ensemble and a standby JobManager, so losing the node running
the pipeline no longer stops the pipeline (`make mh-ha-up`, `make mh-ha-drill`).
Both are in [docs/MULTIHOST.md](docs/MULTIHOST.md).

**Closed, each verified by breaking it** (drills in `scripts/failure_drills.sh`):

| Gap | Now | How it was proven |
|---|---|---|
| Single broker, RF=1 | 3 brokers, RF=3, `min.insync.replicas=2` | Killed a broker: ISR 3→2, **zero data loss**, writes continued at `acks=all`, broker rejoined |
| State lost on restart | Checkpoints to object storage (MinIO → S3) | Killed the TaskManager: restored from `s3://.../chk-N`, Kafka offsets recovered with it |
| Single Redis | primary + replica + Sentinel; the sink discovers the master | Killed the master: promotion in ~9 s, 8,018 keys intact, writes resumed |
| Nothing authenticated | SASL/PLAIN on Kafka; `requirepass` on Redis nodes *and* Sentinel | Authenticated clients work, unauthenticated are refused — on both |
| Nothing watched it | Prometheus + 5 alerts on lag, checkpoints, restarts, backpressure, feature staleness | 2/2 scrape targets up, rules loaded |
| Tests never ran | CI on every push, **fails on any skip** | A skipped round-trip test is the same as no test |
| Sentinel could split-brain | 3 sentinels, quorum 2, config-file Redis so promotions persist | Killed the master, restarted the old one: it rejoins as a replica, exactly one master, all 3 sentinels agree |
| Everything in the clear | **TLS everywhere**: Kafka SASL_SSL, Redis + Sentinel TLS-only (`port 0`), replication encrypted | An unencrypted client *with valid credentials* is refused on both; the whole path producer→Kafka→Flink→Redis is encrypted |
| Nothing proved any of it | `make prod-drill` — 17 assertions | **17/17 passing** against the live topology |
| Credentials in env vars with dev defaults | **Mounted files, no defaults** — `make secrets`, and the code resolves `${VAR}_FILE` first | Grepped every container's `docker inspect` and `docker compose config` for each secret: **absent**, while the file *is* present inside the container that needs it |
| **Everything on one host** | **Three nodes, separate failure domains** — routable addressing throughout, mTLS on the KRaft quorum | Killed the whole node running the job: rescheduled onto another node, restored from `s3://.../chk-4`, zero data loss, features still writing. Killed the Redis primary's node: promoted across hosts. **24/24** ([MULTIHOST](docs/MULTIHOST.md)) |
| **The JobManager and the object store were single points of failure** | **ZooKeeper ensemble + standby JobManager + object store distributed across all three nodes**, each Flink process using its node-local S3 endpoint | Killed the whole node holding the *leading* JobManager: leadership moved automatically, the pipeline recovered **with no resubmission and the same job id**, restored from `s3://.../chk-2`, the pool **kept accepting writes** at 2 of 3 drives, and checkpoints kept completing into it. **18/18** |

**Still open, honestly:**

- **Only one node may be lost.** Three of anything tolerates one failure. The
  object store in particular runs at parity 1 — the most a three-node pool can
  offer — so while a node is down there is no redundancy left. Four nodes at
  EC:2 is the first configuration that is comfortable rather than merely correct.
- **A real deployment should still use S3/GCS rather than self-run MinIO** — not
  for anything demonstrated here, but because running an object store is
  operational work an API call avoids. `s3.endpoint` keeps that a
  credentials-and-URL change.
- **Two of the four stacks still use env-var credentials.** The HA topology
  takes every credential from a mounted file with no dev default, asserted by
  drill (`docker inspect` and `docker compose config` contain none of them).
  `docker-compose.prod.yml` and `docker-compose.multihost.yml` have not been
  converted — they are separately verified stacks, and changing their credential
  plumbing without re-running both drill suites would ship unverified changes
  ([ADR-015](docs/DECISIONS.md)).
- **File-based secrets are not a secret manager.** Nothing rotates, expires or
  audits; the file sits on a host filesystem. It is the interface one plugs
  into — point the paths at a K8s projected secret or a Vault agent and no
  service config changes.
- **Feast internals dependency** ([ADR-003](docs/DECISIONS.md)). CI is the
  tripwire; a supported path would be `write_to_online_store`, at the cost of a hop.
- **One unexplained behaviour**: a long-lived `FeatureStore` drifts 2.7× over 200
  calls. Mitigated by refreshing the registry, root cause unknown
  ([SCALING §8](docs/SCALING.md)).

So: **durability, availability, authentication, transport encryption, deployment
topology, control-plane failover and object storage are all real and tested** —
62 assertions across the three topologies, every one of them made by breaking
something. **No single node's loss stops the pipeline**: Kafka, Redis,
ZooKeeper, the JobManager and the object store each survive one, and the drill
proves it by killing whichever node ZooKeeper currently says is in charge.

The multi-host claims are verified by running the three node profiles as three
Compose projects on **separate Docker networks**, so there is no shared DNS and
every address has to be routable — the property that actually breaks multi-host
deployments. The first drill asserts that isolation before any other, because a
suite that quietly measured a single-host stack would be worse than one that
failed. It does not simulate network partitions or hardware failure; that
boundary is drawn explicitly in [docs/MULTIHOST.md](docs/MULTIHOST.md) §3.

## Documentation

| Doc | What's in it |
|---|---|
| [PLAN.md](PLAN.md) | Phased build plan with acceptance criteria and risk register |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Data flow, schemas, how the Feast/Redis encoding works |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Why these versions, why direct-write, what was rejected |
| [docs/SCALING.md](docs/SCALING.md) | The petabyte math, bottleneck ordering, what breaks at each 10× |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Production topology: drills, sizing, upgrades, alert response |
| [docs/MULTIHOST.md](docs/MULTIHOST.md) | Three-node deployment: what changes without Compose DNS, and the host-loss drills |
| [CLAUDE.md](CLAUDE.md) | Working context for AI coding sessions |

## Troubleshooting

| Symptom | Cause |
|---|---|
| Producer hangs, no error | Using `kafka:9092` from the host. Host scripts must use `localhost:29092`. |
| `get_online_features` returns all `None` | Feature-view or feature name drift between the Flink sink and the Feast registry — the Redis field is a hash of both. Check `contracts/schema.py` is the only definition. |
| Flink job fails on submit | Version skew between the cluster image and the `apache-flink` pip package. Both must be 2.2.1. |
| First `make build` takes a few minutes | It builds a 3.48 GB image with PyTorch, bakes the model, and — on Apple Silicon — compiles PyFlink from source since no Linux aarch64 wheel exists. Measured ~5 min cold. Once only. |
| Image build fails compiling PyFlink | Missing `build-essential` / `python3.12-dev` in the Flink Dockerfile. See [ADR-009](docs/DECISIONS.md). |
| `make up` fails: port 6379 in use | Expected if you run a system/Homebrew redis. The stack uses **host port 6380** by default and leaves 6379 alone. `REDIS_HOST_PORT=6379 make up` to override. |
| Restarting Docker Desktop mid-build | Kills the running build. Completed layers are cached, so re-running `make build` resumes rather than starting over. |
| `Cannot connect to the Docker daemon` | Docker Desktop isn't running. |
| `Failed to create directory for shared state` | The `flink-state` volume is root-owned from before the Dockerfile created it. `make clean` (drops volumes), then `make build && make up`. |
| Edited a UDF/sink but the job behaves as before | PyFlink reuses Python worker processes across job submissions, so an edited module may not be reloaded. `docker compose restart taskmanager` after changing Python that runs inside Flink. |
| Only some users appear in the ANN index | Was a race between task slots on `FT.CREATE`; fixed. If it recurs, check `ensure_index` still treats "index already exists" as success rather than disabling indexing for that slot. |
