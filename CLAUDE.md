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
docker/          docker-compose.yml, docker-compose.prod.yml, flink/Dockerfile,
                 docker-compose.multihost.yml     ← 3 nodes; multihost/sim.env
                 docker-compose.multihost-ha.yml  ← + ZK ensemble & standby JM
                 tls/generate-certs.sh, observability/
                 redis/write-acl.sh               ← ACL users, shared by prod/mh/ha
                 observability/  prometheus*.yml, alerts.yml, alerts_test.yml,
                                 alertmanager.yml, webhook_sink.py, grafana/
contracts/       schema.py    ← single source of truth for feature names/dtypes
                 secrets.py   ← ${VAR}_FILE beats ${VAR}; used by sink + producer
src/producer/    generate_stream.py
src/flink/       streaming_job.py, udfs/embeddings.py, sinks/feast_redis_sink.py
                 dlq.py           ← --on-bad-record dlq: Flink plumbing
                 event_parser.py  ← the routing decision; NO pyflink import, so CI covers it
src/feast_repo/  feature_store.yaml, definitions.py, data/*.parquet
src/serving/     online_read.py, benchmark.py
scripts/         bootstrap.sh, seed_offline.py, smoke_test.sh, failure_drills.sh,
                 multihost_health.sh, multihost_drill.sh,
                 multihost_ha_health.sh, multihost_ha_drill.sh,
                 gen_secrets.sh, alerts_verify.sh, backup.sh, restore.sh
tests/           test_feast_codec_roundtrip.py  ← the load-bearing test
docs/            ARCHITECTURE.md, DECISIONS.md, SCALING.md, RUNBOOK.md, MULTIHOST.md
docker/secrets/  generated credentials (gitignored) — `make secrets`
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

## CI

`.github/workflows/tests.yml` runs all 89 tests on push/PR with a Redis Stack service and an applied
registry — no Flink needed. It **fails the build if any test is skipped**: every test here has an infra
guard that degrades to a skip, so a broken service container would otherwise produce a green run that
asserted nothing. That matters most for the codec round-trip, which is the only tripwire on the Feast
private-API dependency (ADR-003).

## Verify commands

```bash
make up            # bring up infra, wait for healthy (prints a health summary)
make health        # re-check: container status + kafka/redis/flink reachability
make topics        # create `transactions` with retention caps
make smoke         # end-to-end: produce → flink → redis → feast read
make test          # pytest, includes the codec round-trip
make mh-tls && make mh-up   # 3-node topology as 3 isolated compose projects
make mh-drill      # kill a WHOLE node; 24 assertions. See docs/MULTIHOST.md
make secrets       # generate docker/secrets/* — REQUIRED before mh-ha-up
make mh-ha-up && make mh-ha-drill   # + ZK/standby JM; 34 assertions (MULTIHOST §5)
make alerts-test   # promtool: every alert rule fires on its condition (no stack needed)
make alerts-verify # metrics exist AND alerts reach the receiver (needs a stack)
make backup        # registry.db + offline parquet — the only unrebuildable state
make restore ARCHIVE=backups/...   # verifies checksums BEFORE writing anything
make prod-topics   # prod's `transactions` (auto-create is off there)
# ⚠️ mh and mh-ha must not run at once, and neither alongside the dev stack.
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
- **A long-lived `FeatureStore` gets slower.** `get_online_features()` drifts 2.7× over ~200 calls
  (6.9 → 18.4 ms for 100 entities). Calling `store.get_feature_view(...)` periodically removes the drift
  entirely (1.1×); so does constructing a new `FeatureStore`. Both touch the registry. Ruled out:
  elapsed time, connection leak, object leak, Prometheus metrics, growing projection. Any benchmark that
  reuses one store across thousands of calls is measuring this, not the read path. See SCALING §8.
- **`TableEnvironment.create()` cannot reach the DataStream API.** It returns a plain
  `TableEnvironment` with no `to_data_stream`, so the sink hop is unreachable. Build
  `StreamTableEnvironment.create(StreamExecutionEnvironment.get_execution_environment())` instead, and
  execute DataStream jobs with `stream_env.execute_async(name)` — not `t_env.execute(...)`.
- **A `UnixTimestamp` feature is written as int64 seconds and read back as a `datetime`.** Same instant,
  different Python type; comparing the raw int to the read value looks like a mismatch when it isn't.
- **`execution.checkpointing.interval` alone is not fault tolerance.** Without
  `execution.checkpointing.dir`, storage falls back to the JobManager and state does not survive a
  restart — with no warning. The directory must be mounted into *both* flink services, and because
  Docker seeds a new named volume's ownership from the image, the Dockerfile has to `mkdir` + `chown
  flink:flink` it or the job dies with "Failed to create directory for shared state". See ADR-011.
- **PyFlink caches Python modules across job submissions.** Worker processes are reused within a
  TaskManager, so cancelling and resubmitting a job does *not* reload edited UDF/sink code — the old
  module keeps running and the change appears to have no effect. `docker compose restart taskmanager`
  after editing any Python that executes inside Flink. Cost me a debugging round chasing a bug I had
  already fixed.
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

### Multi-host (docs/MULTIHOST.md)

- **Docker Desktop containers cannot reach this Mac's LAN IP** (192.168.x.x times out) — only
  `host.docker.internal`, which in turn does **not** resolve on the macOS host. So container-side clients
  use `host.docker.internal:<port>` and host-side scripts need a separate Kafka listener advertised as
  `localhost:<port>`. Same shape as Invariant 4, one level out.
- **`blob.server.port` has no bind-port counterpart.** `taskmanager.rpc.port`/`.bind-port` and
  `taskmanager.data.port`/`.bind-port` let a TaskManager bind one port and advertise another, which is
  what lets two of them run on one machine. The blob server advertises whatever it binds, so the
  JobManager's ports must be published 1:1 or TaskManagers fail to fetch job artifacts.
- **ZooKeeper HA needs no extra jar in Flink 2.x.** `flink-dist-2.2.1.jar` bundles shaded ZooKeeper 3 and
  Curator 5 (in 1.x these were a separate jar to copy into `lib/`). Not enabled — it does not fit in the
  7.75 GiB Docker VM alongside everything else.
- **The Kafka CLI answers an unauthenticated call with silence, not an error.** Drills that recreate a
  node give it a fresh `/tmp`, so a credentials file written once at startup vanishes and
  `kafka-get-offsets.sh` starts returning empty — a topic with 163k records reads as 0. Rewrite the
  properties file on every call.
- **Feast's Redis keys are binary.** Any probe that scans keys out to the shell and passes them back
  (`TTL`, `HGET`, …) mangles every one and gets `-2`/nil, which looks like "the pipeline stopped" rather
  than "the probe is broken". Use a Lua `EVAL` so keys never leave Redis.
- **A master has no `master_link_status`.** Never hardcode which container is the replica — roles swap on
  failover and the field simply disappears, reporting a healthy link as absent.
- **A dead broker lingers in cluster metadata** until its session expires, so a broker count taken right
  after a kill still reads the old number. Assert ISR shrank, not that the count dropped.

### JobManager HA (docker-compose.multihost-ha.yml)

- **In HA mode `jobmanager.rpc.port` is IGNORED.** `high-availability.jobmanager.port` takes over and
  defaults to `0` — a random port, published into ZooKeeper, that Docker never mapped. Symptom is the
  TaskManager looping on `Could not resolve ResourceManager address pekko.tcp://...:42335`. Works by
  accident on one host because nothing is port-mapped there.
- **The leader is NOT "whichever REST answers 200".** Both JobManagers answer 200 with identical job
  lists — a non-leader proxies to the leader. 503 only appears during startup, so a check written then
  looks right and silently stops discriminating. Read
  `/flink/<cluster-id>/leader/rest_server/connection_info` from ZooKeeper instead.
- **Kafka RSS is mmap'd log segments, not `KAFKA_HEAP_OPTS`.** With heap pinned at 320m and default 2 GiB
  retention, three brokers went from ~390 MiB to ~1.4 GiB each and starved Flink into `Could not create
  actor system` / `NoClassDefFoundError` on pekko classes. Cap `retention.bytes` + `segment.bytes` on
  memory-tight boxes. Do NOT conclude "it doesn't fit" from Flink's error text — check the brokers.
- **ZooKeeper HA needs no extra jar in Flink 2.x** (shaded ZK3 + Curator5 are inside `flink-dist`), but
  it DOES need `high-availability.storageDir` on storage that outlives the leader's node — ZooKeeper
  holds only pointers; the job graph (`submittedExecutionPlan`) lives in the HA store.
- **`docker compose down -v` can silently skip the volumes** if a container was mid-restart, while still
  reporting success. Stale Kafka logs then survive and `--if-not-exists` keeps the old topic, so a
  retention change looks ignored. Sweep by name and verify.
- **Credentials in the HA topology are MOUNTED FILES, not env vars** (`make secrets`). Anything reading
  a password must go through `contracts/secrets.py` (`${VAR}_FILE` beats `${VAR}`). `tls_storepass` is
  baked into the JKS keystores, so changing it means `make mh-tls`; Redis writes its password into its
  own config file on first boot, so rotating means dropping the Redis volumes too.
- **Compose `profiles` MERGE across `-f` files, they do not replace.** An overlay cannot move a service
  to a different node — it ends up in both profiles. That is why HA is a separate file, not an overlay.
- **ZooKeeper tells clients which port to use and they believe it over their own config.** Curator
  replaces its connection string with the ensemble's advertised addresses, which come from the
  `;clientPort` in `server.N=host:2888:3888;2181`. Bind 2181 and publish 12181 → every client learns
  `:2181`, a port Docker never mapped, and loops on `Connection refused`. Advertised must EQUAL
  published (`ZOO_PORT`). ZK 3.5+ also **persists this into its data dir**, so changing it needs the ZK
  volumes dropped, not just a container recreate.
- **`zookeeper.sasl.disable: true` is required on every Flink service.** Flink writes a JAAS file for
  Kafka; ZooKeeper's client finds it, has no `Client` section, and Curator then logs
  `Authentication failed` and re-connects several times a second. Idle boxes survive it; under load the
  churn stops a JobManager ever holding the leader latch and failover silently never happens.
- **A Kafka broker's RSS grows for as long as it runs** — ~390 MiB → 2.2 GiB over hours here, with the
  heap pinned at 320m. Flink then dies with `Could not create actor system`, naming nothing about
  memory. A rolling `docker restart` of the brokers took the VM 7,377 → 3,717 MiB. Restart the brokers
  before concluding the topology does not fit.
- **MinIO assigns erasure sets in flat endpoint order.** 3 drives × 3 nodes = three sets each living
  entirely on ONE node, so losing a node destroys a set (cluster → 503). Use ONE drive per node so the
  single set spans them. Real hosts use `http://minio-{a...c}:9000/d{1...N}` and MinIO interleaves
  itself, but the host ellipsis needs identical ports everywhere. MinIO also needs `hostname:` set (it
  refuses a name resolving to 127.0.0.1), listen port == published port, and real volume mounts.

### Observability and drill hygiene (ADR-017)

- **`sum(rate(x[10m])) == 0` does NOT fire when `x` disappears.** A dead TaskManager's metrics go
  *stale*, not to zero; `sum()` over nothing is an empty vector and `empty == 0` is empty, not true. The
  `NoFeaturesWritten` alert — the one covering the failure this design fears most — was therefore silent
  in the most likely way for the pipeline to stop writing. Needs `or absent(x)`.
- **"Rules loaded" is not "alerts work."** Same configured-vs-enforced trap as everywhere else here.
  `make alerts-test` replays synthetic series through the real rules; `make alerts-verify` asserts the
  metrics those rules name actually exist. Neither alone is evidence: promtool cannot catch a metric
  that never exists, and the live check cannot catch a wrong threshold.
- **Sentinel and Redis persist RESOLVED IPs, and `docker compose down` destroys the network.** The
  compose file writes `sentinel monitor mymaster redis-primary 6379`; Sentinel rewrites its own config
  to `monitor mymaster 172.21.0.2 6379`, and Redis does the same to `replicaof`. Bring the stack down
  and up, the network is recreated on a NEW subnet, and both files now name addresses that cannot
  exist. Every container is up, every healthcheck is green, and failover is dead — measured: all three
  sentinels at `flags s_down,master` against a 172.21.x address while everything ran on 172.19.x,
  `num-slaves 0`, and killing the master produced no promotion at all. `sentinel resolve-hostnames yes`
  does NOT prevent this; it only permits a hostname as *input*. Fix: regenerate `sentinel.conf` on
  every boot (its contents are all re-derivable), and rewrite the `replicaof` ADDRESS while keeping the
  ROLE (with two data nodes, "my master is the other one" is always right). Same family as the ZK
  `clientPort` trap above.
- **`json.ignore-parse-errors` drops records with NO metric and NO log line.** Measured: 5 unparseable
  records into a running pipeline left `numRecordsInErrors` at 0 and nothing in any TaskManager log.
  There is no counter to alert on, and Kafka-consumed vs source-emitted are ~4x apart on a healthy
  pipeline so conservation cannot detect it either. A dead-letter topic is the only real fix. Do not
  write an alert for this — one was written and deleted; it would load cleanly, never fire, and make the
  gap look covered.
- **Prometheus does not reload alerts.yml on its own** (no `--web.enable-lifecycle` here), so editing a
  rule and re-running the verifier compares the new file against the old in-memory copy. `docker restart
  petafs-prod-prometheus` first — `make alerts-verify` catches the mismatch and says so.
- **A drill assertion whose PRECONDITION was not met must be a note, not a FAIL.** "no keys after
  failover" blamed Redis for an empty database it was handed — the pipeline had not written a feature
  yet. Capture the before-state and say so instead; scoring it either way is a lie.
- **Converging state has to be polled to a deadline, never sampled once.** A rejoining Redis replica
  reports `master_link_status:down` during its resync, so a single read right after a failover measures
  the resync. Seen failing at `down` with "sync: Finished with success" in the log seconds later.
- **The prod topology has no `prod-topics` target and `auto.create.topics.enable=false`.** Producing
  before creating `transactions` gives `0 eps` and a handful of errors, not a clear message.

### Authorization (ADR-016)

- **The Kafka CLI answers a DENIED call with silence too**, not just an unauthenticated one. A consumer
  refused `Read` does not exit — it retries the fetch forever and logs nothing at the default level. So
  a two-valued probe (`grep AuthorizationException || echo allowed`) reports a boundary *breach* when
  the truth is "no answer", and takes unbounded wall-clock with it. Denial probes must be three-valued:
  denied / allowed / **indeterminate**. `scripts/multihost_ha_drill.sh:probe()`.
- **`timeout N cmd | other` bounds `cmd` only.** The shell parses the pipeline first, so a probe piping
  into `kafka-console-producer` times out the `printf` and lets the JVM run forever. Write
  `timeout -s KILL N sh -c '<whole pipeline>'` — coreutils `timeout` signals the child's whole process
  group, so the JVM dies with the shell. Cost two 11-minute hangs before it was spotted.
- **A drill that writes to the live topic must write SCHEMA-VALID JSON.** A marker string (`drill`, `x`)
  parses as garbage, and the Flink source dies on `Unrecognized token`, re-reads the same offset from
  its checkpoint, and restart-loops permanently — which on this box drove the load average past 160 and
  made every later assertion fail for an unrelated reason. Use an event with an ancient
  `event_timestamp` so it parses, counts as a write, and is dropped as late by the window.
- **One malformed record kills the pipeline unless `--on-bad-record skip`.** `make mh-ha-submit` passes
  it; the dev stack deliberately does not (a schema mismatch should be loud there). `skip` counts what
  it drops — "tolerate but count" is the only safe version.
- **No features after ONE bounded burst is not a sink failure.** The HOP window needs the watermark to
  advance *past* the window end, and a burst that stops leaves the last windows unfired. Produce a
  second burst before concluding the sink is broken — 250 s of polling an empty Redis was this, not a
  bad ACL.
- **Redis: `masterauth` alone authenticates as `default`.** With `user default off`, a replica
  configured the old way fails to sync with `NOAUTH` and Sentinel never sees it come back. It needs
  `masteruser` too — and Sentinel needs `sentinel auth-user` alongside `auth-pass`, which is a
  *different* credential from the `requirepass` clients use to talk to the Sentinel itself.
- **`redis-cli --user` becomes mandatory once `default` is off.** A bare `-a "$PW"` authenticates a
  disabled user, so every probe reports the store as unreachable rather than as locked down — the
  failure looks like "Redis is down", not "your script is wrong".
- **The Kafka healthcheck spawns a JVM every 10 s per broker.** Three brokers = a JVM launch every
  ~3.3 s forever, and under load the authenticated `kafka-broker-api-versions.sh` exceeds its own 10 s
  timeout, so healthy brokers report `health: starting` indefinitely. Took 195 s to go healthy on a
  settled box.
- **Docker Desktop can be "manually paused"** and it presents exactly like a wedged daemon: every
  `docker` call hangs, `docker.sock` exists, the backend processes are running. The error text only
  appears on `docker version`. `docker desktop restart` clears it; there is no `resume` subcommand.
- **This box cannot run the HA topology, the embedding pipeline, and JVM-spawning probes at once.**
  18 containers + sentence-transformers + a drill peaked at load 302 on 6 VM CPUs and took the daemon
  down with it. Produce a bounded burst, let it drain, *then* drill.
