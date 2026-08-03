# Runbook — production topology

Covers `docker/docker-compose.prod.yml`. The laptop stack is `docker-compose.yml`
and none of this applies to it.

```bash
make prod-up        # 3 brokers, MinIO, Redis HA, Prometheus
make prod-submit    # submit the pipeline
make prod-drill     # kill things and assert what survived
make prod-down      # stop (volumes kept)
```

Flink UI `:8081` · Prometheus `:9090` · MinIO console `:9001` · Grafana `:3000` (`make prod-up UI=1`)

---

## 1. What is actually guaranteed

Each row was established by killing something, not by reading config. The drills
live in `scripts/failure_drills.sh`.

| Failure | Behaviour | Evidence |
|---|---|---|
| One Kafka broker dies | Leadership fails over, ISR 3→2, **no data loss**, writes continue at `acks=all` | 20,100 records intact; 20,100 → 23,100 written during the outage; ISR back to 1,2,3 on rejoin |
| TaskManager dies | Job restores from object storage, Kafka offsets recover with operator state | restored from `s3://flink-checkpoints/petafs/<job>/chk-N` |
| Redis master dies | Sentinel promotes the replica; the sink re-discovers and keeps writing | promotion in ~9 s; 8,018 keys replicated |
| Unauthenticated client | Rejected | `NOAUTH Authentication required` on both Redis and Sentinel |

| Unauthenticated Kafka client | Refused; authenticated one works | SASL/PLAIN on both client listeners |

## 2. Sizing — the numbers here are NOT production sizing

The prod compose runs small heaps so three brokers and a Flink cluster fit on a
laptop for the drills. Real deployment:

| Component | In this file | Production |
|---|---|---|
| Kafka broker heap | 512 MB | 6+ GB, dedicated disks, 3+ AZs |
| Kafka replication | RF=3, min.insync=2 | same, but across availability zones |
| TaskManager | 2 GB, 2 slots | sized from §5 below; the embedding tier is the constraint |
| Sentinels | **3, quorum 2** | same — plus separate failure domains |
| Object storage | MinIO, single node | S3/GCS with versioning and lifecycle rules |
| Retention | 2 h | driven by how far back you may need to replay |

## 3. Capacity, from measurement

`docs/SCALING.md` has the full argument. The operational numbers:

- **~8,500 events/s** is the measured pipeline ceiling on 2 slots / 8 cores.
  Beyond that, lag grows and does not recover.
- **The embedding tier saturates first**, not Kafka, not Redis. MiniLM runs
  450–475 emb/s per process and is memory-bandwidth bound, so scaling means more
  slots (or a GPU, or a longer window slide), not a bigger broker.
- **Reads**: p99 2.17 ms single-entity, 22.55 ms for 100. Redis is ~20% of that;
  the rest is Feast's Python.

Scale the TaskManager count first. Kafka partitions cap parallelism, so raising
slots past the partition count buys nothing.

## 4. Known operational traps

**Redis promotion must survive a restart — FIXED, and worth understanding.**
Originally: one sentinel (quorum 1) and `--replicaof` as a command-line argument.
Sentinel would promote the replica; the old primary would restart, its argument
would reassert the old role, and after enough failovers *both* nodes ended up
slaves with no master, reporting `s_down,o_down` while clients failed with
`MasterNotFoundError` — which reads as a connectivity problem, not a topology one.

Two changes fixed it, and both are necessary:
- **Three sentinels, quorum 2.** One sentinel cannot form a quorum, so a single
  node's opinion was enough to trigger — and mis-trigger — failover.
- **A config file, not command-line args.** Redis persists a Sentinel-driven
  promotion with `CONFIG REWRITE`, which only works against a file. With
  `--replicaof` as an argument there is nothing to rewrite.

Verified: kill the primary, wait for promotion, restart the old primary — it
rejoins as a *replica*, exactly one master remains, and all three sentinels
agree.

**The sentinel needs its own `requirepass`,** not just `auth-pass` for the master.
An open sentinel plus a password-sending client fails the AUTH, and discovery
dies with `MasterNotFoundError` rather than an auth error.

**The Kafka connector is a SHADED jar, and SASL needs the relocated login
module.** `flink-sql-connector-kafka-5.0.0-2.2.jar` ships
`org/apache/flink/kafka/shaded/org/apache/kafka/common/security/plain/PlainLoginModule`
and *not* the plain `org.apache.kafka` one. Naming the unshaded class in
`sasl.jaas.config` throws a `LoginException` that surfaces as **`flink run`
hanging during submission** — no "class not found" anywhere obvious. The broker
config and the kafka-python producer use the *unshaded* name; only the Flink
connector needs the relocated one. This cost the most time of anything here.

**Enabling SASL breaks every unauthenticated Kafka call**, not just clients:
healthchecks go `unhealthy` so `depends_on: service_healthy` never releases
(symptom: "Flink won't come up"), and CLI tools like `kafka-get-offsets.sh`
return empty rather than erroring — which made a failure drill silently skip
itself while reporting success.

**Flink rejects a bucket root for checkpoints** — `s3://bucket/` fails with
"Cannot use the root directory for checkpoints". Use a prefix.

**The S3 plugin must be copied into `plugins/`.** It ships in `/opt/flink/opt/`
but is only loaded from `/opt/flink/plugins/<name>/`; otherwise `s3://` fails
with "Could not find a file system implementation for scheme 's3'".

**PyFlink caches Python workers across job submissions.** Cancelling and
resubmitting does not reload edited UDF or sink code. `docker compose restart
taskmanager` after changing Python that runs inside Flink.

**Consumer-group lag is not Flink's progress.** Flink tracks offsets in
checkpoints, so `kafka-consumer-groups.sh` can show a frozen lag while the job
is healthy. Use the Flink metrics, not the consumer group.

## 5. Upgrading the job without losing state

The pipeline's state is the in-flight windows. A plain cancel/resubmit throws
them away and replays from the last checkpoint; a savepoint carries them across.

```bash
# 1. Take a savepoint and stop cleanly.
JID=$(curl -s localhost:8081/jobs/overview | python3 -c \
  "import json,sys;print(next(j['jid'] for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING')")
docker compose -f docker/docker-compose.prod.yml exec -T jobmanager \
  flink stop --savepointPath s3://flink-savepoints/petafs "$JID"

# 2. Deploy the new code (restart the TaskManager — PyFlink caches modules).
docker compose -f docker/docker-compose.prod.yml restart taskmanager

# 3. Resume from the savepoint.
docker compose -f docker/docker-compose.prod.yml exec -T jobmanager \
  flink run -d -s s3://flink-savepoints/petafs/savepoint-xxxx \
  -py /opt/project/src/flink/streaming_job.py --no-wait --sink redis \
  --bootstrap kafka1:9092,kafka2:9092,kafka3:9092
```

**A schema change is not just a deploy.** Adding or renaming a feature changes
`mmh3("{feature_view}:{feature}")`, so old Redis entries become unreadable for
the changed fields and the cache cold-starts. Sequence: apply the registry
first, deploy the job, then accept one TTL window of partial features. Never
enable `registry.enable_online_feature_view_versioning` — the sink hashes the
raw name (Invariant 8).

## 6. When an alert fires

| Alert | First thing to check |
|---|---|
| `KafkaConsumerLagGrowing` | Embedding operator busy-time. If pinned, add TaskManagers — the ceiling is §3's 8,500/s. |
| `CheckpointsFailing` | Object-storage credentials, then checkpoint duration vs the 30 s interval. Unaligned checkpoints are already on. |
| `JobRestarting` | TaskManager log for an SDK-harness traceback — a Python UDF or sink exception is the usual cause. |
| `SustainedBackpressure` | Which operator. Embedding is the expected culprit; anything else is a regression. |
| `NoFeaturesWritten` | **Most serious.** The pipeline is up and serving increasingly stale features that Feast still reports as `PRESENT`. Check the sink's Redis connection and whether windows are firing at all. |

## 7. Restoring from nothing

Features are derived and TTL'd at 30 minutes, so the online store is a cache,
not a system of record. Rebuilding:

1. Confirm Kafka retention still covers the window you need (default 2 h here).
2. Resubmit with `--startup-mode earliest-offset`.
3. Features repopulate as windows close — roughly one slide interval for active
   users, up to the full window for complete values.

If Kafka retention has aged out, those features are gone and cannot be
recomputed from the online store. That is the argument for the offline store,
not a bug.
