# Multi-host topology

The last open gap in [README](../README.md)'s production-readiness table was
deployment topology: `docker-compose.prod.yml` replicates everything three ways
but puts all three copies on one machine, which survives a *process* failure
and not a *host* failure.

This closes it. `docker-compose.multihost.yml` spreads the same services across
three independent nodes, and `make mh-drill` kills an entire node to prove what
survives. `docker-compose.multihost-ha.yml` (§5) goes further: a ZooKeeper
ensemble and a second JobManager, so that losing the node running the pipeline
does not stop the pipeline.

```bash
make mh-tls        # CA + certs, with the routable node names in the SANs
make mh-up         # three Compose projects = three isolated nodes
make mh-topics     # transactions at RF=3 across the three
make mh-produce    # events in, from this machine, over SASL_SSL
make mh-submit     # the pipeline
make mh-drill      # kill a whole node, assert what lived
make mh-down       # stop (volumes kept)   /  make mh-clean drops them
```

---

## 1. Layout

| Node | Runs | Why there |
|---|---|---|
| **A** | kafka1, Redis primary, sentinel 1, MinIO, **JobManager** | The two singletons — JobManager and object store — are co-located deliberately, so exactly one node is special instead of two |
| **B** | kafka2, Redis replica, sentinel 2, TaskManager | |
| **C** | kafka3, sentinel 3, TaskManager | No Redis data node. Two data nodes plus three sentinels is the standard shape: the third vote has to live somewhere that is not either data node, or losing a data node also loses the quorum that was meant to react to it |

Losing **B or C** is fully survivable and is what drill 2 proves. Losing **A**
keeps all the data but stops the running job, because this file has a single
JobManager. §5 removes that with a ZooKeeper ensemble and a standby; §6 is what
is left after it.

## 2. What actually changes without Compose DNS

This is the substance. On one host every service finds every other by its
Compose service name; that convenience is also the single host. Take it away
and four separate discovery mechanisms have to be re-pointed at addresses a
peer on another machine can route to — each of which fails differently, and
three of the four fail *silently*.

| | Single host | Multi host | Failure mode if you skip it |
|---|---|---|---|
| **Kafka** | `advertised.listeners: kafka1:9092` | routable `${NODE_A_HOST}:${PORT}`; `controller.quorum.voters` likewise | Clients bootstrap fine, then hang forever on the redirect to an unresolvable name |
| **Redis** | `replicaof redis-primary 6379` | plus `replica-announce-ip/-port` and `sentinel announce-ip/-port` | The primary reports its replica's *container-private* IP; Sentinel publishes that to clients; every off-node client discovers an address that does not exist |
| **Flink** | defaults | `jobmanager.rpc.address` + fixed `rpc`/`blob` ports; per-TaskManager `taskmanager.host` and a bind/advertise **port split** | TaskManagers register under a container hostname nothing can reach, or on an ephemeral port; they retry forever with nothing logged JobManager-side |
| **TLS** | SANs for service names | SANs must include every routable node name | Handshake fails hostname verification — visible at the client, absent from the server log |

The Flink one is the least obvious. Flink separates *what to bind* from *what
to advertise*, and the multi-host config needs both:

```
taskmanager.bind-host: 0.0.0.0        # listen everywhere inside the container
taskmanager.rpc.bind-port: 6122       # on this port
taskmanager.host: node-b.internal     # but tell the JobManager to reach me here
taskmanager.rpc.port: 16122           # on this port
```

The JobManager publishes 1:1 instead, because `blob.server.port` has **no**
bind-port counterpart — whatever it binds is what it advertises, so a remapped
published port silently breaks artifact download for every TaskManager. Only
one node runs a JobManager, so there is nothing to remap anyway.

Proof it worked, from drill 1 — these are the *routable* addresses, not
container hostnames:

```
pekko.tcp://flink@host.docker.internal:16122/user/rpc/taskmanager_0
pekko.tcp://flink@host.docker.internal:16222/user/rpc/taskmanager_0
```

### The KRaft controller is no longer "internal"

`docker-compose.prod.yml` leaves the controller listener PLAINTEXT and
justifies it as never leaving the Compose network. Across hosts that
justification evaporates — the quorum crosses the wire like everything else.
Here it is **SSL with `ssl.client.auth=required`**, i.e. mTLS between
controllers. This does not hit the bootstrap-ordering problem that ruled out
SCRAM for the quorum, because certificates are files on disk rather than
credentials living in the metadata log the quorum is still forming.

Drill 1 asserts it by probing the port directly, because "configured" and
"enforced" are different claims:

```
no client cert  -> SSLError: TLSV13_ALERT_CERTIFICATE_REQUIRED
with client cert-> accepted
```

## 3. The local simulation, and what it does not prove

Three Compose projects, each with **its own Docker network**. There is no
shared network and therefore no Compose DNS between them: a container on node B
cannot resolve `kafka1`, exactly as it could not if node A were a different
machine. Every cross-node connection leaves through a published port and comes
back in over an address that resolves off-node.

That is the property that breaks real multi-host deployments, and it is
genuinely reproduced. **Drill 0 asserts it before anything else** — separate
networks, and node B failing to resolve node A's service names. If the projects
ever shared a network, every later assertion would be measuring a single-host
stack in a three-host costume, and passing would be worse than failing.

**It does not reproduce**: network partitions, cross-datacentre latency, or
hardware failure. `docker compose down -t 0` on a whole project is a close
analogue of losing a machine — SIGKILL with no grace period, all processes at
once, that node's volumes left intact — but it is not the same thing. A real
deployment also has to answer split-brain across a partition, which nothing
here exercises.

One local quirk worth writing down: **Docker Desktop containers cannot reach
this Mac's LAN IP** (verified — the connection times out), so all three nodes
use `host.docker.internal` and node identity is carried by the port. On real
hosts the addresses differ and every port returns to its natural value. The
*shape* of the config is identical either way, which is what makes the
simulation worth anything.

## 4. Measured — `make mh-drill`, 24/24

| Drill | Assertion | Result |
|---|---|---|
| 0 | Three separate Docker networks; node B cannot resolve node A's names | isolation is real |
| 1 | KRaft quorum spans three nodes | 3 brokers, voters at routable addresses |
| 1 | Topic replicated across three failure domains | ISR=3 on all 6 partitions |
| 1 | Redis replication + Sentinel gossip cross the boundary | link up; all 3 sentinels see 2 peers |
| 1 | Controller enforces mTLS | no client cert → TLS alert |
| 1 | TaskManagers registered at routable addresses | ports 16122 / 16222 |
| 2 | **Kill the node running the job** (chosen by asking Flink, not fixed) | job rescheduled C → B |
| 2 | State recovered from object storage, not the dead node | `s3://flink-checkpoints/petafs/<job>/chk-4` |
| 2 | No data lost | 48,985 → 60,389 records |
| 2 | Features still being written | max TTL 1778 s of 1800 |
| 2 | Node rejoins | back to 3 brokers, ISR 3 |
| 3 | **Kill node A** — cross-host Sentinel promotion | master moved A → B |
| 3 | Kafka degrades correctly and keeps serving | ISR → 2 on 6 partitions; 93,055 records still readable |
| 3 | No split brain when A returns | exactly one master |
| 3 | Pipeline resumes once A is back | resubmitted, RUNNING |

Numbers are from one cold-start run — `mh-clean` then the sequence at the top of
this file — so they are what the committed scripts produce from nothing, not an
accumulated state.

Drill 2 picks its victim by asking Flink where the job is executing. At
parallelism 1 the job occupies one TaskManager, so killing a *fixed* node would
half the time miss it entirely — and "still RUNNING" would then prove nothing
about recovery. The victim genuinely varies with placement: across runs it has
killed node C (job only, above) and node B (job **and** the Redis master at the
same time, which also passed).

End to end, read back the way a model would — `get_online_features()` against the
multi-host cluster **after** the drills, so this is the Redis that was promoted
when node A died, not the one the features were originally written to:

```
entities queried: 25  with features: 25
  user 1: count=5870 avg=402.45 emb_dim=384 behavior='airline, bookstore, coffee_shop, electronics'
  user 2: count=2825 avg=69.22  emb_dim=384 behavior='airline, bookstore, coffee_shop, electronics'
```

## 5. JobManager HA — `docker-compose.multihost-ha.yml`

In the topology above, node A carries the single JobManager: kill it and the
data all survives but the *pipeline* stops until someone resubmits. The HA file
closes that. It is a superset — same routable addressing, same mTLS quorum,
same Sentinel setup — plus a **ZooKeeper ensemble** and a **second
JobManager**, with the layout rearranged so nothing critical shares a node with
its own failover target:

```
node A   kafka1, redis primary, sentinel 1, zk 1, MinIO
node B   kafka2, redis replica,  sentinel 2, zk 2, JobManager, TaskManager
node C   kafka3,                 sentinel 3, zk 3, JobManager, TaskManager
```

```bash
make mh-ha-up && make mh-ha-topics && make mh-ha-produce && make mh-ha-submit
make mh-ha-drill      # kills the node holding the LEADING JobManager
```

Flink 2.2.1 needs **no extra jar** for this: shaded ZooKeeper 3 and Curator 5
are already inside `flink-dist-2.2.1.jar`. In 1.x they were a separate jar you
copied into `lib/`, which is what most tutorials still tell you to do.

### Measured — `make mh-ha-drill`, 14/14

| Assertion | Result |
|---|---|
| ZooKeeper ensemble spans three nodes | 1 leader, 2 followers |
| Both JobManagers live, exactly one leader | per ZooKeeper, not per REST |
| Job graph reached the HA store | `submittedExecutionPlan` + blob in `s3://flink-ha` |
| **Leadership moves after the leader's node dies** | automatic, no operator |
| **Pipeline recovers with no resubmission** | same job id throughout |
| State recovered, not restarted | `s3://flink-checkpoints/petafs/<job>/chk-32` |
| Features flowing after failover | max TTL 1770 s of 1800 |
| Leadership does not bounce back on restore | restored JM rejoins as contender |

Run in both directions — once killing node B while node C led, once killing
node C while node B led. The victim is whichever node ZooKeeper says is
leading, never a fixed one.

### Ask ZooKeeper who leads. Not the REST API.

The obvious leader check — "the JobManager whose REST answers 200" — is wrong.
In steady state **both** JobManagers answer 200 and return identical job lists
and TaskManager counts, because a non-leading JobManager proxies to the leader.
A 503 appears only in the brief window before its REST leader-contender starts,
which is exactly long enough for a check written during startup to look
correct and then silently stop discriminating forever after. The leader address
lives in ZooKeeper at
`/flink/<cluster-id>/leader/rest_server/connection_info`, and that is the only
authority.

### `high-availability.jobmanager.port`, or nothing registers

In HA mode `jobmanager.rpc.port` is **ignored**. The port actually used comes
from `high-availability.jobmanager.port`, which defaults to `0` — a random one.
The JobManager then publishes an ephemeral port into ZooKeeper, the TaskManager
reads it and dials a port Docker never published, and you get

```
Could not resolve ResourceManager address pekko.tcp://flink@host:42335/... retrying
```

forever. On a single host it works by accident, because there is no port
mapping in the way. Set it explicitly, to a port you publish.

## 6. What still fails

**The object store is one node, and that is the honest remaining gap.** MinIO
runs on node A and `s3.endpoint` names it. Lose node A and the data is fine —
Redis fails over, Kafka keeps serving, both JobManagers are alive — but there is
nowhere to checkpoint to.

Distributed MinIO across the three nodes would fix the *durability* half and
not the other half: Flink takes a single `s3.endpoint`, so the endpoint itself
still has to be highly available, which means a load balancer or DNS failover
in front of it. That is precisely what a managed object store is — S3 and GCS
give you a regional endpoint with the availability already solved. Swapping
MinIO for either is a credentials-and-URL change, not a code one; it is already
how `s3.endpoint` is wired. Standing up an LB tier on a laptop to simulate
something whose real answer is "use the managed service" would be theatre.

Also still open, unchanged by this work: secrets are Compose environment
variables with dev defaults, and the Feast private-API dependency
([ADR-003](DECISIONS.md)).

## 7. Going to three real machines

1. Put the certs where they are needed and regenerate with the real names:
   `NODE_SANS=host-a.internal,host-b.internal,host-c.internal make mh-tls`.
   Every cert gets every node name — after a Redis failover or a Kafka
   leadership move, a client can legitimately end up talking to any node.
2. Write an `.env` with `NODE_A_HOST`/`NODE_B_HOST`/`NODE_C_HOST` set to real
   DNS names. **Names, not IPs** — see the trap below. Delete every port
   override; the defaults are already the natural ports, one per machine.
3. On each host: `docker compose -f docker-compose.multihost.yml --profile <a|b|c> up -d`.
4. Open between hosts: Kafka 9092/9093, Redis 6379, Sentinel 26379, Flink
   6123/6124/6122/6121, MinIO 9000, metrics 9250.
5. Replace MinIO with real object storage and the dev credentials with a secret
   store.

**Use hostnames, not IPs.** Redis writes the *address* it was pointed at into
its config file and Sentinel rewrites it on promotion, so an IP-based
`replicaof` does not survive the peer moving — the single-host version of this
trap is in [RUNBOOK §4](RUNBOOK.md). Everything here announces a hostname for
that reason.

## 8. Traps found building this

**The Kafka CLI answers an unauthenticated call with silence.** These drills
delete and recreate whole nodes, so a credentials file written into a
container's `/tmp` at startup is gone the moment that node is restored. The
next `kafka-get-offsets.sh` returned *empty* rather than failing, `kafka_records`
read 0, and a topic holding 163,420 records looked like it had been wiped. The
scripts now rewrite the credentials on every call. This is the same failure the
single-host drills already paid for once, in a new disguise.

**Feast's Redis keys are binary.** A "are features still being written" probe
that scans keys out to the shell and passes them back to `TTL` mangles every
one, and each lookup returns `-2`, "no such key" — which reads as *the pipeline
has stopped* rather than *the probe is broken*. The check runs as a Lua script
so the keys never leave Redis.

**A master has no `master_link_status`.** Naming a container when reading the
replication link reports a perfectly healthy link as absent the first time a
failover has swapped the roles. Ask whichever node currently reports
`role:slave`. `scripts/failure_drills.sh` documents the same trap; I reproduced
it in a new script anyway.

**A dead broker lingers in cluster metadata** until its session expires, so a
broker count taken immediately after a kill still reads 3. That makes a
count-based assertion timing-dependent rather than correctness-based. Assert
that ISR shrank and the topic is still readable instead — those mean the
survivors actually noticed.

**Do not run a Python client inside the Flink containers on this box.** With all
three nodes up, a `get_online_features()` call inside the JobManager container
gets OOM-killed part-way through — silently, so it looks like a hang. Run it
from the host venv against the published Redis port instead; the certs carry
`DNS:localhost` for exactly this. That is a property of a 7.75 GiB Docker VM
running a whole three-node cluster, not of the topology.

**A Kafka broker's resident size is dominated by mmap'd log segments, not by
`KAFKA_HEAP_OPTS`.** With the heap pinned at 320m and the topic left at the
default 2 GiB retention, the three brokers grew from ~390 MiB each to ~1.4 GiB
each under a sustained producer. The box went to 7.5 of 7.75 GiB, and Flink
started failing in ways that name nothing to do with memory:

```
Could not create actor system
TimeoutException: Futures timed out after [10000 milliseconds]
NoClassDefFoundError: org/apache/pekko/actor/dungeon/FaultHandling$...
```

I concluded from this that an 8 GiB laptop could not run two TaskManagers
alongside HA, and that was **wrong** — it was the topic, not the topology. With
retention capped the same TaskManager registers in 30 s and everything fits in
5.4 GiB. If Flink starts throwing class-loading errors, look at the brokers
before you believe an arithmetic argument about what fits.

**`docker compose down -v` can leave the volumes behind** — if any container was
mid-restart when it ran, it removes the containers, reports success, and skips
the volumes. The stale Kafka logs then survive into the next run and the topic
config you just set appears to have been ignored (`--if-not-exists` quietly
keeps the old topic). `make mh-ha-clean` now sweeps by name and verifies.
