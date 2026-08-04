#!/usr/bin/env bash
# Failure drills for the MULTI-HOST topology. `make mh-drill`.
#
# scripts/failure_drills.sh kills processes. This kills HOSTS: every container
# in one Compose project at once, which is as close to losing a machine as one
# machine can get.
#
# Drill 0 comes first and is not optional. It asserts the three projects really
# are isolated — separate Docker networks, no cross-node name resolution. If
# that fails, every assertion after it is measuring a single-host stack wearing
# a three-host costume, and passing would be worse than failing.
#
# Destructive by design. Runs against docker-compose.multihost.yml only.
set -uo pipefail
cd "$(dirname "$0")/.."

MH="docker compose -f docker/docker-compose.multihost.yml --env-file docker/multihost/sim.env"
A="$MH -p petafs-a --profile a"
B="$MH -p petafs-b --profile b"
C="$MH -p petafs-c --profile c"

PW="${REDIS_PASSWORD:-petafs-dev-secret}"
RCLI="redis-cli --tls --cacert /tls/ca.crt"
K1=petafs-mh-kafka1
TOPIC="${TOPIC:-transactions}"

pass=0; fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }
note() { printf '  \033[90m%s\033[0m\n' "$*"; }

kafka_conf() {
  docker exec "$1" sh -c "cat > /tmp/mh.properties <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USER:-petafs}\" password=\"${KAFKA_PASSWORD:-petafs-dev-secret}\";
ssl.truststore.location=/tls/kafka.truststore.jks
ssl.truststore.password=${TLS_STOREPASS:-petafs-dev-secret}
EOF" >/dev/null 2>&1
}
kbin() {  # kbin <container> <tool> [args...]
  local c="$1" tool="$2"; shift 2
  # Re-write the credentials every call. These drills DELETE and recreate whole
  # nodes, so a properties file written into /tmp at startup is gone the moment
  # its node is restored. The Kafka CLI then returns EMPTY rather than failing,
  # so `kafka_records` reads 0 and "no data lost" turns into a false failure —
  # or worse, a false pass. Cheap insurance against an unfalsifiable assertion.
  kafka_conf "$c"
  docker exec "$c" "/opt/kafka/bin/${tool}" --bootstrap-server localhost:9092 \
    --command-config /tmp/mh.properties "$@" 2>/dev/null
}
broker_count() { kbin "${1:-$K1}" kafka-broker-api-versions.sh | grep -cE '\(id: [0-9]+ '; }
kafka_records() {
  kbin "${1:-$K1}" kafka-get-offsets.sh --topic "$TOPIC" | awk -F: '{s+=$3} END {print s+0}'
}
isr_of_size() {  # isr_of_size <container> <n> -> partition count whose ISR has n members
  kbin "$1" kafka-topics.sh --describe --topic "$TOPIC" \
    | grep -oE "Isr: [0-9]+(,[0-9]+)*" | awk -F, -v n="$2" 'NF==n' | wc -l | tr -d ' '
}

# Whichever Redis node is currently the master — roles swap during these drills,
# so nothing may assume the container named "primary" still is one.
redis_master_container() {
  for c in petafs-mh-redis-primary petafs-mh-redis-replica; do
    docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null \
      | grep -q '^role:master' && { echo "$c"; return; }
  done
}
redis_replica_container() {
  for c in petafs-mh-redis-primary petafs-mh-redis-replica; do
    docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null \
      | grep -q '^role:slave' && { echo "$c"; return; }
  done
}
sentinel_master() {  # ask a specific sentinel; returns "host:port"
  docker exec "$1" $RCLI -p 26379 -a "$PW" \
    SENTINEL get-master-addr-by-name mymaster 2>/dev/null | tr '\n' ':' | sed 's/:$//' | tr -d '\r'
}
# Highest TTL across a sample of feature keys. Every write re-issues
# EXPIRE(1800), so a TTL near the ceiling means the sink wrote recently. This is
# the cheapest honest answer to "are features still flowing" — key COUNT goes
# flat once the user population is covered and would look identical whether the
# pipeline was healthy or dead.
#
# Done in Lua so the keys never leave Redis. Feast keys are BINARY (a serialized
# EntityKeyProto), so scanning them out to the shell and passing them back as
# arguments mangles every one: each TTL then returns -2, "no such key", which
# reads as "the pipeline stopped writing" rather than "the probe is broken".
max_ttl() {
  local m; m=$(redis_master_container); [ -z "$m" ] && { echo -1; return; }
  docker exec "$m" $RCLI -a "$PW" EVAL \
    "local c=redis.call('SCAN',0,'COUNT',300); local m=-2;
     for _,k in ipairs(c[2]) do local t=redis.call('TTL',k); if t>m then m=t end end; return m" 0 \
    2>/dev/null | tr -d '\r'
}

# Which node is actually executing the job. With parallelism 1 the job lives on
# exactly one TaskManager, so killing a node at random has a coin-flip chance of
# not touching it at all — and "the job is still running" would then prove
# nothing about recovery. Ports identify the node: 16122 is B, 16222 is C.
job_node() {
  local jid="$1"
  python3 - "$jid" <<'PY' 2>/dev/null
import json, sys, urllib.request
jid = sys.argv[1]
base = "http://localhost:8081"
hosts = set()
try:
    j = json.load(urllib.request.urlopen(f"{base}/jobs/{jid}", timeout=5))
    for v in j.get("vertices", []):
        d = json.load(urllib.request.urlopen(f"{base}/jobs/{jid}/vertices/{v['id']}", timeout=5))
        for st in d.get("subtasks", []):
            hosts.add(str(st.get("host") or "") + str(st.get("taskmanager-id") or ""))
except Exception:
    pass
blob = " ".join(hosts)
print("b" if "16122" in blob else "c" if "16222" in blob else "")
PY
}
flink_running_job() {
  curl -s -m 5 http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json,sys
try: print(next((j['jid'] for j in json.load(sys.stdin).get('jobs',[]) if j['state']=='RUNNING'),''))
except Exception: print('')"
}
flink_tm_count() {
  curl -s -m 5 http://localhost:8081/overview 2>/dev/null | python3 -c "
import json,sys
try: print(json.load(sys.stdin).get('taskmanagers',0))
except Exception: print(0)"
}
kafka_conf "$K1"

# ===========================================================================
step "Drill 0 — the three nodes really are isolated (the simulation is honest)"
# ===========================================================================
NETS=$(for n in a b c; do
         docker network ls --filter "name=petafs-$n" --format '{{.ID}}'
       done | sort -u | wc -l | tr -d ' ')
[ "${NETS:-0}" -eq 3 ] && ok "three separate Docker networks, one per node" \
                       || bad "expected 3 distinct networks, found ${NETS:-0}"

# Node B must NOT be able to resolve a node-A service name. If it can, the
# projects share a network and this whole exercise proves nothing.
if docker exec petafs-mh-taskmanager-b python3 -c "import socket;socket.gethostbyname('kafka1')" >/dev/null 2>&1; then
  bad "node B resolved 'kafka1' — the nodes share a network, isolation is fake"
else
  ok "node B cannot resolve node A's service names (no shared Compose DNS)"
fi
if docker exec petafs-mh-taskmanager-b python3 -c "import socket;socket.gethostbyname('host.docker.internal')" >/dev/null 2>&1; then
  ok "node B resolves the routable node address instead"
else
  bad "node B cannot resolve host.docker.internal — nothing can work"
fi

# ===========================================================================
step "Drill 1 — the cluster spans all three nodes"
# ===========================================================================
BC=$(broker_count)
[ "${BC:-0}" -eq 3 ] && ok "KRaft quorum formed across three nodes (3 brokers)" \
                     || bad "only ${BC:-0}/3 brokers in the cluster"

if [ "$(kafka_records)" -eq 0 ] 2>/dev/null; then
  note "topic '$TOPIC' is empty — run 'make mh-topics && make mh-produce' first"
fi
FULL=$(isr_of_size "$K1" 3)
[ "${FULL:-0}" -ge 1 ] && ok "partitions replicated across all three nodes (ISR=3 on $FULL)" \
                       || bad "no partition has a 3-member ISR"

# Ask whichever node is CURRENTLY the replica. Roles swap during these drills
# and a master has no master_link_status at all, so naming a container here
# reports a perfectly healthy link as "absent" the first time a failover has
# happened — the same trap as scripts/failure_drills.sh drill 5.
RC=$(redis_replica_container)
LINK=$(docker exec "${RC:-petafs-mh-redis-replica}" $RCLI -a "$PW" INFO replication 2>/dev/null \
       | grep -o 'master_link_status:[a-z]*' | cut -d: -f2 | tr -d '\r')
[ "$LINK" = "up" ] && ok "Redis replicates across the node boundary over TLS (${RC#petafs-mh-} is the replica)" \
                   || bad "replication link is '${LINK:-absent}' on ${RC:-no replica found}"

PEERS=$(for n in 1 2 3; do
          docker exec "petafs-mh-redis-sentinel-$n" $RCLI -p 26379 -a "$PW" \
            SENTINEL master mymaster 2>/dev/null | grep -A1 '^num-other-sentinels$' | tail -1 | tr -d '\r'
        done | sort -u | tr '\n' ' ')
[ "$(echo "$PEERS" | tr -d ' ')" = "2" ] && ok "all three sentinels see both peers (announce-ip works)" \
                                         || bad "sentinel peer counts: $PEERS (expected 2 2 2)"

# The KRaft quorum crosses the network now, so PLAINTEXT is no longer
# defensible the way it was inside one Compose network. Prove mTLS is actually
# enforced rather than merely configured: the discriminator is a TLS alert, not
# an application error, because Kafka never gets a chance to reply.
MTLS=$(docker exec petafs-mh-taskmanager-b python3 -c "
import socket, ssl
def probe(cert):
    ctx = ssl.create_default_context(cafile='/tls/ca.crt')
    if cert: ctx.load_cert_chain('/tls/client.crt','/tls/client.key')
    try:
        s = ctx.wrap_socket(socket.create_connection(('${NODE_A_HOST:-host.docker.internal}', ${KAFKA_A_CTRL_PORT:-19093}), timeout=10),
                            server_hostname='${NODE_A_HOST:-host.docker.internal}')
        s.send(b'\x00\x00\x00\x04\x00\x12\x00\x00'); s.recv(16); return 'ok'
    except ssl.SSLError as e:
        return 'refused' if 'CERTIFICATE_REQUIRED' in str(e) else 'other'
    except Exception:
        return 'unreachable'
print(probe(False), probe(True))" 2>/dev/null)
[ "$MTLS" = "refused ok" ] \
  && ok "KRaft controller port enforces mTLS (no client cert -> TLS alert)" \
  || bad "controller mTLS probe returned '${MTLS:-nothing}', expected 'refused ok'"

# The strongest single proof that the advertise/bind split is right: the
# JobManager knows its TaskManagers by their ROUTABLE addresses. Default Flink
# would register them under a container hostname no other node can reach.
TMPATHS=$(curl -s -m 5 http://localhost:8081/taskmanagers 2>/dev/null | python3 -c "
import json,sys
try: print(' '.join(t.get('path','') for t in json.load(sys.stdin).get('taskmanagers',[])))
except Exception: print('')")
if echo "$TMPATHS" | grep -q "16122" && echo "$TMPATHS" | grep -q "16222"; then
  ok "JobManager registered both TaskManagers at their routable addresses"
  note "$(echo "$TMPATHS" | tr ' ' '\n' | sed 's/^/       /')"
else
  bad "TaskManager paths are not the routable addresses: ${TMPATHS:-none}"
fi

# ===========================================================================
step "Drill 2 — lose the node that is actually running the job"
# ===========================================================================
# `down -t 0` is SIGKILL with no grace period, then remove. A graceful stop
# would let Kafka hand off leadership cleanly, which is precisely the thing a
# dying machine does not do.
#
# The victim is chosen by asking Flink where the job is executing, not fixed in
# advance: at parallelism 1 the job occupies one TaskManager, so killing a fixed
# node would half the time leave it untouched and "still RUNNING" would prove
# nothing.
REC_BEFORE=$(kafka_records)
JOB_BEFORE=$(flink_running_job)
VN=""
[ -n "$JOB_BEFORE" ] && VN=$(job_node "$JOB_BEFORE")
[ -z "$VN" ] && { VN=c; note "job placement unknown — falling back to node C"; }
case "$VN" in
  b) VICTIM="$B"; VNAME="B (kafka2 + redis replica + sentinel 2 + taskmanager)" ;;
  *) VICTIM="$C"; VNAME="C (kafka3 + sentinel 3 + taskmanager)" ;;
esac
note "before: $REC_BEFORE records, $(flink_tm_count) taskmanagers, job=${JOB_BEFORE:-none}"
note "the job is executing on node ${VN} — killing that node"

$VICTIM down -t 0 >/dev/null 2>&1
note "node $VNAME is gone"
sleep 25

BC=$(broker_count)
[ "${BC:-0}" -eq 2 ] && ok "Kafka serving from the surviving two nodes" \
                     || bad "broker count is ${BC:-0}, expected 2"

REC_AFTER=$(kafka_records)
[ "${REC_AFTER:-0}" -ge "${REC_BEFORE:-0}" ] && ok "no data lost with a node down ($REC_BEFORE -> $REC_AFTER)" \
                                             || bad "records went $REC_BEFORE -> $REC_AFTER"

UNDER=$(isr_of_size "$K1" 2)
[ "${UNDER:-0}" -gt 0 ] && ok "ISR shrank to 2 and held at min.insync.replicas" \
                        || bad "unexpected ISR state after losing a node"

SM=$(sentinel_master petafs-mh-redis-sentinel-1)
[ -n "$SM" ] && ok "Sentinel quorum survives on two nodes (master $SM)" \
             || bad "sentinels lost quorum when one node went away"

if [ -n "$JOB_BEFORE" ]; then
  JID2=""
  for _ in $(seq 1 60); do JID2=$(flink_running_job); [ -n "$JID2" ] && break; sleep 5; done
  if [ -n "$JID2" ]; then
    SURV=$(job_node "$JID2")
    [ -n "$SURV" ] && [ "$SURV" != "$VN" ] \
      && ok "job rescheduled from node $VN onto node $SURV" \
      || ok "job is RUNNING again (now on node ${SURV:-unknown})"
    R=$(curl -s "http://localhost:8081/jobs/$JID2/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try: print(((json.load(sys.stdin).get('latest') or {}).get('restored') or {}).get('external_path',''))
except Exception: print('')")
    case "$R" in
      s3://*) ok "state restored from object storage, not from the dead node" ; note "$R" ;;
      *)      bad "no object-storage restore recorded (got '${R:-nothing}')" ;;
    esac
    # Give the recovered job a moment to close a window and write.
    sleep 60
    T=$(max_ttl)
    [ "${T:-0}" -gt 1650 ] && ok "features still being written after the node loss (max TTL ${T}s of 1800)" \
                           || bad "no recent feature writes (max TTL ${T:-none})"
  else
    bad "job did not return to RUNNING within 300 s"
  fi
else
  note "no running job — skipping the recovery assertions ('make mh-submit' first)"
fi

note "restoring node $VN..."
$VICTIM up -d >/dev/null 2>&1
for _ in $(seq 1 30); do [ "$(broker_count)" = "3" ] && break; sleep 5; done
BC=$(broker_count)
[ "${BC:-0}" -eq 3 ] && ok "node $VN rejoined the cluster (3 brokers)" \
                     || bad "node $VN did not rejoin (broker count ${BC:-0})"

# ===========================================================================
step "Drill 3 — lose node A, the node holding the Redis primary"
# ===========================================================================
# Cross-host Sentinel promotion: the surviving sentinels on B and C are exactly
# a quorum of 2. This also demonstrates the topology's remaining single point of
# failure, because node A carries the JobManager and the object store.
M0=$(sentinel_master petafs-mh-redis-sentinel-2)
note "master before: ${M0:-unknown}"

$A down -t 0 >/dev/null 2>&1
note "node A is gone (with it: the JobManager and MinIO)"

M1=""
for _ in $(seq 1 25); do
  M1=$(sentinel_master petafs-mh-redis-sentinel-2)
  [ -n "$M1" ] && [ "$M1" != "$M0" ] && break
  sleep 3
done
[ -n "$M1" ] && [ "$M1" != "$M0" ] \
  && ok "sentinels on B and C promoted across hosts ($M0 -> $M1)" \
  || bad "no cross-host promotion (master still ${M1:-unknown})"

ROLE=$(docker exec petafs-mh-redis-replica $RCLI -a "$PW" INFO replication 2>/dev/null \
       | grep -o '^role:[a-z]*' | cut -d: -f2 | tr -d '\r')
[ "$ROLE" = "master" ] && ok "node B's Redis is now the master and accepting writes" \
                       || bad "node B's Redis reports role='$ROLE'"

KEYS=$(docker exec petafs-mh-redis-replica $RCLI -a "$PW" DBSIZE 2>/dev/null | tr -d '\r')
[ "${KEYS:-0}" -gt 0 ] && ok "features survived the loss of the primary's host ($KEYS keys)" \
                       || bad "no keys on the promoted node"

# Assert Kafka still WORKS rather than counting brokers. A broker that has just
# died lingers in cluster metadata until its session expires, so a count taken
# right after the kill legitimately still reads 3 — a timing-dependent
# assertion, not a correctness one. ISR shrinking is the signal that the
# surviving brokers actually noticed and are serving degraded-but-correct.
DEG=0
for _ in $(seq 1 24); do
  DEG=$(isr_of_size petafs-mh-kafka2 2)
  [ "${DEG:-0}" -gt 0 ] && break
  sleep 5
done
[ "${DEG:-0}" -gt 0 ] && ok "Kafka noticed node A is gone; ISR degraded to 2 on $DEG partitions" \
                      || bad "ISR never shrank after losing node A"
REC=$(kafka_records petafs-mh-kafka2)
[ "${REC:-0}" -gt 0 ] && ok "topic still readable from a surviving node ($REC records)" \
                      || bad "topic unreadable with node A down"

note "restoring node A..."
$A up -d >/dev/null 2>&1
sleep 45
MASTERS=0
for c in petafs-mh-redis-primary petafs-mh-redis-replica; do
  docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null | grep -q '^role:master' \
    && MASTERS=$((MASTERS+1))
done
[ "$MASTERS" -eq 1 ] && ok "exactly one master after node A came back (no split brain)" \
                     || bad "$MASTERS masters after restore — split brain"

# The honest part. Losing node A took the JobManager and the object store with
# it, so unlike drill 2 the job does NOT come back by itself — there is no
# JobManager HA here and the checkpoints it would recover from lived on the
# node that died. Resubmission is a manual step, and asserting it is what keeps
# that limitation documented as a procedure rather than a surprise.
note "node A hosted the JobManager and MinIO — the job needs resubmitting"
for _ in $(seq 1 40); do curl -fs -m 3 http://localhost:8081/overview >/dev/null 2>&1 && break; sleep 5; done
$A exec -T jobmanager flink run -d -py /opt/project/src/flink/streaming_job.py \
  --no-wait --sink redis --topic "$TOPIC" \
  --bootstrap "host.docker.internal:19092,host.docker.internal:19192,host.docker.internal:19292" \
  >/dev/null 2>&1
JID3=""
for _ in $(seq 1 40); do JID3=$(flink_running_job); [ -n "$JID3" ] && break; sleep 5; done
[ -n "$JID3" ] && ok "pipeline resumes on the restored node A (job $JID3)" \
               || bad "pipeline did not restart after node A returned"

printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
