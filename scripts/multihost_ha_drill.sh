#!/usr/bin/env bash
# JobManager high-availability drills. `make mh-ha-drill`.
#
# scripts/multihost_drill.sh kills a whole node and shows the DATA survives —
# but the job there stops, because that topology has one JobManager and losing
# its node ends the pipeline until a human resubmits.
#
# This drill kills the node running the LEADING JobManager and asserts nobody
# has to do anything: the standby takes the ZooKeeper latch, recovers the job
# graph from the HA store, and redeploys onto a surviving TaskManager.
#
# The leader is discovered, never assumed. In Flink HA there is no "standby" in
# the configuration — both JobManagers are identical and contend for the same
# latch — so which one leads is runtime state. Killing a hardcoded node would
# half the time kill the standby and prove nothing.
#
# Destructive by design. Runs against docker-compose.multihost-ha.yml only.
set -uo pipefail
cd "$(dirname "$0")/.."

MH="docker compose -f docker/docker-compose.multihost-ha.yml --env-file docker/multihost/sim-ha.env"
A="$MH -p petafs-ha-a --profile a"
B="$MH -p petafs-ha-b --profile b"
C="$MH -p petafs-ha-c --profile c"

# Credentials come from the same files the topology mounts (`make secrets`),
# never from a dev default. A drill that silently fell back to a hardcoded
# password would pass against a stack whose real credentials it never touched.
SECRETS=docker/secrets
_secret() { [ -f "$SECRETS/$1" ] || { echo "missing $SECRETS/$1 — run 'make secrets'" >&2; exit 1; }; tr -d '\r\n' < "$SECRETS/$1"; }
PW=$(_secret redis_password)
KAFKA_PW=$(_secret kafka_admin_password)
STOREPASS=$(_secret tls_storepass)
# --user is REQUIRED now that `default` is off — a bare `-a $PW` authenticates
# as a disabled user, and every Redis probe in this drill would report the
# store as unreachable rather than as locked down.
RCLI="redis-cli --tls --cacert /tls/ca.crt --user petafs-admin"
JM_B_PORT="${JM_B_REST_PORT:-8081}"
JM_C_PORT="${JM_C_REST_PORT:-8082}"
TOPIC="${TOPIC:-transactions}"

pass=0; fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }
note() { printf '  \033[90m%s\033[0m\n' "$*"; }

http_code() { curl -s -o /dev/null -m 5 -w '%{http_code}' "$1" 2>/dev/null; }

# ASK ZOOKEEPER, NOT THE REST API.
#
# It is tempting to call the leader "the JobManager whose REST answers 200",
# and that is wrong: in steady state BOTH JobManagers answer 200 and return
# identical job lists, because a non-leading JobManager proxies to the leader.
# (A 503 only appears in the window before its REST leader-contender has
# started, which is why a check written during startup looks correct and then
# silently stops discriminating.) ZooKeeper holds the actual leader address in
# the connection_info node, and it is the only authority.
#
# Tries each ZooKeeper in turn: the drill kills a whole node, and that node's
# ZooKeeper goes with it.
zk_leader_port() {
  for z in petafs-mhha-zk1 petafs-mhha-zk2 petafs-mhha-zk3; do
    docker ps --format '{{.Names}}' | grep -q "^${z}$" || continue
    P=$(docker exec "$z" sh -c 'zkCli.sh -server localhost:$ZOO_PORT get /flink/petafs-mh-ha/leader/rest_server/connection_info' 2>/dev/null \
        | grep -aoE 'http://[^ "]+:[0-9]+' | head -1 | sed 's/.*://')
    [ -n "$P" ] && { echo "$P"; return; }
  done
}
leader_port() { zk_leader_port; }
port_to_node() { [ "$1" = "$JM_B_PORT" ] && echo b || echo c; }

running_job() {  # running_job <port>
  curl -s -m 5 "http://localhost:$1/jobs/overview" 2>/dev/null | python3 -c "
import json,sys
try: print(next((j['jid'] for j in json.load(sys.stdin).get('jobs',[]) if j['state']=='RUNNING'),''))
except Exception: print('')"
}
tm_count() {
  curl -s -m 5 "http://localhost:$1/overview" 2>/dev/null | python3 -c "
import json,sys
try: print(json.load(sys.stdin).get('taskmanagers',0))
except Exception: print(0)"
}
zk_mode() { docker exec "$1" sh -c 'echo mntr | nc -w 2 localhost $ZOO_PORT' 2>/dev/null \
              | awk -F'\t' '/zk_server_state/ {print $2}' | tr -d '\r'; }

# Talk to the object store from node A's network. Node A always survives this
# drill (the victim is the LEADING JobManager's node, which is B or C), so this
# stays a valid vantage point after the kill.
mc_a() {
  docker run --rm --entrypoint sh --network petafs-ha-a_default \
    minio/mc:RELEASE.2025-04-16T18-13-26Z -c \
    "mc alias set p http://minio-a:${MINIO_A_PORT:-19000} ${MINIO_USER:-petafs} $(_secret minio_root_password) >/dev/null 2>&1; $1" \
    2>/dev/null | grep -v "older version\|Update:"
}
# `mc admin info --json` nests everything under "info" — the backend block is at
# d["info"]["backend"], not d["backend"]. Reading the wrong level yields 0 and
# looks exactly like "the whole pool is offline".
minio_online() { mc_a "mc admin info p --json" | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
    print((d.get('info') or d).get('backend',{}).get('onlineDisks',0))
except Exception: print(0)" 2>/dev/null; }
minio_sets() { mc_a "mc admin info p --json" | python3 -c "
import json,sys
try:
    b = (json.load(sys.stdin).get('info') or {}).get('backend',{})
    print('%s set(s) of %s drives' % (b.get('totalSets'), b.get('totalDrivesPerSet')))
except Exception: print('unknown')" 2>/dev/null; }
checkpoints_completed() {  # <port> <jid>
  curl -s -m 5 "http://localhost:$1/jobs/$2/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try: print((json.load(sys.stdin).get('counts') or {}).get('completed',0))
except Exception: print(0)"
}

redis_master_container() {
  for c in petafs-mhha-redis-primary petafs-mhha-redis-replica; do
    docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null \
      | grep -q '^role:master' && { echo "$c"; return; }
  done
}
# Lua so the binary Feast keys never leave Redis — see docs/MULTIHOST.md §7.
max_ttl() {
  local m; m=$(redis_master_container); [ -z "$m" ] && { echo -1; return; }
  docker exec "$m" $RCLI -a "$PW" EVAL \
    "local c=redis.call('SCAN',0,'COUNT',300); local m=-2;
     for _,k in ipairs(c[2]) do local t=redis.call('TTL',k); if t>m then m=t end end; return m" 0 \
    2>/dev/null | tr -d '\r'
}

# Write a client config for an arbitrary principal, so the drill can ask "what
# happens if THIS identity tries that" rather than only exercising the happy path.
as_principal() {  # <principal> <secret-file>
  docker exec petafs-mhha-kafka1 sh -c "cat > /tmp/$1.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$1\" password=\"$(_secret "$2")\";
ssl.truststore.location=/tls/kafka.truststore.jks
ssl.truststore.password=$STOREPASS
EOF" >/dev/null 2>&1
}
# Any *AuthorizationException is a denial; which one depends on the request the
# client made first (a consumer trips GROUP before TOPIC, an idempotent producer
# trips CLUSTER). Matching one specific exception would make the assertion
# brittle for no gain.
#
# THREE-VALUED, and that is the load-bearing part. The Kafka CLI answers a call
# it cannot complete with SILENCE, not an error — the same trap documented for
# unauthenticated calls in docs/MULTIHOST.md §7, which also applies to a broker
# that is merely too sick to reply. A denied consumer does not exit: it retries
# the fetch forever and logs nothing at the default level. Two-valued logic
# reads that silence as "no exception seen → allowed", i.e. it reports a
# BOUNDARY BREACH when the truth is "no answer", and burns unbounded wall-clock
# on the way: an early version of this drill sat on one such probe for 11
# minutes before reporting a failure that had not happened.
#
# `timeout` runs INSIDE the container on purpose. Killing the `docker exec` from
# the host leaves the JVM running, so the next probe queues behind a client that
# is still hammering the broker we are trying to measure.
#
# It also has to wrap the WHOLE command, hence `timeout ... sh -c`. Written the
# other way round — `sh -c "timeout N cmd | other"` — the shell parses the
# pipeline first and `timeout` binds to the leftmost command only, so a probe
# that pipes into kafka-console-producer bounds the `printf` and lets the JVM
# run forever. That is not a hypothetical: it hung this drill for two minutes
# per probe until the ordering was fixed. coreutils `timeout` puts its child in
# its own process group and signals the group, so the JVM dies with the shell.
#
# 40s, not 25: every one of these probes pays a cold JVM start, and on a box
# already running the pipeline that alone can exceed 20s. Too short a timeout
# manufactures "indeterminate" verdicts that look like broker trouble.
PROBE_TIMEOUT=${PROBE_TIMEOUT:-40}
probe() {  # <shell-cmd> -> denied | allowed | indeterminate
  local out rc
  out=$(docker exec petafs-mhha-kafka1 timeout -s KILL "$PROBE_TIMEOUT" sh -c "$1" 2>&1); rc=$?
  if printf '%s' "$out" | grep -qiE "AuthorizationException|not authorized"; then
    echo denied
  elif [ "$rc" -eq 137 ] || [ "$rc" -eq 124 ]; then
    echo indeterminate
  else
    echo allowed
  fi
}
# An indeterminate verdict fails the drill, but with its own wording: "the ACL
# did not hold" and "the cluster never answered" need different fixes, and
# collapsing them sends you to read ACLs when the real problem is a broker.
must_deny() {   # <description> <shell-cmd>
  case "$(probe "$2")" in
    denied)        ok  "$1" ;;
    allowed)       bad "$1 — IT SUCCEEDED" ;;
    indeterminate) bad "$1 — no verdict in ${PROBE_TIMEOUT}s; broker degraded, not an ACL result" ;;
  esac
}
must_allow() {  # <description> <shell-cmd>
  case "$(probe "$2")" in
    allowed)       ok  "$1" ;;
    denied)        bad "$1 — it was REFUSED; the ACLs are too tight" ;;
    indeterminate) bad "$1 — no verdict in ${PROBE_TIMEOUT}s; broker degraded, not an ACL result" ;;
  esac
}

# ===========================================================================
step "Drill 0 — the three nodes are isolated"
# ===========================================================================
NETS=$(for n in a b c; do docker network ls --filter "name=petafs-ha-$n" --format '{{.ID}}'; done | sort -u | wc -l | tr -d ' ')
[ "${NETS:-0}" -eq 3 ] && ok "three separate Docker networks, one per node" \
                       || bad "expected 3 distinct networks, found ${NETS:-0}"
if docker exec petafs-mhha-taskmanager-c python3 -c "import socket;socket.gethostbyname('kafka1')" >/dev/null 2>&1; then
  bad "node C resolved node A's service name — isolation is fake"
else
  ok "node C cannot resolve node A's service names (no shared Compose DNS)"
fi

# ===========================================================================
step "Drill 1 — credentials are not visible through the orchestrator"
# ===========================================================================
# The point of moving these to files rather than `environment:`. Anyone who can
# talk to the Docker daemon can read a container's environment; a file mounted
# at /run/secrets is visible only to the processes in that container.
IDS=$(docker ps -q --filter name=petafs-mhha- 2>/dev/null)
LEAKED=""
for s in redis_password kafka_admin_password kafka_flink_password tls_storepass minio_root_password; do
  V=$(_secret "$s")
  [ -n "$IDS" ] && docker inspect $IDS 2>/dev/null | grep -qF -- "$V" && LEAKED="$LEAKED $s"
done
[ -z "$LEAKED" ] && ok "no credential appears anywhere in \`docker inspect\`" \
                 || bad "credentials readable via docker inspect:$LEAKED"

if $A config 2>/dev/null | grep -qF -- "$(_secret redis_password)"; then
  bad "the Redis password appears in \`docker compose config\`"
else
  ok "no credential appears in \`docker compose config\`"
fi

# ...and they really are present where the processes need them, so the check
# above is not passing merely because nothing was wired up.
if docker exec petafs-mhha-redis-primary sh -c 'test -s /run/secrets/redis_password' 2>/dev/null; then
  ok "the secret IS mounted inside the container that needs it"
else
  bad "/run/secrets/redis_password missing inside redis-primary"
fi

# ===========================================================================
step "Drill 2 — authentication is not authorization"
# ===========================================================================
# SASL proves who a client is. Without an authorizer it then permits everything
# that identity might ask for, including deleting the topic the whole pipeline
# depends on. These assert the boundary in both directions: the happy path must
# still work, and everything outside it must not.
as_principal petafs-admin kafka_admin_password
as_principal petafs-producer kafka_producer_password
as_principal petafs-flink kafka_flink_password

# What the write probes put on the wire. It has to be a SCHEMA-VALID event, not
# a marker string: this is the live `transactions` topic, and the Flink source
# deserializes JSON. A probe that wrote `drill` poisoned the stream — the job
# died on `Unrecognized token 'drill'`, entered a restart loop, and every later
# assertion in this drill then failed for a reason that had nothing to do with
# what it was testing. The positive control must prove the WRITE is permitted
# without changing what the pipeline computes.
#
# event_timestamp is deliberately ancient, so the record parses, counts as a
# write, and is then dropped as late by the window — it can never perturb a
# feature value the later drills assert on.
PROBE_EVENT='{"event_id":"drill-probe-000000001","user_id":1,"merchant_id":1,"merchant_name":"drill-probe","merchant_category":"drill-probe","amount":1.0,"currency":"USD","country":"US","device":"drill-probe","is_card_present":true,"event_timestamp":"2000-01-01T00:00:00.000"}'

# Positive controls first. A rule set that denies everything would pass every
# negative assertion below while leaving the pipeline dead, so each principal's
# real job is asserted before its limits are.
must_allow "producer principal CAN write $TOPIC" \
  "printf '%s\n' '$PROBE_EVENT' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $TOPIC --producer.config /tmp/petafs-producer.properties"
# Reads a partition directly rather than joining a group: this isolates the
# TOPIC Read permission instead of tripping over the group ACL first, so the
# allowed and denied cases below differ in exactly one thing.
#
# WHICH partition has to be discovered, not assumed. console-producer leaves the
# choice to the partitioner, so the record just written can land on any of the
# three — and a `--partition 0` that happens to be empty blocks forever, which
# this drill would then (correctly, but uselessly) report as "the broker never
# answered". Ask for a partition whose end offset is non-zero instead.
READ_PART=$(docker exec petafs-mhha-kafka1 timeout -s KILL "$PROBE_TIMEOUT" \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --command-config /tmp/petafs-admin.properties 2>/dev/null | awk -F: '$3 > 0 { print $2; exit }')
READ_PART=${READ_PART:-0}
must_allow "flink principal CAN read $TOPIC (partition $READ_PART)" \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --partition $READ_PART --offset earliest --max-messages 1 --consumer.config /tmp/petafs-flink.properties"

# ...and now the limits. Same command as the positive control above, different
# identity.
must_deny "producer principal CANNOT read $TOPIC (partition $READ_PART)" \
  "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --partition $READ_PART --offset earliest --max-messages 1 --consumer.config /tmp/petafs-producer.properties"
must_deny "producer principal CANNOT delete $TOPIC" \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $TOPIC --command-config /tmp/petafs-producer.properties"
# Same valid payload as the positive control: if this ACL ever regressed, the
# drill would report the breach WITHOUT also poisoning the topic on its way out.
must_deny "flink principal CANNOT write $TOPIC (read-only consumer)" \
  "printf '%s\n' '$PROBE_EVENT' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $TOPIC --producer.config /tmp/petafs-flink.properties"
# The delete must have been REFUSED, not merely errored — an exception in the
# client proves nothing about what the broker did.
kbin_exists=$(docker exec petafs-mhha-kafka1 timeout -s KILL "$PROBE_TIMEOUT" \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
  --command-config /tmp/petafs-admin.properties 2>/dev/null | grep -c "^$TOPIC$")
[ "${kbin_exists:-0}" -eq 1 ] && ok "$TOPIC survived the unauthorized delete attempt" \
                             || bad "$TOPIC is gone — a delete succeeded"

# --- and the same question, asked of Redis ---------------------------------
#
# Kafka was only half the gap. The online store held ONE password with full
# command access, so every component that could write a feature could also read
# the whole keyspace or FLUSHALL it. These assert the split from the outside:
# by what each credential can actually do, not by reading the ACL file back
# (which would only prove the file says what we wrote in it).
#
# No timeout wrapper needed here — redis-cli answers or fails immediately. The
# silent-retry problem that forced the three-valued probe() above is a Kafka
# client behaviour, not a Redis one.
rprobe() {  # <user> <secret-file> <args...> -> the server's reply, or NOAUTH/NOPERM
  local u="$1" s="$2"; shift 2
  docker exec "$RMASTER" redis-cli --tls --cacert /tls/ca.crt \
    --user "$u" -a "$(_secret "$s")" --no-auth-warning "$@" 2>&1 | tr -d '\r'
}
RMASTER=$(redis_master_container)
if [ -z "$RMASTER" ]; then
  bad "no Redis master reachable — cannot assert the online store's ACLs"
else
  note "asserting Redis ACLs against the current master (${RMASTER#petafs-mhha-})"

  # `default` is the account a client gets without asking for one. If it is
  # still enabled, every rule below is decoration.
  D=$(docker exec "$RMASTER" redis-cli --tls --cacert /tls/ca.crt \
        -a "$(_secret redis_password)" --no-auth-warning PING 2>&1 | tr -d '\r')
  case "$D" in
    *WRONGPASS*|*NOAUTH*|*disabled*|*"no password"*)
      ok "the default Redis user is disabled (answered: $(printf '%s' "$D" | head -c 40))" ;;
    PONG) bad "the default Redis user still works — one password is still full access" ;;
    *)    bad "unexpected reply from the default user: $D" ;;
  esac

  # Positive control: the sink's own credential must still do the sink's job,
  # or a "secure" store is just a broken one.
  W=$(rprobe petafs-flink redis_flink_password HSET petafs:drill:probe f v)
  case "$W" in
    0|1) ok "petafs-flink CAN write a feature hash (HSET -> $W)" ;;
    *)   bad "petafs-flink cannot HSET: $W" ;;
  esac
  E=$(rprobe petafs-flink redis_flink_password EXPIRE petafs:drill:probe 60)
  [ "$E" = "1" ] && ok "petafs-flink CAN set a TTL (EXPIRE -> 1)" \
                 || bad "petafs-flink cannot EXPIRE: $E"

  # ...and nothing else. FLUSHALL is the one that matters: it is the difference
  # between a leaked TaskManager credential costing you a restart and costing
  # you the entire online store.
  F=$(rprobe petafs-flink redis_flink_password FLUSHALL)
  case "$F" in
    *NOPERM*) ok "petafs-flink CANNOT FLUSHALL the online store" ;;
    *)        bad "petafs-flink was allowed to FLUSHALL: $F" ;;
  esac
  S=$(rprobe petafs-flink redis_flink_password SCAN 0)
  case "$S" in
    *NOPERM*) ok "petafs-flink CANNOT SCAN the keyspace it writes to" ;;
    *)        bad "petafs-flink was allowed to SCAN: $(printf '%s' "$S" | head -c 60)" ;;
  esac
  # The serving path reads. It must not be able to rewrite what it reads.
  R=$(rprobe petafs-reader redis_reader_password HGET petafs:drill:probe f)
  [ "$R" = "v" ] && ok "petafs-reader CAN read a feature (HGET -> $R)" \
                 || bad "petafs-reader cannot HGET: $R"
  RW=$(rprobe petafs-reader redis_reader_password HSET petafs:drill:probe f tampered)
  case "$RW" in
    *NOPERM*) ok "petafs-reader CANNOT write to the online store" ;;
    *)        bad "petafs-reader was allowed to HSET: $RW" ;;
  esac
  docker exec "$RMASTER" redis-cli --tls --cacert /tls/ca.crt --user petafs-admin \
    -a "$(_secret redis_password)" --no-auth-warning DEL petafs:drill:probe >/dev/null 2>&1
fi

# ===========================================================================
step "Drill 3 — the HA machinery is formed across the three nodes"
# ===========================================================================
MODES=$(for n in 1 2 3; do zk_mode "petafs-mhha-zk$n"; done | sort | tr '\n' ' ')
LEADERS=$(echo "$MODES" | tr ' ' '\n' | grep -c '^leader$')
FOLLOWERS=$(echo "$MODES" | tr ' ' '\n' | grep -c '^follower$')
[ "$LEADERS" = "1" ] && [ "$FOLLOWERS" = "2" ] \
  && ok "ZooKeeper ensemble formed across three nodes (1 leader, 2 followers)" \
  || bad "ZooKeeper modes were: ${MODES:-none} (wanted 1 leader, 2 followers)"

LP=$(leader_port)
if [ -z "$LP" ]; then
  bad "neither JobManager is leading — nothing else can be asserted"
  printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"; exit 1
fi
LN=$(port_to_node "$LP")
OTHER_PORT=$([ "$LP" = "$JM_B_PORT" ] && echo "$JM_C_PORT" || echo "$JM_B_PORT")
OTHER_NODE=$(port_to_node "$OTHER_PORT")
OC=$(http_code "http://localhost:$OTHER_PORT/overview")
note "ZooKeeper names node $LN (:$LP) the leader; node $OTHER_NODE (:$OTHER_PORT) answers HTTP $OC"
# Both JobManagers must be ALIVE (the non-leader proxies, so it also answers
# 200) while ZooKeeper names exactly one of them leader. Asserting on the REST
# code alone cannot tell these apart, which is the point.
[ "$OC" = "200" ] || [ "$OC" = "503" ] \
  && ok "both JobManagers are live, and ZooKeeper names exactly one leader (node $LN)" \
  || bad "the non-leading JobManager answered '$OC' — it is not running"

# ZooKeeper only holds POINTERS. The JOB GRAPH itself lives in the HA store,
# and if it never got there the standby would win the latch with nothing to
# recover. Checked by listing the bucket, not by grepping the JobManager log:
# the "Created ... storage directory" line is only emitted the FIRST time the
# directory is created, so on any later run a log grep reports a perfectly
# populated store as missing.
HA_LS=$(mc_a "mc ls --recursive p/flink-ha")
if printf '%s' "$HA_LS" | grep -q submittedExecutionPlan; then
  ok "job graph is in the HA store — the standby has something to recover"
  note "$(printf '%s' "$HA_LS" | grep -oE '[a-zA-Z]+[0-9a-f]{8,}' | head -3 | tr '\n' ' ')"
else
  bad "no submittedExecutionPlan in s3://flink-ha — failover would recover nothing"
fi

D=$(minio_online)
if [ "${D:-0}" -eq 3 ]; then
  ok "object store is distributed across all three nodes (3 drives online)"
  # One set spanning all three nodes is the property that matters. Three sets
  # of one, or one set per node, would report the same drive count and lose
  # data the moment a node died.
  note "erasure layout: $(minio_sets)"
else
  bad "object store reports ${D:-0} drives online, expected 3"
fi

N=$(tm_count "$LP")
[ "${N:-0}" -ge 1 ] && ok "TaskManager registered with the leader via ZooKeeper ($N)" \
                    || bad "no TaskManager registered with the leader"

JOB=$(running_job "$LP")
[ -n "$JOB" ] && ok "pipeline is running (job $JOB)" \
              || note "no running job — 'make mh-ha-submit' first; recovery cannot be asserted"

# ===========================================================================
step "Drill 4 — kill the node running the LEADING JobManager"
# ===========================================================================
if [ -z "$JOB" ]; then
  note "skipped: no running job to recover"
else
  T0=$(max_ttl)
  note "before: leader on node $LN, job $JOB, max feature TTL ${T0}s"
  VICTIM=$([ "$LN" = "b" ] && echo "$B" || echo "$C")
  $VICTIM down -t 0 >/dev/null 2>&1
  note "node $LN is gone — with it the leading JobManager, a broker, a sentinel and a ZooKeeper"

  # Leadership should move on its own. No resubmission, no operator.
  # Watched in ZOOKEEPER, not over REST: the surviving JobManager already
  # answered 200 before the failover, so polling REST would "pass" instantly
  # and prove nothing at all.
  NEWLEADER=""
  for _ in $(seq 1 40); do
    NP=$(zk_leader_port)
    [ -n "$NP" ] && [ "$NP" = "$OTHER_PORT" ] && { NEWLEADER="$OTHER_NODE"; break; }
    sleep 5
  done
  [ -n "$NEWLEADER" ] \
    && ok "ZooKeeper moved leadership to node $OTHER_NODE automatically" \
    || bad "leadership did not move within 200 s (ZooKeeper still says :${NP:-unknown})"

  if [ -n "$NEWLEADER" ]; then
    MODES=$(for n in 1 2 3; do zk_mode "petafs-mhha-zk$n"; done | grep -cE 'leader|follower')
    [ "${MODES:-0}" -ge 2 ] && ok "ZooKeeper kept quorum on the two surviving nodes" \
                            || bad "ZooKeeper lost quorum ($MODES nodes responding)"

    JOB2=""
    for _ in $(seq 1 48); do JOB2=$(running_job "$OTHER_PORT"); [ -n "$JOB2" ] && break; sleep 5; done
    if [ -n "$JOB2" ]; then
      ok "pipeline recovered with NO resubmission (job $JOB2)"
      R=$(curl -s -m 5 "http://localhost:$OTHER_PORT/jobs/$JOB2/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try: print(((json.load(sys.stdin).get('latest') or {}).get('restored') or {}).get('external_path',''))
except Exception: print('')")
      case "$R" in
        s3://*) ok "recovered STATE from the HA store, not restarted from scratch"; note "$R" ;;
        *)      note "no restore path reported (${R:-none}) — job may have been rescheduled without state" ;;
      esac
      # The object store must still take WRITES with a node gone, not merely
      # serve reads. A pool that has gone read-only looks fine to a `mc ls` and
      # then silently fails the next checkpoint.
      if mc_a "echo after-the-kill | mc pipe p/flink-ha/drill-probe.txt >/dev/null 2>&1 && mc cat p/flink-ha/drill-probe.txt" \
           | grep -q after-the-kill; then
        ok "object store still accepts writes with a node down"
      else
        bad "object store would not accept a write after the node loss"
      fi
      D2=$(minio_online)
      [ "${D2:-0}" -eq 2 ] && ok "pool degraded to 2 of 3 drives and stayed serving" \
                           || note "pool reports ${D2:-0} drives online"

      CP0=$(checkpoints_completed "$OTHER_PORT" "$JOB2")
      sleep 75
      T1=$(max_ttl)
      [ "${T1:-0}" -gt 1600 ] && ok "features flowing again after failover (max TTL ${T1}s of 1800)" \
                              || bad "no recent feature writes after failover (max TTL ${T1:-none})"
      # The end-to-end proof that the object store survived: Flink is still
      # completing checkpoints INTO it after losing a node.
      CP1=$(checkpoints_completed "$OTHER_PORT" "$JOB2")
      [ "${CP1:-0}" -gt "${CP0:-0}" ] \
        && ok "checkpoints still completing into the degraded pool ($CP0 -> $CP1)" \
        || bad "no new checkpoints after the node loss ($CP0 -> $CP1)"
    else
      bad "job did not return to RUNNING within 240 s of the failover"
    fi
  fi

  note "restoring node $LN..."
  $VICTIM up -d >/dev/null 2>&1
  RESTORED=""
  for _ in $(seq 1 40); do
    RC=$(http_code "http://localhost:$LP/overview")
    { [ "$RC" = "503" ] || [ "$RC" = "200" ]; } && { RESTORED="$RC"; break; }
    sleep 5
  done
  [ -n "$RESTORED" ] && ok "the restored JobManager came back up (HTTP $RESTORED)" \
                     || bad "the restored JobManager never answered"
  # It must rejoin as a CONTENDER and NOT seize the latch back. A restored node
  # that grabs leadership causes a second, entirely avoidable failover — and
  # ZooKeeper is the only place that difference is visible.
  L2=$(zk_leader_port)
  [ -n "$L2" ] && [ "$(port_to_node "$L2")" = "$OTHER_NODE" ] \
    && ok "leadership stayed on node $OTHER_NODE; it did not bounce back" \
    || bad "leadership is now on '$(port_to_node "${L2:-0}")', expected node $OTHER_NODE to keep it"
fi

printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
