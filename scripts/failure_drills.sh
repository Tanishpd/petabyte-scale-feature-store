#!/usr/bin/env bash
# Failure drills for the production topology. `make prod-drill`.
#
# Every claim in the README's production-readiness table is made by one of these
# drills, not by reading config. Each kills something and asserts the property
# that was supposed to survive it.
#
# Drills are destructive by design. They run against docker-compose.prod.yml,
# never the dev stack.
set -uo pipefail

cd "$(dirname "$0")/.."
P="docker compose -f docker/docker-compose.prod.yml"

# Credentials come from the same files the topology mounts (`make secrets`),
# never from a dev default. A drill that quietly fell back to a hardcoded
# password would pass against a stack whose real credentials it never touched —
# and would keep passing after those credentials were rotated.
SECRETS=docker/secrets
_secret() { [ -f "$SECRETS/$1" ] || { echo "missing $SECRETS/$1 — run 'make secrets'" >&2; exit 1; }; tr -d '\r\n' < "$SECRETS/$1"; }
PW=$(_secret redis_password)
KAFKA_PW=$(_secret kafka_password)
STOREPASS=$(_secret tls_storepass)
# Redis and Sentinel are TLS-only in the production topology (`port 0` disables
# the plaintext listener), so every redis-cli call needs --tls and the CA.
# --user is REQUIRED now that `default` is off on the data nodes: a bare
# `-a $PW` authenticates a DISABLED user, and every Redis probe would then
# report the store as unreachable rather than as locked down.
RCLI="redis-cli --tls --cacert /tls/ca.crt --user petafs-admin"

pass=0; fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }
# Neither pass nor fail. A drill whose PRECONDITION was never met has not
# discovered anything about the system, and scoring it either way is a lie:
# `bad` blames the component for the setup, `ok` claims evidence that does not
# exist. Say so instead.
note() { printf '  \033[90m%s\033[0m\n' "$*"; }

# Every kafka CLI call needs credentials now that SASL is on. Without them the
# call returns nothing, which reads as "no data" rather than "not authenticated"
# — that silently skipped Drill 1 the first time.
kafka_client_config() {
  $P exec -T kafka1 sh -c "cat > /tmp/drill.properties <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USER:-petafs}\" password=\"$KAFKA_PW\";
ssl.truststore.location=/tls/kafka.truststore.jks
ssl.truststore.password=$STOREPASS
EOF" >/dev/null 2>&1
}
kafka_records() {
  $P exec -T kafka1 /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka1:9092 --topic "${1:-transactions}" \
    --command-config /tmp/drill.properties 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}
sentinel_master() {
  docker exec petafs-prod-redis-sentinel-1 $RCLI -p 26379 -a "$PW" \
    SENTINEL get-master-addr-by-name mymaster 2>/dev/null | head -1
}
count_masters() {
  n=0
  for c in primary replica; do
    docker exec "petafs-prod-redis-$c" $RCLI -a "$PW" INFO replication 2>/dev/null \
      | grep -q "^role:master" && n=$((n+1))
  done
  echo "$n"
}

# ---------------------------------------------------------------------------
kafka_client_config
step "Drill 1 — lose a Kafka broker"
# RF=3 with min.insync.replicas=2 should ride this out: leadership moves, ISR
# shrinks to 2, and nothing is lost. At RF=1 the same kill loses a third of the
# partitions permanently.
BEFORE=$(kafka_records)
if [ "${BEFORE:-0}" -eq 0 ]; then
  echo "  (no data in 'transactions' — produce some first; skipping)"
else
  docker kill petafs-prod-kafka2 >/dev/null 2>&1
  sleep 20
  AFTER=$(kafka_records)
  [ "$AFTER" = "$BEFORE" ] && ok "no data loss ($AFTER records)" || bad "records $BEFORE -> $AFTER"

  # Count partitions whose ISR has exactly 2 members. Anchoring with `$` fails:
  # the describe line continues with Elr: / LastKnownElr: columns.
  UNDER=$($P exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
          --describe --topic transactions --command-config /tmp/drill.properties 2>/dev/null \
          | grep -oE "Isr: [0-9]+(,[0-9]+)*" | awk -F, 'NF==2' | wc -l | tr -d " ")
  [ "${UNDER:-0}" -gt 0 ] && ok "ISR shrank to 2 but stayed at min.insync.replicas" \
                          || bad "unexpected ISR state"

  $P up -d kafka2 >/dev/null 2>&1
  for _ in $(seq 1 20); do
    FULL=$($P exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
           --describe --topic transactions --command-config /tmp/drill.properties 2>/dev/null \
           | grep -oE "Isr: [0-9]+(,[0-9]+)*" | awk -F, 'NF==3' | wc -l | tr -d " ")
    [ "${FULL:-0}" -ge 6 ] && break
    sleep 5
  done
  [ "${FULL:-0}" -ge 6 ] && ok "broker rejoined, ISR back to 3" || bad "broker did not rejoin"
fi

# ---------------------------------------------------------------------------
step "Drill 2 — lose the Redis master"
# Sentinel should promote the replica. The sink discovers the new master through
# Sentinel rather than a fixed host, so writes resume instead of failing READONLY.
M0=$(sentinel_master)
if [ -z "$M0" ]; then
  bad "sentinel is not reporting a master — check credentials"
else
  echo "  master before: $M0"
  # Pick the victim by asking each node its role, not by matching addresses.
  # Sentinel is configured with `announce-hostnames yes` so it reports
  # "redis-primary", while docker inspect gives an IP — comparing the two never
  # matches and silently killed the replica every time, making a correct
  # "no promotion" look like a failure.
  VICTIM=""
  for c in primary replica; do
    if docker exec "petafs-prod-redis-$c" $RCLI -a "$PW" INFO replication 2>/dev/null \
       | grep -q "^role:master"; then VICTIM="petafs-prod-redis-$c"; fi
  done
  if [ -z "$VICTIM" ]; then bad "no node reports role:master — cannot pick a victim"; VICTIM=petafs-prod-redis-primary; fi
  BEFORE=$(docker exec "$VICTIM" $RCLI -a "$PW" DBSIZE 2>/dev/null | tr -d '\r')
  docker kill "$VICTIM" >/dev/null 2>&1
  echo "  killed $VICTIM (held ${BEFORE:-0} keys)"

  M1=""
  for _ in $(seq 1 20); do
    M1=$(sentinel_master); [ -n "$M1" ] && [ "$M1" != "$M0" ] && break
    sleep 3
  done
  [ -n "$M1" ] && [ "$M1" != "$M0" ] && ok "sentinel promoted a new master ($M0 -> $M1)" \
                                     || bad "no promotion (master still $M1)"

  SURV=$(docker exec petafs-prod-redis-replica $RCLI -a "$PW" DBSIZE 2>/dev/null \
      || docker exec petafs-prod-redis-primary $RCLI -a "$PW" DBSIZE 2>/dev/null)
  # $BEFORE is captured above, ahead of the kill. Without it this assertion
  # compares against nothing: on a stack where the pipeline has not yet written
  # a feature it reported "no keys after failover" and blamed Redis for handing
  # back the empty database it was given. Windows have to have FIRED first —
  # a single bounded burst of events is not enough, because the watermark never
  # advances past the last window (see CLAUDE.md).
  if [ "${BEFORE:-0}" -eq 0 ]; then
    note "no features in Redis before the kill — nothing to survive, so this proves nothing."
    note "  produce, wait for a window to fire, then re-run to assert data survival."
  elif [ "${SURV:-0}" -gt 0 ]; then
    ok "features survived the failover ($SURV keys, $BEFORE before)"
  else
    bad "$BEFORE keys before the failover, none after"
  fi

  # The promotion must SURVIVE the old primary coming back. With a
  # command-line --replicaof the argument reasserts on restart and fights
  # Sentinel; enough failovers and both nodes end up replicas with no master.
  # Redis persists a promotion via CONFIG REWRITE, which needs a config FILE.
  $P up -d redis-primary redis-replica >/dev/null 2>&1
  sleep 25
  M=$(count_masters)
  [ "$M" -eq 1 ] && ok "exactly one master after the old primary rejoined (no split brain)" \
                 || bad "$M masters after restart — split brain"

  AGREE=$(for n in 1 2 3; do
            docker exec "petafs-prod-redis-sentinel-$n" $RCLI -p 26379 -a "$PW" \
              SENTINEL get-master-addr-by-name mymaster 2>/dev/null | head -1
          done | sort -u | wc -l | tr -d " ")
  [ "${AGREE:-0}" -eq 1 ] && ok "all three sentinels agree on the master" \
                          || bad "sentinels disagree ($AGREE distinct answers)"
fi

# ---------------------------------------------------------------------------
step "Drill 3 — lose a TaskManager (state recovery from object storage)"
JID=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json,sys
try: print(next((j['jid'] for j in json.load(sys.stdin).get('jobs',[]) if j['state']=='RUNNING'),''))
except Exception: print('')")
if [ -z "$JID" ]; then
  echo "  (no running job — 'make prod-submit' first; skipping)"
else
  docker kill petafs-prod-taskmanager-1 >/dev/null 2>&1
  $P up -d taskmanager >/dev/null 2>&1
  R=""
  for _ in $(seq 1 30); do
    R=$(curl -s "http://localhost:8081/jobs/$JID/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try: print(((json.load(sys.stdin).get('latest') or {}).get('restored') or {}).get('external_path',''))
except Exception: print('')")
    [ -n "$R" ] && break
    sleep 8
  done
  case "$R" in
    s3://*) ok "restored from object storage: $R" ;;
    "")     bad "no restore recorded" ;;
    *)      bad "restored from non-S3 path: $R" ;;
  esac
fi

# ---------------------------------------------------------------------------
step "Drill 4 — authentication is actually enforced"
if docker exec petafs-prod-redis-sentinel-1 $RCLI -p 26379 PING 2>&1 | grep -q NOAUTH; then
  ok "sentinel rejects unauthenticated clients"
else
  bad "sentinel answered without credentials"
fi
if docker exec petafs-prod-redis-primary $RCLI PING 2>&1 | grep -q NOAUTH; then
  ok "redis rejects unauthenticated clients"
else
  bad "redis answered without credentials"
fi

# Kafka: an authenticated client must work and an unauthenticated one must not.
$P exec -T kafka1 sh -c 'printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"'"${KAFKA_USER:-petafs}"'\" password=\"'"$KAFKA_PW"'\";\n" > /tmp/d.properties' >/dev/null 2>&1
if $P exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
     --command-config /tmp/drill.properties --list >/dev/null 2>&1; then
  ok "kafka accepts an authenticated client"
else
  bad "kafka refused a correctly authenticated client"
fi
if $P exec -T kafka1 sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list --command-config /dev/null 2>&1 | head -1' 2>/dev/null | grep -qiE "list|^$"; then
  # An unauthenticated client cannot list topics; anything non-empty here is a
  # timeout/error string rather than a topic list.
  UNAUTH=$($P exec -T kafka1 sh -c 'timeout 25 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list 2>/dev/null | wc -l' 2>/dev/null | tr -d " ")
  [ "${UNAUTH:-0}" -eq 0 ] && ok "kafka rejects unauthenticated clients" \
                           || bad "kafka listed $UNAUTH topics without credentials"
fi

# ---------------------------------------------------------------------------
step "Drill 5 — transport is encrypted, not just authenticated"
# Kafka: a client with the CORRECT credentials but no TLS must still be refused.
# SASL_PLAINTEXT would prove identity while leaving every event and the password
# itself readable on the wire.
$P exec -T kafka1 sh -c "cat > /tmp/nossl.properties <<'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USER:-petafs}\" password=\"$KAFKA_PW\";
EOF" >/dev/null 2>&1
if $P exec -T kafka1 sh -c 'timeout 25 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --command-config /tmp/nossl.properties --list >/dev/null 2>&1'; then
  bad "kafka served an unencrypted client that had valid credentials"
else
  ok "kafka refuses unencrypted clients even with valid credentials"
fi
if $P exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
     --command-config /tmp/drill.properties --list >/dev/null 2>&1; then
  ok "kafka serves clients over SASL_SSL"
else
  bad "kafka refused a correctly configured SASL_SSL client"
fi
# `port 0` removes the plaintext listener entirely. A password-only setup would
# still put every feature value and the password itself on the wire in clear.
# Assert on the exit code: the error text ("I/O error", "Connection reset",
# "protocol error") varies by redis-cli version, and matching it made this
# check fail against a server that was correctly refusing the connection.
if docker exec petafs-prod-redis-primary sh -c "redis-cli -a '$PW' PING >/dev/null 2>&1"; then
  bad "redis answered a plaintext client — transport is not encrypted"
else
  ok "redis refuses plaintext connections (TLS-only)"
fi
if docker exec petafs-prod-redis-primary $RCLI -a "$PW" PING 2>/dev/null | grep -q PONG; then
  ok "redis answers over TLS"
else
  bad "redis did not answer over TLS"
fi
# Check whichever node is CURRENTLY the replica. Drill 2 may have swapped the
# roles, and a master has no master_link_status at all — hardcoding the
# container name reported a healthy link as down.
# POLLED, not read once. Drill 2 above promoted a replica and brought the old
# master back, and a rejoining node reports master_link_status:down while it is
# doing the full resync — reading the field immediately after a failover
# measures the resync, not the health of the link. Seen failing at 'down' with
# "MASTER <-> REPLICA sync: Finished with success" already in the log seconds
# later. Converging state has to be polled to a deadline, never sampled once.
LINK=""
for _ in $(seq 1 60); do
  for c in primary replica; do
    INFO=$(docker exec "petafs-prod-redis-$c" $RCLI -a "$PW" INFO replication 2>/dev/null)
    echo "$INFO" | grep -q "^role:slave" || continue
    LINK=$(echo "$INFO" | grep -o "master_link_status:[a-z]*" | cut -d: -f2)
  done
  [ "$LINK" = "up" ] && break
  sleep 3
done
if [ "$LINK" = "up" ]; then
  ok "replication link is up over TLS (replica -> master)"
else
  # Report what the MASTER thinks the sync is doing. "down" alone sends you
  # looking at TLS and credentials; `state=wait_bgsave` says the opposite —
  # the link is fine and the full resync has not finished. Drill 2 above forces
  # exactly that by promoting a replica and bringing the old master back, and on
  # a loaded box the RDB snapshot and transfer take longer than you would guess.
  SUB=$(for c in primary replica; do
          docker exec "petafs-prod-redis-$c" $RCLI -a "$PW" INFO replication 2>/dev/null \
            | grep -o "state=[a-z_]*"
        done | head -1)
  bad "TLS replication link is '${LINK:-absent}' after 180s (master-side sync ${SUB:-unknown})"
fi

printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
