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
PW="${REDIS_PASSWORD:-petafs-dev-secret}"

pass=0; fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }

kafka_records() {
  $P exec -T kafka1 /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka1:9092 --topic "${1:-transactions}" 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}
sentinel_master() {
  docker exec petafs-prod-redis-sentinel redis-cli -p 26379 -a "$PW" \
    SENTINEL get-master-addr-by-name mymaster 2>/dev/null | head -1
}

# ---------------------------------------------------------------------------
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
          --describe --topic transactions 2>/dev/null \
          | grep -oE "Isr: [0-9]+(,[0-9]+)*" | awk -F, 'NF==2' | wc -l | tr -d " ")
  [ "${UNDER:-0}" -gt 0 ] && ok "ISR shrank to 2 but stayed at min.insync.replicas" \
                          || bad "unexpected ISR state"

  $P up -d kafka2 >/dev/null 2>&1
  for _ in $(seq 1 20); do
    FULL=$($P exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 \
           --describe --topic transactions 2>/dev/null \
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
  VICTIM=petafs-prod-redis-primary
  # `.Networks.petafs-prod_default` does not parse — the hyphen breaks the Go
  # template field path, it returns empty, and we end up killing the replica
  # while reporting it as the master. Use index with a quoted key.
  IP_P=$(docker inspect -f '{{index .NetworkSettings.Networks "petafs-prod_default" "IPAddress"}}' petafs-prod-redis-primary 2>/dev/null)
  if [ -z "$IP_P" ]; then bad "could not resolve redis-primary IP — check the network name"; fi
  [ -n "$IP_P" ] && [ "$M0" != "$IP_P" ] && VICTIM=petafs-prod-redis-replica
  docker kill "$VICTIM" >/dev/null 2>&1
  echo "  killed $VICTIM"

  M1=""
  for _ in $(seq 1 20); do
    M1=$(sentinel_master); [ -n "$M1" ] && [ "$M1" != "$M0" ] && break
    sleep 3
  done
  [ -n "$M1" ] && [ "$M1" != "$M0" ] && ok "sentinel promoted a new master ($M0 -> $M1)" \
                                     || bad "no promotion (master still $M1)"

  SURV=$(docker exec petafs-prod-redis-replica redis-cli -a "$PW" DBSIZE 2>/dev/null \
      || docker exec petafs-prod-redis-primary redis-cli -a "$PW" DBSIZE 2>/dev/null)
  [ "${SURV:-0}" -gt 0 ] && ok "features survived the failover ($SURV keys)" \
                         || bad "no keys after failover"

  $P up -d redis-primary redis-replica >/dev/null 2>&1
  sleep 10
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
if docker exec petafs-prod-redis-sentinel redis-cli -p 26379 PING 2>&1 | grep -q NOAUTH; then
  ok "sentinel rejects unauthenticated clients"
else
  bad "sentinel answered without credentials"
fi
if docker exec petafs-prod-redis-primary redis-cli PING 2>&1 | grep -q NOAUTH; then
  ok "redis rejects unauthenticated clients"
else
  bad "redis answered without credentials"
fi

printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
