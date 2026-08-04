#!/usr/bin/env bash
# Formation summary for the multi-host HA topology. `make mh-ha-health`.
#
# The question that matters here is not "are both JobManagers up" — they always
# are — but "which one holds LEADERSHIP". In Flink HA there is no standby role
# in the configuration: both processes contend for the same ZooKeeper leader
# latch and whichever wins is the leader. So leadership is runtime state, and
# the only honest way to read it is to ask each REST endpoint and see which one
# answers as a working cluster.
set -uo pipefail
cd "$(dirname "$0")/.."

PW="${REDIS_PASSWORD:-petafs-dev-secret}"
RCLI="redis-cli --tls --cacert /tls/ca.crt"
K1=petafs-mhha-kafka1
JM_B_PORT="${JM_B_REST_PORT:-8081}"
JM_C_PORT="${JM_C_REST_PORT:-8082}"

kafka_conf() {
  docker exec "$K1" sh -c "cat > /tmp/mh.properties <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USER:-petafs}\" password=\"${KAFKA_PASSWORD:-petafs-dev-secret}\";
ssl.truststore.location=/tls/kafka.truststore.jks
ssl.truststore.password=${TLS_STOREPASS:-petafs-dev-secret}
EOF" >/dev/null 2>&1
}
kbin() {
  local tool="$1"; shift
  kafka_conf
  docker exec "$K1" "/opt/kafka/bin/${tool}" --bootstrap-server localhost:9092 \
    --command-config /tmp/mh.properties "$@" 2>/dev/null
}

# `mntr` over the 4-letter-word port. Mode is "leader" or "follower" — an
# ensemble that has NOT formed reports neither, which is the failure worth
# catching: each node looks individually healthy while being alone.
zk_mode() { docker exec "$1" sh -c 'echo mntr | nc -w 2 localhost $ZOO_PORT' 2>/dev/null \
              | awk -F'\t' '/zk_server_state/ {print $2}' | tr -d '\r'; }

if [ "${1:-}" = "--create-topic" ]; then
  # Deliberately tiny retention, and fewer partitions than the non-HA topology.
  # A Kafka broker's resident size is dominated by mmap'd log segments, NOT by
  # KAFKA_HEAP_OPTS: with 6 partitions and a 2 GiB retention cap these three
  # brokers grew from ~390 MiB each to ~1.4 GiB each under a sustained
  # producer, ate the machine, and starved the JobManager into
  # "Could not create actor system". Capping retention caps the brokers.
  TOPIC="${2:-transactions}"; PARTS="${3:-3}"
  kbin kafka-topics.sh --create --if-not-exists --topic "$TOPIC" \
    --partitions "$PARTS" --replication-factor 3 \
    --config retention.ms=300000 \
    --config retention.bytes=33554432 \
    --config segment.bytes=8388608 >/dev/null
  echo "topic '$TOPIC' at RF=3:"
  kbin kafka-topics.sh --describe --topic "$TOPIC" | head -2 | sed 's/^/  /'
  echo
fi

printf '\n\033[1m== multi-host HA formation ==\033[0m\n'

printf '\n\033[1mzookeeper\033[0m\n'
for n in 1 2 3; do
  printf '  zk%s: %s\n' "$n" "$(zk_mode "petafs-mhha-zk$n" || echo unreachable)"
done

printf '\n\033[1mkafka\033[0m\n'
printf '  brokers in cluster : %s/3\n' "$(kbin kafka-broker-api-versions.sh | grep -cE '\(id: [0-9]+ ')"

printf '\n\033[1mredis\033[0m\n'
for c in petafs-mhha-redis-primary petafs-mhha-redis-replica; do
  ROLE=$(docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null | grep -o '^role:[a-z]*' | cut -d: -f2 | tr -d '\r')
  printf '  %-22s role=%s\n' "${c#petafs-mhha-}" "${ROLE:-down}"
done

printf '\n\033[1mflink\033[0m\n'
# A non-leading JobManager answers 503, not silence. That is correct Flink
# behaviour — it is a live contender on the ZooKeeper latch with no cluster to
# serve — so 503 must be reported as "standby", never as "down". Conflating the
# two makes a healthy HA pair look half-broken.
for spec in "node-B:$JM_B_PORT" "node-C:$JM_C_PORT"; do
  name="${spec%:*}"; port="${spec##*:}"
  CODE=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "http://localhost:$port/overview" 2>/dev/null)
  case "$CODE" in
    200)
      # "serving", NOT "leading" — a non-leading JobManager proxies to the
      # leader and returns exactly the same numbers. The leader is the line
      # below, from ZooKeeper.
      printf '  jobmanager %-8s :%-5s serving   %s\n' "$name" "$port" \
        "$(curl -s -m 5 "http://localhost:$port/overview" | python3 -c "
import json,sys
o=json.load(sys.stdin)
print('tms=%d slots=%d(free %d) jobs_running=%d' % (
  o.get('taskmanagers',0), o.get('slots-total',0),
  o.get('slots-available',0), o.get('jobs-running',0)))" 2>/dev/null || echo '')" ;;
    503) printf '  jobmanager %-8s :%-5s standby   (contending on the ZooKeeper latch)\n' "$name" "$port" ;;
    000|"") printf '  jobmanager %-8s :%-5s DOWN\n' "$name" "$port" ;;
    *)   printf '  jobmanager %-8s :%-5s http %s\n' "$name" "$port" "$CODE" ;;
  esac
done

# Leadership comes from ZooKeeper, never from the REST API. In steady state a
# non-leading JobManager proxies to the leader, so BOTH endpoints answer 200
# with identical job lists and identical TaskManager counts — any check based
# on those cannot tell the two apart.
LEADER_PORT=""
for z in petafs-mhha-zk1 petafs-mhha-zk2 petafs-mhha-zk3; do
  docker ps --format '{{.Names}}' | grep -q "^${z}$" || continue
  LEADER_PORT=$(docker exec "$z" sh -c 'zkCli.sh -server localhost:$ZOO_PORT get /flink/petafs-mh-ha/leader/rest_server/connection_info' 2>/dev/null \
    | grep -aoE 'http://[^ "]+:[0-9]+' | head -1 | sed 's/.*://')
  [ -n "$LEADER_PORT" ] && break
done
case "$LEADER_PORT" in
  "$JM_B_PORT") printf '  leader (per ZooKeeper): node-B :%s\n' "$LEADER_PORT" ;;
  "$JM_C_PORT") printf '  leader (per ZooKeeper): node-C :%s\n' "$LEADER_PORT" ;;
  "")           printf '  leader (per ZooKeeper): none — no leader elected\n' ;;
  *)            printf '  leader (per ZooKeeper): :%s\n' "$LEADER_PORT" ;;
esac
echo
