#!/usr/bin/env bash
# Formation summary for the multi-host topology. `make mh-health`.
#
# The question this answers is narrower than "is everything up": it is "did the
# three nodes find EACH OTHER". Every service here starts happily in isolation
# and reports itself healthy while being completely alone — a broker with no
# quorum, a sentinel that sees no peers, a TaskManager retrying registration.
# Per-container health is therefore nearly uninformative; peer counts are not.
#
#   multihost_health.sh                       summary
#   multihost_health.sh --create-topic t 6    create `t` at RF=3, then summarise
set -uo pipefail
cd "$(dirname "$0")/.."

PW="${REDIS_PASSWORD:-petafs-dev-secret}"
RCLI="redis-cli --tls --cacert /tls/ca.crt"
K1=petafs-mh-kafka1

# Every Kafka CLI call needs SASL_SSL credentials. Without them the tools
# return empty rather than erroring, which reads as "no data" instead of "not
# authenticated" — that silently skipped a drill the first time in the prod
# stack, so it is written once here and reused.
kafka_conf() {
  docker exec "$K1" sh -c "cat > /tmp/mh.properties <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_USER:-petafs}\" password=\"${KAFKA_PASSWORD:-petafs-dev-secret}\";
ssl.truststore.location=/tls/kafka.truststore.jks
ssl.truststore.password=${TLS_STOREPASS:-petafs-dev-secret}
EOF" >/dev/null 2>&1
}

# Bootstrap on localhost:9092 (the BROKER listener) — metadata then redirects to
# the advertised host.docker.internal:1909x addresses, so a successful call also
# proves the routable path works, not just the local socket.
kbin() {
  local tool="$1"; shift
  # Written on every call: a restored node is a NEW container with an empty
  # /tmp, and the Kafka CLI answers an unauthenticated call with silence rather
  # than an error — so a stale credentials file reads as "the topic is empty".
  kafka_conf
  docker exec "$K1" "/opt/kafka/bin/${tool}" --bootstrap-server localhost:9092 \
    --command-config /tmp/mh.properties "$@" 2>/dev/null
}

broker_count() { kbin kafka-broker-api-versions.sh 2>/dev/null | grep -cE '^[^ ]+:[0-9]+ \(id: '; }

sentinel_peers() {  # $1 = container
  docker exec "$1" $RCLI -p 26379 -a "$PW" SENTINEL master mymaster 2>/dev/null \
    | grep -A1 '^num-other-sentinels$' | tail -1 | tr -d '\r'
}

kafka_conf

if [ "${1:-}" = "--create-topic" ]; then
  TOPIC="${2:-transactions}"; PARTS="${3:-6}"
  kbin kafka-topics.sh --create --if-not-exists --topic "$TOPIC" \
    --partitions "$PARTS" --replication-factor 3 \
    --config retention.ms=3600000 --config retention.bytes=2147483648 >/dev/null
  echo "topic '$TOPIC' at RF=3:"
  kbin kafka-topics.sh --describe --topic "$TOPIC" | sed 's/^/  /'
  echo
fi

printf '\n\033[1m== multi-host formation ==\033[0m\n'

printf '\n\033[1mcontainers\033[0m\n'
for n in a b c; do
  printf '  node %s: ' "$n"
  docker ps --filter "label=com.docker.compose.project=petafs-$n" \
            --format '{{.Names}}' 2>/dev/null | wc -l | tr -d ' ' | tr -d '\n'
  printf ' running\n'
done

printf '\n\033[1mkafka\033[0m\n'
BC=$(broker_count)
printf '  brokers in cluster : %s/3\n' "${BC:-0}"
CQ=$(docker exec "$K1" /opt/kafka/bin/kafka-metadata-quorum.sh \
       --bootstrap-server localhost:9092 --command-config /tmp/mh.properties describe --status 2>/dev/null \
       | grep -E '^(LeaderId|CurrentVoters)' | tr '\n' ' ')
printf '  kraft quorum       : %s\n' "${CQ:-unavailable}"

printf '\n\033[1mredis\033[0m\n'
for c in petafs-mh-redis-primary petafs-mh-redis-replica; do
  ROLE=$(docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null | grep -o '^role:[a-z]*' | cut -d: -f2)
  LINK=$(docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null | grep -o 'master_link_status:[a-z]*' | cut -d: -f2)
  printf '  %-28s role=%-7s %s\n' "${c#petafs-mh-}" "${ROLE:-down}" \
    "${LINK:+link=$LINK}"
done
M=$(docker exec petafs-mh-redis-sentinel-1 $RCLI -p 26379 -a "$PW" \
      SENTINEL get-master-addr-by-name mymaster 2>/dev/null | tr '\n' ':' | sed 's/:$//' | tr -d '\r')
printf '  sentinel says master: %s\n' "${M:-unknown}"
for n in 1 2 3; do
  printf '  sentinel-%s sees %s other sentinels\n' "$n" \
    "$(sentinel_peers "petafs-mh-redis-sentinel-$n" || echo '?')"
done

printf '\n\033[1mflink\033[0m\n'
OV=$(curl -s -m 5 http://localhost:8081/overview 2>/dev/null)
if [ -z "$OV" ]; then
  printf '  jobmanager         : not reachable on :8081\n'
else
  printf '  %s\n' "$(printf '%s' "$OV" | python3 -c "
import json,sys
o=json.load(sys.stdin)
print('taskmanagers       : %d registered, %d slots (%d free)' % (
    o.get('taskmanagers',0), o.get('slots-total',0), o.get('slots-available',0)))
print('  jobs               : %d running, %d failed' % (
    o.get('jobs-running',0), o.get('jobs-failed',0)))")"
fi
echo
