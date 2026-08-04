#!/usr/bin/env bash
# Generate a local CA plus per-service certs for the production topology.
#
# Self-signed and committed to nothing — `make prod-tls` regenerates them and
# docker/tls/out/ is gitignored. In a real deployment these come from your CA or
# a cert-manager, and the only thing that changes is where the files come from.
set -euo pipefail
cd "$(dirname "$0")"
OUT=out
DAYS=${DAYS:-825}

rm -rf "$OUT" && mkdir -p "$OUT"
cd "$OUT"

# Extra SANs for the multi-host topology. On one host every service is reached
# by its compose service name, so the SANs below are enough. Across hosts the
# address a peer dials is the *node's* routable name, and a cert without it
# fails hostname verification — which presents as a handshake error at the
# client and nothing at all in the server log.
#
#   NODE_SANS=host-a.internal,host-b.internal,10.0.0.11 make prod-tls
#
# Entries that parse as IPv4 become IP: SANs, everything else DNS:. Both are
# added to every cert: after a Redis failover or a Kafka leadership move, a
# client can legitimately end up talking to any node.
NODE_SANS=${NODE_SANS:-host.docker.internal}
extra_sans=""
IFS=',' ; for h in $NODE_SANS; do
  h=$(printf '%s' "$h" | tr -d '[:space:]')
  [ -z "$h" ] && continue
  case "$h" in
    *[0-9].[0-9]*) case "$h" in *[!0-9.]*) extra_sans="${extra_sans}DNS:${h}," ;;
                                        *) extra_sans="${extra_sans}IP:${h}," ;; esac ;;
    *) extra_sans="${extra_sans}DNS:${h}," ;;
  esac
done
unset IFS

# --- CA ---------------------------------------------------------------------
openssl req -new -x509 -nodes -days "$DAYS" -newkey rsa:2048 \
  -keyout ca.key -out ca.crt -subj "/CN=petafs-local-ca" 2>/dev/null

# --- one cert per service, with SANs for the compose hostnames --------------
gen() {
  local name="$1"; shift
  local sans=""
  for h in "$@"; do sans="${sans}DNS:${h},"; done
  sans="${sans}${extra_sans}DNS:localhost,IP:127.0.0.1"

  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "${name}.key" -out "${name}.csr" -subj "/CN=${name}" 2>/dev/null
  openssl x509 -req -in "${name}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "${name}.crt" -days "$DAYS" \
    -extfile <(printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n" "$sans") 2>/dev/null
  rm -f "${name}.csr"
}

gen redis-primary  redis-primary
gen redis-replica  redis-replica
gen sentinel       redis-sentinel-1 redis-sentinel-2 redis-sentinel-3
gen client         client

# One cert every Redis node presents, covering both node names. Roles swap on
# failover, so a per-role cert is wrong by construction: the promoted replica
# would present a cert naming it a replica while Sentinel announces it as the
# master. redis-py verifies the hostname (ssl_check_hostname defaults to True
# in redis-py 7.x), so that mismatch breaks writes right after a promotion —
# the one moment the failover was supposed to cover.
gen redis-node     redis-primary redis-replica \
                   redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

# --- Kafka: same CA, packaged as JKS ---------------------------------------
# Redis takes PEM; Kafka is Java and takes keystores. Each broker's cert covers
# its compose hostname AND localhost, because the EXTERNAL listener is
# advertised to host clients as localhost:2909x — a cert without that SAN fails
# hostname verification on the host side only, which is a confusing
# half-working state.
STOREPASS=${STOREPASS:-petafs-dev-secret}
for b in kafka1 kafka2 kafka3; do
  gen "$b" "$b"
  # keytool cannot import a bare PEM key pair, so go PEM -> PKCS12 -> JKS.
  openssl pkcs12 -export -in "${b}.crt" -inkey "${b}.key" -certfile ca.crt \
    -name "$b" -out "${b}.p12" -password "pass:${STOREPASS}" 2>/dev/null
  keytool -importkeystore -noprompt \
    -srckeystore "${b}.p12" -srcstoretype PKCS12 -srcstorepass "$STOREPASS" \
    -destkeystore "${b}.keystore.jks" -deststorepass "$STOREPASS" \
    -destkeypass "$STOREPASS" >/dev/null 2>&1
  rm -f "${b}.p12"
done

# One truststore holding the CA, shared by the brokers and by every Java client
# — the Flink Kafka connector included.
keytool -importcert -noprompt -alias petafs-ca -file ca.crt \
  -keystore kafka.truststore.jks -storepass "$STOREPASS" >/dev/null 2>&1

chmod 644 ./*.key ./*.crt ./*.jks 2>/dev/null || true
echo "generated in docker/tls/$OUT:"
ls -1 | sed 's/^/  /'
