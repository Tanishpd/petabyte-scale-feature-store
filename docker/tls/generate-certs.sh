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

# --- CA ---------------------------------------------------------------------
openssl req -new -x509 -nodes -days "$DAYS" -newkey rsa:2048 \
  -keyout ca.key -out ca.crt -subj "/CN=petafs-local-ca" 2>/dev/null

# --- one cert per service, with SANs for the compose hostnames --------------
gen() {
  local name="$1"; shift
  local sans=""
  for h in "$@"; do sans="${sans}DNS:${h},"; done
  sans="${sans}DNS:localhost,IP:127.0.0.1"

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
