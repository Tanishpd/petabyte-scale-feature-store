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

chmod 644 *.key *.crt
echo "generated in docker/tls/$OUT:"
ls -1 | sed 's/^/  /'
