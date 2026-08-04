#!/usr/bin/env bash
# Generate the credential files the authenticated topologies mount. `make secrets`.
#
# These replace the `${REDIS_PASSWORD:-petafs-dev-secret}` pattern. The
# difference is not cosmetic: an environment variable holding a password is
# readable by anything that can ask the orchestrator about the container —
#
#     docker inspect <container> | grep PASSWORD
#     docker compose config
#     cat /proc/1/environ
#
# — and it ends up in `docker compose config` output, which is the thing people
# paste into bug reports. A file mounted at /run/secrets is visible to that
# container's processes and to nothing else.
#
# IDEMPOTENT BY DESIGN. An existing file is never overwritten, because
# regenerating one silently is how you end up with a Redis that has rotated its
# password and a Flink sink that has not — which presents as NOAUTH on a stack
# that was working a minute ago. `make secrets-rotate` is the explicit path.
#
# ⚠️ tls_storepass is consumed by docker/tls/generate-certs.sh when it builds
# the JKS keystores. Changing it means regenerating the certs (`make mh-tls`),
# or every Java client fails to open its truststore.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=docker/secrets
mkdir -p "$OUT"
chmod 700 "$OUT"

# Alphanumeric only. Several of these land inside a JAAS config string, a Redis
# config file and a shell heredoc before anything reads them, and a quote or a
# backslash in the middle of that chain fails somewhere far from the cause.
#
# `tr -dc ... </dev/urandom | head -c 32` is the idiomatic one-liner and it
# fails under `set -o pipefail`: head closes the pipe at 32 bytes, tr dies of
# SIGPIPE, and the script exits 141 having written a partial file. Read a
# bounded amount FIRST so every stage terminates on its own.
gen() {
  head -c 512 /dev/urandom | LC_ALL=C tr -dc 'A-Za-z0-9' | cut -c1-32
}

created=0
for name in redis_password kafka_password tls_storepass minio_root_password; do
  f="$OUT/$name"
  # An EMPTY or short file is treated as absent. A crashed generator leaves one
  # behind, and "kept" on a truncated credential is far worse than regenerating:
  # every service would come up agreeing on a password nobody can reproduce.
  if [ -f "$f" ] && [ "$(wc -c <"$f" | tr -d ' ')" -ge 16 ]; then
    chmod 600 "$f"
    printf '  kept     %s\n' "$f"
  else
    gen > "$f"
    chmod 600 "$f"
    printf '  created  %s\n' "$f"
    created=$((created + 1))
  fi
done

if [ "$created" -gt 0 ]; then
  printf '\n%s new secret(s). The certs embed tls_storepass, so run:\n  make mh-tls\n' "$created"
fi
