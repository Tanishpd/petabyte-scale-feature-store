#!/usr/bin/env bash
# Back up the state that CANNOT be rebuilt. `make backup`.
#
# Most of this system is derived and deliberately disposable:
#
#   Redis    a cache. TTL'd at 30 min, rebuilt by replaying Kafka
#            (RUNBOOK §7). Backing it up would be backing up a projection.
#   Kafka    has its own replication (RF=3, min.insync.replicas=2) and its
#            retention is the real bound on how far back you can rebuild.
#   Flink    checkpoints live in the object store and are covered by ITS
#            durability, not by this script.
#
# Two things are not derived, and until this script existed neither had a copy
# anywhere — both are gitignored, so they lived on exactly one disk:
#
#   registry.db   THE load-bearing one. Feast stores the feature-view
#                 definitions here, and the Redis field keys are
#                 mmh3("<view>:<feature>") hashes computed FROM those names
#                 (Invariant 1). Lose this and every key already in Redis
#                 becomes unreadable — the data is intact and nothing can
#                 address it. It is also small (a few KB), which makes the
#                 absence of a backup harder to justify, not easier.
#
#   *.parquet     the offline store. The source of truth for training sets and
#                 point-in-time joins, and the only copy of history once Kafka
#                 retention has aged out.
#
# NOT INCLUDED BY DEFAULT: docker/secrets/. A restore needs them, but writing
# credentials into a tarball next to the data turns one stolen file into a full
# compromise. `INCLUDE_SECRETS=1 make backup` if you have somewhere appropriate
# to put the result; the manifest records which choice was made.
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_DATA=src/feast_repo/data
OUT_DIR="${BACKUP_DIR:-backups}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
NAME="petafs-backup-${STAMP}"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

[ -f "$REPO_DATA/registry.db" ] || {
  echo "no registry at $REPO_DATA/registry.db — run 'make apply' first" >&2
  exit 1
}

mkdir -p "$OUT_DIR" "$STAGE/$NAME/feast"
cp "$REPO_DATA/registry.db" "$STAGE/$NAME/feast/"
# The parquet may be absent on a registry-only deployment; that is legitimate.
found_parquet=0
for f in "$REPO_DATA"/*.parquet; do
  [ -e "$f" ] || continue
  cp "$f" "$STAGE/$NAME/feast/"
  found_parquet=1
done
# The yaml is in git, but a backup that restores to a DIFFERENT configuration
# than the one that wrote it is a trap — entity_key_serialization_version in
# particular changes the on-disk encoding (Invariant 3).
cp src/feast_repo/feature_store.yaml "$STAGE/$NAME/feast/" 2>/dev/null || true

if [ "${INCLUDE_SECRETS:-0}" = "1" ] && [ -d docker/secrets ]; then
  mkdir -p "$STAGE/$NAME/secrets"
  cp docker/secrets/* "$STAGE/$NAME/secrets/" 2>/dev/null || true
  # 600 on the FILES, 700 on the directory. `chmod -R 600` strips the
  # directory's execute bit, and a directory you cannot traverse cannot be
  # read, tarred or cleaned up — it fails later and somewhere else.
  chmod 700 "$STAGE/$NAME/secrets"
  chmod 600 "$STAGE/$NAME/secrets/"* 2>/dev/null || true
fi

# A manifest with checksums, because a backup you cannot verify is a hope. The
# restore refuses on a mismatch rather than writing corrupt state over good.
{
  printf 'created_utc=%s\n' "$STAMP"
  printf 'host=%s\n' "$(hostname)"
  printf 'git_commit=%s\n' "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  printf 'includes_secrets=%s\n' "${INCLUDE_SECRETS:-0}"
  printf 'includes_parquet=%s\n' "$found_parquet"
  printf '\n[sha256]\n'
  (cd "$STAGE/$NAME" && find . -type f ! -name MANIFEST -exec shasum -a 256 {} \; | sort -k2)
} > "$STAGE/$NAME/MANIFEST"

tar -czf "$OUT_DIR/$NAME.tar.gz" -C "$STAGE" "$NAME"
SIZE=$(wc -c < "$OUT_DIR/$NAME.tar.gz" | tr -d ' ')

printf '\n\033[1mbacked up\033[0m %s (%s bytes)\n' "$OUT_DIR/$NAME.tar.gz" "$SIZE"
sed -n '1,6p' "$STAGE/$NAME/MANIFEST" | sed 's/^/  /'
printf '  files:\n'
(cd "$STAGE/$NAME" && find . -type f ! -name MANIFEST | sed 's|^\./|    |')
printf '\nRestore with:  make restore ARCHIVE=%s\n' "$OUT_DIR/$NAME.tar.gz"
printf 'A backup you have never restored is not a backup — tests/test_backup_restore.py\nexercises the round trip on every CI run.\n'
