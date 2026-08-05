#!/usr/bin/env bash
# Restore what scripts/backup.sh saved. `make restore ARCHIVE=...`
#
# VERIFIES BEFORE IT WRITES. Every file is checksummed against the manifest and
# a single mismatch aborts the whole restore, because the failure mode this
# guards against is not "the restore errored" — it is a half-written registry
# that Feast opens successfully and that addresses the wrong Redis keys. A
# corrupt registry does not announce itself; reads just return None
# (Invariant 1).
#
# The existing state is moved aside rather than overwritten, so a restore that
# turns out to be the wrong archive is itself reversible.
set -euo pipefail
cd "$(dirname "$0")/.."

ARCHIVE="${1:-${ARCHIVE:-}}"
[ -n "$ARCHIVE" ] || { echo "usage: restore.sh <archive.tar.gz>" >&2; exit 1; }
[ -f "$ARCHIVE" ] || { echo "no such archive: $ARCHIVE" >&2; exit 1; }

REPO_DATA=src/feast_repo/data
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

tar -xzf "$ARCHIVE" -C "$STAGE"
ROOT="$(find "$STAGE" -mindepth 1 -maxdepth 1 -type d | head -1)"
[ -n "$ROOT" ] && [ -f "$ROOT/MANIFEST" ] || {
  echo "archive has no MANIFEST — refusing to restore something this script did not write" >&2
  exit 1
}

printf '\n\033[1mverifying\033[0m %s\n' "$ARCHIVE"
sed -n '1,6p' "$ROOT/MANIFEST" | sed 's/^/  /'

# Re-derive every checksum and compare. Done BEFORE anything is written.
bad=0
while read -r want path; do
  [ -n "${want:-}" ] || continue
  case "$want" in \[sha256\]|created_utc*|host=*|git_commit=*|includes_*) continue ;; esac
  f="$ROOT/${path#./}"
  if [ ! -f "$f" ]; then
    printf '  \033[31mMISSING\033[0m %s\n' "$path"; bad=$((bad+1)); continue
  fi
  got="$(shasum -a 256 "$f" | awk '{print $1}')"
  if [ "$got" = "$want" ]; then
    printf '  \033[32mok\033[0m      %s\n' "$path"
  else
    printf '  \033[31mCORRUPT\033[0m %s\n' "$path"; bad=$((bad+1))
  fi
done < <(sed -n '/^\[sha256\]/,$p' "$ROOT/MANIFEST" | tail -n +2)

if [ "$bad" -gt 0 ]; then
  printf '\n\033[31m%d file(s) failed verification — nothing was written.\033[0m\n' "$bad"
  exit 1
fi

# Move the current state aside instead of clobbering it.
if [ -d "$REPO_DATA" ] && [ -n "$(ls -A "$REPO_DATA" 2>/dev/null)" ]; then
  PREV="${REPO_DATA}.pre-restore.$(date -u +%Y%m%dT%H%M%SZ)"
  mv "$REPO_DATA" "$PREV"
  printf '\n  previous state moved to %s\n' "$PREV"
fi
mkdir -p "$REPO_DATA"
cp "$ROOT/feast/"* "$REPO_DATA/" 2>/dev/null || true
# feature_store.yaml belongs next to the repo definition, not in data/.
if [ -f "$REPO_DATA/feature_store.yaml" ]; then
  mv "$REPO_DATA/feature_store.yaml" src/feast_repo/feature_store.yaml
fi

if [ -d "$ROOT/secrets" ]; then
  mkdir -p docker/secrets && chmod 700 docker/secrets
  cp "$ROOT/secrets/"* docker/secrets/ && chmod 600 docker/secrets/*
  printf '  restored credentials into docker/secrets/\n'
fi

printf '\n\033[1mrestored\033[0m into %s\n' "$REPO_DATA"
ls -1 "$REPO_DATA" | sed 's/^/  /'
printf '\nThe online store is NOT restored and does not need to be — it is a cache.\n'
printf 'Replay it with:  make submit FROM_START=1   (RUNBOOK §7)\n'
