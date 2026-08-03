#!/usr/bin/env bash
# End-to-end smoke test: produce -> flink -> redis -> feast read.
#
# Proves the whole hot path in one command. Run with `make smoke`.
#
# Deliberately uses the verification topic and a short backfill rather than the
# live `transactions` topic, so the run is bounded and repeatable.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE="docker compose -f docker/docker-compose.yml"
VENV=.venv/bin/python
TOPIC="${VERIFY_TOPIC:-transactions_verify}"
REDIS_PORT="${REDIS_HOST_PORT:-6380}"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

step "1/6 preflight"
docker info >/dev/null 2>&1 || fail "Docker daemon not running"
[ -f src/feast_repo/data/registry.db ] || fail "no registry — run 'make seed && make apply'"
curl -fs http://localhost:8081/overview >/dev/null || fail "Flink not up — run 'make up'"
redis-cli -p "$REDIS_PORT" ping >/dev/null || fail "redis not on :$REDIS_PORT"
echo "  stack is up"

step "2/6 cancel any running jobs"
for jid in $(curl -s http://localhost:8081/jobs/overview \
  | $VENV -c "import json,sys; print(' '.join(j['jid'] for j in json.load(sys.stdin).get('jobs',[]) if j['state'] in ('RUNNING','RESTARTING')))"); do
  $COMPOSE exec -T jobmanager flink cancel "$jid" >/dev/null 2>&1 || true
done
echo "  clear"

step "3/6 flush the feature keys we are about to write"
BEFORE=$(redis-cli -p "$REDIS_PORT" dbsize)
redis-cli -p "$REDIS_PORT" flushdb >/dev/null
echo "  cleared $BEFORE keys (this is OUR redis on :$REDIS_PORT, not a system one)"

step "4/6 backfill $TOPIC"
$VENV src/producer/generate_stream.py --topic "$TOPIC" \
  --backfill "${SPAN:-1200}" --eps "${VEPS:-40}" --users "${VUSERS:-150}" \
  --late-rate 0 --seed 42 2>&1 | tail -1

step "5/6 submit the pipeline (redis sink) and wait for keys"
$COMPOSE exec -T jobmanager flink run -d -py /opt/project/src/flink/streaming_job.py \
  --no-wait --sink redis --topic "$TOPIC" --startup-mode earliest-offset 2>&1 | tail -1

deadline=$(( $(date +%s) + 300 ))
while [ "$(redis-cli -p "$REDIS_PORT" dbsize)" -eq 0 ]; do
  [ "$(date +%s)" -lt "$deadline" ] || fail "no keys in redis after 300s"
  sleep 5
done
sleep 15   # let a few more windows land
KEYS=$(redis-cli -p "$REDIS_PORT" dbsize)
echo "  $KEYS feature keys written"

step "6/6 read back the way a model would"
$VENV - <<'PY'
import sys; sys.path.insert(0, ".")
from feast import FeatureStore
from contracts.schema import ENTITY_JOIN_KEY, FEATURES, EMBEDDING_DIM
from src.flink.sinks.feast_codec import feature_refs

store = FeatureStore(repo_path="src/feast_repo")
rows = [{ENTITY_JOIN_KEY: u} for u in range(1, 26)]
res = store.get_online_features(features=feature_refs(), entity_rows=rows).to_dict()

hits = [i for i, v in enumerate(res["txn_amount_avg_10m"]) if v is not None]
if not hits:
    raise SystemExit("FAIL: every feature read back as None — field-hash drift?")

i = hits[0]
uid = res[ENTITY_JOIN_KEY][i]
print(f"  {len(hits)}/{len(rows)} sampled users have features")
print(f"  user {uid}: avg={res['txn_amount_avg_10m'][i]:.2f} "
      f"count={res['txn_count_10m'][i]} "
      f"merchants={res['distinct_merchants_10m'][i]}")
print(f"    behavior_summary: {res['behavior_summary'][i][:60]}")

emb = res["embedding"][i]
assert emb is not None and len(emb) == EMBEDDING_DIM, "embedding missing or wrong dim"
print(f"    embedding: dim {len(emb)}")

missing = [f for f in FEATURES if res[f][i] is None]
if missing:
    raise SystemExit(f"FAIL: features read back as None: {missing}")
print("  every contract feature present")
PY

printf '\n\033[32mSMOKE TEST PASSED\033[0m — produce -> flink -> redis -> feast read\n'
