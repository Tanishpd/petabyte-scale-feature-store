#!/usr/bin/env bash
# Assert every metric the alert rules depend on actually EXISTS in Prometheus.
# `make alerts-verify`, against a running topology with the pipeline submitted.
#
# This is the half that `promtool test rules` cannot cover. Those tests feed
# synthetic series named by hand, so a rule written over
# `flink_jobmanager_job_numRestart` (no trailing s) passes every unit test and
# then never fires in production — the metric it watches does not exist, the
# expression returns empty, and empty is not a firing alert. Nothing anywhere
# reports an error. That is the same silent-success shape as a Feast feature
# name typo (Invariant 1): both sides look fine, the answer is just absent.
#
# So: extract the metric names FROM the rules file rather than restating them
# here — a list maintained by hand drifts from the rules and then asserts
# nothing — and require a non-empty instant query for each.
set -uo pipefail
cd "$(dirname "$0")/.."

PROM="${PROM_URL:-http://localhost:9090}"
RULES="${RULES_FILE:-docker/observability/alerts.yml}"

pass=0; fail=0
ok()  { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }

printf '\n\033[1m== alert metrics exist in Prometheus ==\033[0m\n'

if ! curl -sf -m 5 "$PROM/-/ready" >/dev/null 2>&1; then
  printf '  \033[31mFAIL\033[0m Prometheus is not reachable at %s\n' "$PROM"
  printf '\n\033[1m== 0 passed, 1 failed ==\033[0m\n'
  exit 1
fi

# Every flink_* identifier mentioned anywhere in the rules file. Deliberately
# greedy: if a rule starts watching a new metric, this picks it up without
# anyone remembering to update a list.
METRICS=$(grep -oE '\bflink_[a-zA-Z0-9_]+' "$RULES" | sort -u)
[ -z "$METRICS" ] && { bad "no flink_* metrics found in $RULES — is the path right?"; exit 1; }

for m in $METRICS; do
  n=$(curl -s -m 10 --get "$PROM/api/v1/query" --data-urlencode "query=$m" 2>/dev/null \
      | python3 -c "import json,sys
try: print(len(json.load(sys.stdin).get('data',{}).get('result',[])))
except Exception: print(-1)" 2>/dev/null)
  case "${n:--1}" in
    -1|0) bad "$m — no series (the rule that uses it can never fire)" ;;
    *)    ok  "$m ($n series)" ;;
  esac
done

# And the rules themselves have to be LOADED, not merely present on disk: a
# YAML error leaves Prometheus running happily with zero rule groups.
LOADED=$(curl -s -m 10 "$PROM/api/v1/rules" 2>/dev/null | python3 -c "
import json,sys
try:
    g=json.load(sys.stdin)['data']['groups']
    print(sum(len(x.get('rules',[])) for x in g))
except Exception: print(0)" 2>/dev/null)
EXPECTED=$(grep -cE '^\s+- alert:' "$RULES")
[ "${LOADED:-0}" -eq "${EXPECTED:-0}" ] \
  && ok "all $EXPECTED alert rules are loaded by Prometheus" \
  || bad "Prometheus has ${LOADED:-0} rules loaded, $RULES defines $EXPECTED"

# --- delivery ---------------------------------------------------------------
#
# Everything above proves the rules can fire. None of it proves anyone finds
# out. Prometheus will happily evaluate rules with no `alerting:` block at all,
# which is exactly the state this repo was in: alerts "fired" into a web page
# nobody was looking at. These assert the rest of the path —
# rule -> Prometheus -> Alertmanager -> receiver — from the receiving end.
AM="${ALERTMANAGER_URL:-http://localhost:9093}"
SINK="${ALERT_SINK_URL:-http://localhost:9099}"

printf '\n\033[1m== alerts are delivered, not just evaluated ==\033[0m\n'

# Prometheus must have DISCOVERED an Alertmanager. A typo in the `alerting:`
# block leaves this empty and every alert is silently undeliverable.
AMS=$(curl -s -m 10 "$PROM/api/v1/alertmanagers" 2>/dev/null | python3 -c "
import json,sys
try: print(len(json.load(sys.stdin)['data']['activeAlertmanagers']))
except Exception: print(0)" 2>/dev/null)
[ "${AMS:-0}" -ge 1 ] && ok "Prometheus has $AMS active Alertmanager(s)" \
                      || bad "Prometheus knows of NO Alertmanager — alerts go nowhere"

if ! curl -sf -m 5 "$AM/-/healthy" >/dev/null 2>&1; then
  bad "Alertmanager is not reachable at $AM"
else
  ok "Alertmanager is reachable"
fi

# The Watchdog is the end-to-end probe: it always fires, so if the path works
# at all it must arrive. Poll rather than sample once — Prometheus evaluates on
# its own schedule and Alertmanager batches by group_wait.
GOT=""
for _ in $(seq 1 40); do
  GOT=$(curl -s -m 5 "$SINK/alerts" 2>/dev/null | python3 -c "
import json,sys
try: print(sum(1 for a in json.load(sys.stdin) if a.get('alertname')=='Watchdog'))
except Exception: print(0)" 2>/dev/null)
  [ "${GOT:-0}" -ge 1 ] && break
  sleep 5
done
[ "${GOT:-0}" -ge 1 ] \
  && ok "the receiver actually received the Watchdog heartbeat ($GOT notification(s))" \
  || bad "nothing arrived at the receiver in 200s — the alerting path is broken end to end"

printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
