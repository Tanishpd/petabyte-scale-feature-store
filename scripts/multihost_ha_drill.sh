#!/usr/bin/env bash
# JobManager high-availability drills. `make mh-ha-drill`.
#
# scripts/multihost_drill.sh kills a whole node and shows the DATA survives —
# but the job there stops, because that topology has one JobManager and losing
# its node ends the pipeline until a human resubmits.
#
# This drill kills the node running the LEADING JobManager and asserts nobody
# has to do anything: the standby takes the ZooKeeper latch, recovers the job
# graph from the HA store, and redeploys onto a surviving TaskManager.
#
# The leader is discovered, never assumed. In Flink HA there is no "standby" in
# the configuration — both JobManagers are identical and contend for the same
# latch — so which one leads is runtime state. Killing a hardcoded node would
# half the time kill the standby and prove nothing.
#
# Destructive by design. Runs against docker-compose.multihost-ha.yml only.
set -uo pipefail
cd "$(dirname "$0")/.."

MH="docker compose -f docker/docker-compose.multihost-ha.yml --env-file docker/multihost/sim-ha.env"
A="$MH -p petafs-ha-a --profile a"
B="$MH -p petafs-ha-b --profile b"
C="$MH -p petafs-ha-c --profile c"

PW="${REDIS_PASSWORD:-petafs-dev-secret}"
RCLI="redis-cli --tls --cacert /tls/ca.crt"
JM_B_PORT="${JM_B_REST_PORT:-8081}"
JM_C_PORT="${JM_C_REST_PORT:-8082}"
TOPIC="${TOPIC:-transactions}"

pass=0; fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }
note() { printf '  \033[90m%s\033[0m\n' "$*"; }

http_code() { curl -s -o /dev/null -m 5 -w '%{http_code}' "$1" 2>/dev/null; }

# ASK ZOOKEEPER, NOT THE REST API.
#
# It is tempting to call the leader "the JobManager whose REST answers 200",
# and that is wrong: in steady state BOTH JobManagers answer 200 and return
# identical job lists, because a non-leading JobManager proxies to the leader.
# (A 503 only appears in the window before its REST leader-contender has
# started, which is why a check written during startup looks correct and then
# silently stops discriminating.) ZooKeeper holds the actual leader address in
# the connection_info node, and it is the only authority.
#
# Tries each ZooKeeper in turn: the drill kills a whole node, and that node's
# ZooKeeper goes with it.
zk_leader_port() {
  for z in petafs-mhha-zk1 petafs-mhha-zk2 petafs-mhha-zk3; do
    docker ps --format '{{.Names}}' | grep -q "^${z}$" || continue
    P=$(docker exec "$z" sh -c 'zkCli.sh -server localhost:$ZOO_PORT get /flink/petafs-mh-ha/leader/rest_server/connection_info' 2>/dev/null \
        | grep -aoE 'http://[^ "]+:[0-9]+' | head -1 | sed 's/.*://')
    [ -n "$P" ] && { echo "$P"; return; }
  done
}
leader_port() { zk_leader_port; }
port_to_node() { [ "$1" = "$JM_B_PORT" ] && echo b || echo c; }

running_job() {  # running_job <port>
  curl -s -m 5 "http://localhost:$1/jobs/overview" 2>/dev/null | python3 -c "
import json,sys
try: print(next((j['jid'] for j in json.load(sys.stdin).get('jobs',[]) if j['state']=='RUNNING'),''))
except Exception: print('')"
}
tm_count() {
  curl -s -m 5 "http://localhost:$1/overview" 2>/dev/null | python3 -c "
import json,sys
try: print(json.load(sys.stdin).get('taskmanagers',0))
except Exception: print(0)"
}
zk_mode() { docker exec "$1" sh -c 'echo mntr | nc -w 2 localhost $ZOO_PORT' 2>/dev/null \
              | awk -F'\t' '/zk_server_state/ {print $2}' | tr -d '\r'; }

# Talk to the object store from node A's network. Node A always survives this
# drill (the victim is the LEADING JobManager's node, which is B or C), so this
# stays a valid vantage point after the kill.
mc_a() {
  docker run --rm --entrypoint sh --network petafs-ha-a_default \
    minio/mc:RELEASE.2025-04-16T18-13-26Z -c \
    "mc alias set p http://minio-a:${MINIO_A_PORT:-19000} ${MINIO_USER:-petafs} ${MINIO_PASSWORD:-petafs-dev-secret} >/dev/null 2>&1; $1" \
    2>/dev/null | grep -v "older version\|Update:"
}
# `mc admin info --json` nests everything under "info" — the backend block is at
# d["info"]["backend"], not d["backend"]. Reading the wrong level yields 0 and
# looks exactly like "the whole pool is offline".
minio_online() { mc_a "mc admin info p --json" | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
    print((d.get('info') or d).get('backend',{}).get('onlineDisks',0))
except Exception: print(0)" 2>/dev/null; }
minio_sets() { mc_a "mc admin info p --json" | python3 -c "
import json,sys
try:
    b = (json.load(sys.stdin).get('info') or {}).get('backend',{})
    print('%s set(s) of %s drives' % (b.get('totalSets'), b.get('totalDrivesPerSet')))
except Exception: print('unknown')" 2>/dev/null; }
checkpoints_completed() {  # <port> <jid>
  curl -s -m 5 "http://localhost:$1/jobs/$2/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try: print((json.load(sys.stdin).get('counts') or {}).get('completed',0))
except Exception: print(0)"
}

redis_master_container() {
  for c in petafs-mhha-redis-primary petafs-mhha-redis-replica; do
    docker exec "$c" $RCLI -a "$PW" INFO replication 2>/dev/null \
      | grep -q '^role:master' && { echo "$c"; return; }
  done
}
# Lua so the binary Feast keys never leave Redis — see docs/MULTIHOST.md §7.
max_ttl() {
  local m; m=$(redis_master_container); [ -z "$m" ] && { echo -1; return; }
  docker exec "$m" $RCLI -a "$PW" EVAL \
    "local c=redis.call('SCAN',0,'COUNT',300); local m=-2;
     for _,k in ipairs(c[2]) do local t=redis.call('TTL',k); if t>m then m=t end end; return m" 0 \
    2>/dev/null | tr -d '\r'
}

# ===========================================================================
step "Drill 0 — the three nodes are isolated"
# ===========================================================================
NETS=$(for n in a b c; do docker network ls --filter "name=petafs-ha-$n" --format '{{.ID}}'; done | sort -u | wc -l | tr -d ' ')
[ "${NETS:-0}" -eq 3 ] && ok "three separate Docker networks, one per node" \
                       || bad "expected 3 distinct networks, found ${NETS:-0}"
if docker exec petafs-mhha-taskmanager-c python3 -c "import socket;socket.gethostbyname('kafka1')" >/dev/null 2>&1; then
  bad "node C resolved node A's service name — isolation is fake"
else
  ok "node C cannot resolve node A's service names (no shared Compose DNS)"
fi

# ===========================================================================
step "Drill 1 — the HA machinery is formed across the three nodes"
# ===========================================================================
MODES=$(for n in 1 2 3; do zk_mode "petafs-mhha-zk$n"; done | sort | tr '\n' ' ')
LEADERS=$(echo "$MODES" | tr ' ' '\n' | grep -c '^leader$')
FOLLOWERS=$(echo "$MODES" | tr ' ' '\n' | grep -c '^follower$')
[ "$LEADERS" = "1" ] && [ "$FOLLOWERS" = "2" ] \
  && ok "ZooKeeper ensemble formed across three nodes (1 leader, 2 followers)" \
  || bad "ZooKeeper modes were: ${MODES:-none} (wanted 1 leader, 2 followers)"

LP=$(leader_port)
if [ -z "$LP" ]; then
  bad "neither JobManager is leading — nothing else can be asserted"
  printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"; exit 1
fi
LN=$(port_to_node "$LP")
OTHER_PORT=$([ "$LP" = "$JM_B_PORT" ] && echo "$JM_C_PORT" || echo "$JM_B_PORT")
OTHER_NODE=$(port_to_node "$OTHER_PORT")
OC=$(http_code "http://localhost:$OTHER_PORT/overview")
note "ZooKeeper names node $LN (:$LP) the leader; node $OTHER_NODE (:$OTHER_PORT) answers HTTP $OC"
# Both JobManagers must be ALIVE (the non-leader proxies, so it also answers
# 200) while ZooKeeper names exactly one of them leader. Asserting on the REST
# code alone cannot tell these apart, which is the point.
[ "$OC" = "200" ] || [ "$OC" = "503" ] \
  && ok "both JobManagers are live, and ZooKeeper names exactly one leader (node $LN)" \
  || bad "the non-leading JobManager answered '$OC' — it is not running"

# ZooKeeper only holds POINTERS. The JOB GRAPH itself lives in the HA store,
# and if it never got there the standby would win the latch with nothing to
# recover. Checked by listing the bucket, not by grepping the JobManager log:
# the "Created ... storage directory" line is only emitted the FIRST time the
# directory is created, so on any later run a log grep reports a perfectly
# populated store as missing.
HA_LS=$(mc_a "mc ls --recursive p/flink-ha")
if printf '%s' "$HA_LS" | grep -q submittedExecutionPlan; then
  ok "job graph is in the HA store — the standby has something to recover"
  note "$(printf '%s' "$HA_LS" | grep -oE '[a-zA-Z]+[0-9a-f]{8,}' | head -3 | tr '\n' ' ')"
else
  bad "no submittedExecutionPlan in s3://flink-ha — failover would recover nothing"
fi

D=$(minio_online)
if [ "${D:-0}" -eq 3 ]; then
  ok "object store is distributed across all three nodes (3 drives online)"
  # One set spanning all three nodes is the property that matters. Three sets
  # of one, or one set per node, would report the same drive count and lose
  # data the moment a node died.
  note "erasure layout: $(minio_sets)"
else
  bad "object store reports ${D:-0} drives online, expected 3"
fi

N=$(tm_count "$LP")
[ "${N:-0}" -ge 1 ] && ok "TaskManager registered with the leader via ZooKeeper ($N)" \
                    || bad "no TaskManager registered with the leader"

JOB=$(running_job "$LP")
[ -n "$JOB" ] && ok "pipeline is running (job $JOB)" \
              || note "no running job — 'make mh-ha-submit' first; recovery cannot be asserted"

# ===========================================================================
step "Drill 2 — kill the node running the LEADING JobManager"
# ===========================================================================
if [ -z "$JOB" ]; then
  note "skipped: no running job to recover"
else
  T0=$(max_ttl)
  note "before: leader on node $LN, job $JOB, max feature TTL ${T0}s"
  VICTIM=$([ "$LN" = "b" ] && echo "$B" || echo "$C")
  $VICTIM down -t 0 >/dev/null 2>&1
  note "node $LN is gone — with it the leading JobManager, a broker, a sentinel and a ZooKeeper"

  # Leadership should move on its own. No resubmission, no operator.
  # Watched in ZOOKEEPER, not over REST: the surviving JobManager already
  # answered 200 before the failover, so polling REST would "pass" instantly
  # and prove nothing at all.
  NEWLEADER=""
  for _ in $(seq 1 40); do
    NP=$(zk_leader_port)
    [ -n "$NP" ] && [ "$NP" = "$OTHER_PORT" ] && { NEWLEADER="$OTHER_NODE"; break; }
    sleep 5
  done
  [ -n "$NEWLEADER" ] \
    && ok "ZooKeeper moved leadership to node $OTHER_NODE automatically" \
    || bad "leadership did not move within 200 s (ZooKeeper still says :${NP:-unknown})"

  if [ -n "$NEWLEADER" ]; then
    MODES=$(for n in 1 2 3; do zk_mode "petafs-mhha-zk$n"; done | grep -cE 'leader|follower')
    [ "${MODES:-0}" -ge 2 ] && ok "ZooKeeper kept quorum on the two surviving nodes" \
                            || bad "ZooKeeper lost quorum ($MODES nodes responding)"

    JOB2=""
    for _ in $(seq 1 48); do JOB2=$(running_job "$OTHER_PORT"); [ -n "$JOB2" ] && break; sleep 5; done
    if [ -n "$JOB2" ]; then
      ok "pipeline recovered with NO resubmission (job $JOB2)"
      R=$(curl -s -m 5 "http://localhost:$OTHER_PORT/jobs/$JOB2/checkpoints" 2>/dev/null | python3 -c "
import json,sys
try: print(((json.load(sys.stdin).get('latest') or {}).get('restored') or {}).get('external_path',''))
except Exception: print('')")
      case "$R" in
        s3://*) ok "recovered STATE from the HA store, not restarted from scratch"; note "$R" ;;
        *)      note "no restore path reported (${R:-none}) — job may have been rescheduled without state" ;;
      esac
      # The object store must still take WRITES with a node gone, not merely
      # serve reads. A pool that has gone read-only looks fine to a `mc ls` and
      # then silently fails the next checkpoint.
      if mc_a "echo after-the-kill | mc pipe p/flink-ha/drill-probe.txt >/dev/null 2>&1 && mc cat p/flink-ha/drill-probe.txt" \
           | grep -q after-the-kill; then
        ok "object store still accepts writes with a node down"
      else
        bad "object store would not accept a write after the node loss"
      fi
      D2=$(minio_online)
      [ "${D2:-0}" -eq 2 ] && ok "pool degraded to 2 of 3 drives and stayed serving" \
                           || note "pool reports ${D2:-0} drives online"

      CP0=$(checkpoints_completed "$OTHER_PORT" "$JOB2")
      sleep 75
      T1=$(max_ttl)
      [ "${T1:-0}" -gt 1600 ] && ok "features flowing again after failover (max TTL ${T1}s of 1800)" \
                              || bad "no recent feature writes after failover (max TTL ${T1:-none})"
      # The end-to-end proof that the object store survived: Flink is still
      # completing checkpoints INTO it after losing a node.
      CP1=$(checkpoints_completed "$OTHER_PORT" "$JOB2")
      [ "${CP1:-0}" -gt "${CP0:-0}" ] \
        && ok "checkpoints still completing into the degraded pool ($CP0 -> $CP1)" \
        || bad "no new checkpoints after the node loss ($CP0 -> $CP1)"
    else
      bad "job did not return to RUNNING within 240 s of the failover"
    fi
  fi

  note "restoring node $LN..."
  $VICTIM up -d >/dev/null 2>&1
  RESTORED=""
  for _ in $(seq 1 40); do
    RC=$(http_code "http://localhost:$LP/overview")
    { [ "$RC" = "503" ] || [ "$RC" = "200" ]; } && { RESTORED="$RC"; break; }
    sleep 5
  done
  [ -n "$RESTORED" ] && ok "the restored JobManager came back up (HTTP $RESTORED)" \
                     || bad "the restored JobManager never answered"
  # It must rejoin as a CONTENDER and NOT seize the latch back. A restored node
  # that grabs leadership causes a second, entirely avoidable failover — and
  # ZooKeeper is the only place that difference is visible.
  L2=$(zk_leader_port)
  [ -n "$L2" ] && [ "$(port_to_node "$L2")" = "$OTHER_NODE" ] \
    && ok "leadership stayed on node $OTHER_NODE; it did not bounce back" \
    || bad "leadership is now on '$(port_to_node "${L2:-0}")', expected node $OTHER_NODE to keep it"
fi

printf '\n\033[1m== %d passed, %d failed ==\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
