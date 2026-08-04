#!/bin/sh
# Write the Redis ACL file, then make sure redis.conf actually loads it.
# Mounted into redis-primary and redis-replica and run before redis-server.
#
# WHY THIS EXISTS
# ---------------
# Before ACLs there was one credential — `requirepass` — and it made every
# holder an administrator. The Flink sink, the serving path, the replica and
# Sentinel all presented the same string, so anything that could write a
# feature could also read the entire keyspace, rewrite it, or FLUSHALL it, and
# Redis recorded nothing about which client did what. Authentication without
# authorization is a door with a lock and no rooms.
#
# `user default off` is the load-bearing line. `default` is the account a
# client gets without asking for one; leaving it enabled makes every rule below
# optional.
#
# ROLES SWAP, so this file is deliberately identical on both data nodes. After
# a Sentinel failover the promoted replica serves writes (it needs every user
# a master needs) and the demoted master replicates *from* it (it needs to
# authenticate outward as petafs-repl). A per-role rule set would be correct
# until the first failover and wrong from then on — which is the one moment the
# whole topology exists to survive.
set -eu

ACL=/data/users.acl
CONF=${1:?usage: write-acl.sh <redis.conf>}

# Regenerated on EVERY boot, unlike redis.conf. The conf is written once and
# preserved because Sentinel rewrites it during failover — that is where
# `replicaof` ends up, and clobbering it would undo a promotion. An ACL file
# kept the same way would survive a credential rotation and leave this node
# holding a password no other service still has, which presents as NOAUTH on a
# stack that worked a minute ago.
umask 077
cat > "$ACL" <<EOF
user default off
user petafs-admin on >$(cat /run/secrets/redis_password) ~* &* +@all
user petafs-flink on >$(cat /run/secrets/redis_flink_password) ~* resetchannels +@connection +hget +hset +expire
user petafs-reader on >$(cat /run/secrets/redis_reader_password) ~* resetchannels +@connection +hget +hmget +hgetall +exists +ttl
user petafs-repl on >$(cat /run/secrets/redis_repl_password) resetchannels +psync +replconf +ping
user petafs-sentinel on >$(cat /run/secrets/redis_sentinel_password) allchannels +multi +slaveof +replicaof +ping +exec +subscribe +config|rewrite +role +publish +info +client|setname +client|kill +script|kill
EOF

# The command lists above are minimums taken from what each client actually
# issues, not category grants:
#   petafs-flink    HGET (the _ts staleness guard) + HSET + EXPIRE. That is the
#                   entire sink. Notably absent: DEL, KEYS, SCAN, CONFIG,
#                   FLUSHALL, and every command that could read a feature it
#                   did not write.
#   petafs-reader   hash reads only — get_online_features() issues HMGET.
#   petafs-repl     +psync +replconf +ping is what a replica's link needs.
#   petafs-sentinel the set published in the Redis Sentinel docs. Sentinel
#                   sends SLAVEOF to reconfigure instances during a failover,
#                   so this one legitimately needs a write-shaped command;
#                   `+replicaof` is included alongside the `+slaveof` alias
#                   because which name is checked has changed across versions.
#
# `~*` on flink/reader/admin is honest rather than lazy: Feast's Redis keys are
# BINARY (mmh3 hashes of the entity key), so there is no printable prefix a
# key pattern could match. The restriction that carries weight here is the
# command list, not the key glob.

# A volume created before ACLs existed has a redis.conf with no `aclfile`
# directive, and redis-server starts perfectly happily without one — every rule
# above would then be silently absent while the stack looked healthy. Append it
# rather than rewriting the conf, so a failover-persisted `replicaof` survives.
grep -q '^aclfile ' "$CONF" || printf 'aclfile %s\n' "$ACL" >> "$CONF"

# Same reasoning for the outbound half of replication. `masterauth` alone
# authenticates as `default`, which is now off, so a replica configured the old
# way would fail to sync with NOAUTH and Sentinel would never see it come back.
grep -q '^masteruser ' "$CONF" || {
  printf 'masteruser petafs-repl\n' >> "$CONF"
  printf 'masterauth %s\n' "$(cat /run/secrets/redis_repl_password)" >> "$CONF"
}
