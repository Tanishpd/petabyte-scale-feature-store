"""Flink -> Redis, in Feast's own encoding. The hot path of the whole project.

Shape (ARCHITECTURE.md §6):

    ds = t_env.to_data_stream(table)
    ds.map(FeastRedisWriter(), output_type=Types.STRING()).name("feast-redis-writer")

Three PyFlink specifics, each a plausible Java-idiom mistake:

- `to_data_stream` is a **StreamTableEnvironment** method, not a `Table` one.
- There is **no `RichMapFunction`** in PyFlink. `MapFunction` is already rich:
  `open(runtime_context)` / `close()` live on the base `Function`.
- **No terminal sink is required.** `map()` registers its operator with the
  execution environment at creation, so a side-effect-only branch runs on
  `execute()`. (`DiscardingSink` is the instinctive choice and does not exist
  in PyFlink 2.2.1.)

Writes are pipelined and flushed at 500 rows or 100 ms, whichever comes first.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/opt/project")

from pyflink.datastream.functions import MapFunction  # noqa: E402

from contracts.schema import FEATURES, ONLINE_TTL_SECONDS, TS_KEY  # noqa: E402
from contracts.secrets import read_secret  # noqa: E402
from src.flink.sinks.ann_index import ensure_index, index_mapping, vec_key  # noqa: E402
from src.flink.sinks.feast_codec import decode_ts, encode_row  # noqa: E402

# Invariant 4's shape: inside the compose network Redis is always redis:6379.
# Only the *host* mapping moved to 6380 to coexist with a system redis.
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
#: Read from `REDIS_PASSWORD_FILE` when the topology mounts one, so the value
#: never reaches `docker inspect`. See contracts/secrets.py.
REDIS_PASSWORD = read_secret("REDIS_PASSWORD") or None

#: Sentinel discovery for the production topology. Writing to a fixed host would
#: undo the failover the topology exists to provide: after a promotion the old
#: primary is gone (or demoted to a read-only replica) and every write fails
#: with READONLY. Empty means "connect directly", which is the dev stack.
REDIS_SENTINELS = os.environ.get("REDIS_SENTINELS", "").strip()
REDIS_SENTINEL_MASTER = os.environ.get("REDIS_SENTINEL_MASTER", "mymaster")

#: CA bundle for verifying the Redis/Sentinel server certificates. Empty means
#: plaintext (the dev stack). The production topology is TLS-only — `port 0`
#: disables the plaintext listener — so without this the sink cannot connect at
#: all rather than silently falling back to an unencrypted connection.
REDIS_TLS_CA = os.environ.get("REDIS_TLS_CA", "").strip()


def _tls_kwargs() -> dict:
    if not REDIS_TLS_CA:
        return {}
    return {
        "ssl": True,
        "ssl_ca_certs": REDIS_TLS_CA,
        # Verify the server chain. `none` would encrypt the transport while
        # accepting any certificate, which defeats most of the point.
        "ssl_cert_reqs": "required",
    }

FLUSH_ROWS = int(os.environ.get("SINK_FLUSH_ROWS", "500"))
FLUSH_MS = int(os.environ.get("SINK_FLUSH_MS", "100"))

#: Maintain the parallel RediSearch HNSW index alongside the Feast hash.
#: Feast cannot do vector search on Redis (ADR-004), so similarity search needs
#: a second copy of the vector as a raw float32 blob. Set 0 to skip it.
WRITE_ANN_INDEX = os.environ.get("SINK_ANN_INDEX", "1") not in ("0", "false", "")


class FeastRedisWriter(MapFunction):
    """Buffers rows, then writes them as Feast-encoded hashes.

    Row layout is positional and comes from `enriched_sql()`:
        0 user_id, 1 window_start, 2 window_end, 3..8 numeric features,
        9 behavior_summary, 10 embedding
    """

    def open(self, runtime_context):
        import redis

        if REDIS_SENTINELS:
            from redis.sentinel import Sentinel

            nodes = []
            for hp in REDIS_SENTINELS.split(","):
                host, _, port = hp.strip().partition(":")
                nodes.append((host, int(port or 26379)))
            tls = _tls_kwargs()
            # The sentinels themselves are TLS too, so sentinel_kwargs needs the
            # same settings — otherwise discovery fails while the data
            # connection would have worked.
            sentinel_kwargs = dict(tls)
            if REDIS_PASSWORD:
                sentinel_kwargs["password"] = REDIS_PASSWORD
            sentinel = Sentinel(
                nodes,
                socket_timeout=5,
                password=REDIS_PASSWORD,
                sentinel_kwargs=sentinel_kwargs,
                **tls,
            )
            # master_for() re-resolves on every failover, so a promotion costs a
            # reconnect rather than a stream of READONLY errors.
            self._client = sentinel.master_for(
                REDIS_SENTINEL_MASTER,
                socket_timeout=5,
                password=REDIS_PASSWORD,
                health_check_interval=30,
                **tls,
            )
        else:
            self._client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                password=REDIS_PASSWORD,
                socket_timeout=5, socket_connect_timeout=5,
                health_check_interval=30,
                **_tls_kwargs(),
            )
        self._client.ping()          # fail at open(), not on the first row

        self._ann = WRITE_ANN_INDEX
        if self._ann:
            try:
                ensure_index(self._client)
            except Exception:
                # An index we cannot build must not take the pipeline down —
                # it is an optional retrieval path, not the feature store.
                self._ann = False

        self._buf: list[tuple[int, dict, datetime]] = []
        self._last_flush = time.monotonic()
        self.written = 0
        self.skipped_stale = 0

    def _row_to_features(self, row) -> tuple[int, dict, datetime]:
        user_id = row[0]
        window_end = row[2]
        # window_end arrives as a datetime; make it tz-aware so the Timestamp
        # proto and any comparison against a stored _ts agree on a zone.
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        values = {
            "txn_count_10m": row[3],
            "txn_amount_sum_10m": row[4],
            "txn_amount_avg_10m": row[5],
            "txn_amount_max_10m": row[6],
            "txn_amount_stddev_10m": row[7],
            "distinct_merchants_10m": row[8],
            "behavior_summary": row[9],
            "embedding": row[10],
            "window_end": int(window_end.timestamp()),
        }
        missing = set(FEATURES) - set(values)
        if missing:
            raise KeyError(f"sink is not writing contract features: {sorted(missing)}")
        return user_id, values, window_end

    def _flush(self) -> None:
        if not self._buf:
            return

        encoded = [encode_row(uid, feats, ts) for uid, feats, ts in self._buf]

        # Staleness guard. Feast's own default write path reads `_ts` first and
        # refuses to regress a newer value to an older one; our sink bypasses
        # online_write_batch, so it has to do the same. HOP windows fire in
        # window_end order per key, but a replay after a Flink restart would
        # otherwise overwrite fresh values with stale ones.
        with self._client.pipeline(transaction=False) as pipe:
            for key, _ in encoded:
                pipe.hget(key, TS_KEY)
            existing = pipe.execute()

        with self._client.pipeline(transaction=False) as pipe:
            n = 0
            for (key, mapping), (_uid, _feats, ts), prev_raw in zip(
                encoded, self._buf, existing
            ):
                prev = decode_ts(prev_raw)
                if prev is not None:
                    prev = prev.replace(tzinfo=timezone.utc)
                    if ts <= prev:
                        self.skipped_stale += 1
                        continue
                pipe.hset(key, mapping=mapping)
                # FeatureView.ttl does NOT expire Redis keys, and because this
                # sink bypasses online_write_batch, key_ttl_seconds in the yaml
                # would never be applied either. The sink must do it.
                pipe.expire(key, ONLINE_TTL_SECONDS)

                if self._ann:
                    vk = vec_key(_uid)
                    pipe.hset(vk, mapping=index_mapping(
                        _uid, _feats["embedding"], _feats["behavior_summary"]))
                    pipe.expire(vk, ONLINE_TTL_SECONDS)
                n += 1
            if n:
                pipe.execute()

        self.written += n
        self._buf.clear()
        self._last_flush = time.monotonic()

    def map(self, row):
        self._buf.append(self._row_to_features(row))
        due = (len(self._buf) >= FLUSH_ROWS
               or (time.monotonic() - self._last_flush) * 1000 >= FLUSH_MS)
        if due:
            self._flush()
        # A map must return something; the value is unused downstream.
        return f"{row[0]}"

    def close(self):
        try:
            self._flush()
        finally:
            client = getattr(self, "_client", None)
            if client is not None:
                client.close()
