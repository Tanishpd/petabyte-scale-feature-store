"""Feast feature definitions, generated from contracts/schema.py.

Nothing here names a feature directly. The contract is the only place feature
names exist, because the Redis field key is a hash of
`{feature_view}:{feature_name}` and a one-character drift between this registry
and the Flink sink returns None with no error (Invariant 1).
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Array, Float32, Float64, Int64, String, UnixTimestamp

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from contracts.schema import (  # noqa: E402
    EMBEDDING_DIM,
    ENTITY_JOIN_KEY,
    ENTITY_NAME,
    FEATURE_VIEW_NAME,
    FEATURES,
    ONLINE_TTL_SECONDS,
    DType,
)

# Logical dtype -> Feast type. The only place this mapping exists.
_FEAST_TYPE = {
    DType.INT64: Int64,
    DType.FLOAT64: Float64,
    DType.STRING: String,
    DType.FLOAT_VECTOR: Array(Float32),
    DType.UNIX_TIMESTAMP: UnixTimestamp,
}

user = Entity(
    name=ENTITY_NAME,
    join_keys=[ENTITY_JOIN_KEY],
    value_type=ValueType.INT64,
    description="A transacting user. Zipf-distributed in the mock stream, so "
                "the keyspace is genuinely skewed.",
)

# Feast requires a batch source on every FeatureView. Ours is a seeded parquet
# (scripts/seed_offline.py) that exists to satisfy that requirement and to make
# get_historical_features demonstrable. The ONLINE path never reads it — Flink
# writes Redis directly (ADR-003), so this is not the serving source of truth.
txn_stats_batch_source = FileSource(
    name="user_txn_stats_10m_batch",
    path=str(REPO_ROOT / "src" / "feast_repo" / "data" / "user_txn_stats_10m.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="Seeded historical aggregates. Embeddings here are placeholders "
                "— see scripts/seed_offline.py.",
)

user_txn_stats_10m = FeatureView(
    name=FEATURE_VIEW_NAME,
    entities=[user],
    # Bounds offline point-in-time joins and materialization windows ONLY.
    # It does not expire Redis keys; online expiry is key_ttl_seconds in
    # feature_store.yaml, enforced by the sink's EXPIRE.
    ttl=timedelta(seconds=ONLINE_TTL_SECONDS),
    schema=[
        Field(
            name=name,
            dtype=_FEAST_TYPE[dtype],
            **({"vector_index": False} if dtype is DType.FLOAT_VECTOR else {}),
        )
        for name, dtype in FEATURES.items()
    ],
    source=txn_stats_batch_source,
    online=True,
    description=(
        f"Rolling 10-minute behavioural aggregates per user, refreshed every "
        f"minute by a Flink HOP window, plus a {EMBEDDING_DIM}-dim embedding of "
        f"the user's recent behaviour. Written to Redis directly by the Flink "
        f"sink in Feast's own encoding."
    ),
    tags={"phase": "3", "owner": "streaming", "source": "flink"},
)
