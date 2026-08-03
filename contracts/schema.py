"""Single source of truth for the feature view: names, dtypes, window, TTL.

Why this file exists (CLAUDE.md Invariant 1): the Redis field key is
`mmh3(f"{feature_view_name}:{feature_name}")`. If the Flink sink and the Feast
registry disagree by one character, writes succeed and reads return `None` —
with no error on either side. Two independent string literals that must stay
byte-identical, in two processes, in two interpreters, is a bug with no error
path. One import removes the class of failure.

Imported by:
  - src/feast_repo/definitions.py   (host venv, py3.11) -> Feast types
  - src/flink/sinks/feast_redis_sink.py (container, py3.12) -> ValueProto fields
  - src/flink/streaming_job.py      (container) -> window config, column names

Therefore this module imports NOTHING beyond the stdlib. feast and pyflink are
not installed in the same places, and a dependency here would break one side.
"""

from __future__ import annotations

from enum import Enum

# --- identity --------------------------------------------------------------

PROJECT = "petafs"
FEATURE_VIEW_NAME = "user_txn_stats_10m"

ENTITY_NAME = "user"
ENTITY_JOIN_KEY = "user_id"

#: Feast tracks freshness per feature view in this field.
TS_KEY = f"_ts:{FEATURE_VIEW_NAME}"


class DType(str, Enum):
    """Logical types. Each consumer maps these into its own type system:
    Feast `Field(dtype=...)` on one side, `ValueProto` setters on the other."""

    INT64 = "int64"
    FLOAT64 = "float64"
    STRING = "string"
    FLOAT_VECTOR = "float_vector"
    UNIX_TIMESTAMP = "unix_timestamp"


#: The 384 is `all-MiniLM-L6-v2`'s output width. Invariant 6 bakes that model
#: into the image; changing one without the other yields vectors Feast accepts
#: and a model cannot use.
EMBEDDING_DIM = 384

#: Ordered. Order is not load-bearing for correctness (fields are hashed, not
#: positional) but keeps the registry, the parquet, and the sink readable.
FEATURES: dict[str, DType] = {
    "txn_count_10m":          DType.INT64,
    "txn_amount_sum_10m":     DType.FLOAT64,
    "txn_amount_avg_10m":     DType.FLOAT64,   # the rolling average from the brief
    "txn_amount_max_10m":     DType.FLOAT64,
    "txn_amount_stddev_10m":  DType.FLOAT64,
    "distinct_merchants_10m": DType.INT64,
    "behavior_summary":       DType.STRING,
    "embedding":              DType.FLOAT_VECTOR,
    "window_end":             DType.UNIX_TIMESTAMP,
}

# --- windowing (Flink) -----------------------------------------------------

WINDOW_SIZE_MINUTES = 10
WINDOW_SLIDE_MINUTES = 1

#: Must not be less than the producer's injected lateness, or Flink drops
#: events the producer deliberately delayed. See generate_stream.py.
WATERMARK_DELAY_SECONDS = 30

# --- online store ----------------------------------------------------------

#: FeatureView.ttl does NOT expire Redis keys — it bounds offline point-in-time
#: joins only, and the online read path never checks staleness (it returns old
#: values as PRESENT). Real online expiry is online_store.key_ttl_seconds, and
#: because our sink bypasses Feast's online_write_batch, the sink must issue
#: EXPIRE itself. Both read this constant so they cannot drift.
ONLINE_TTL_SECONDS = 30 * 60

#: Feast defaults to 3; pinned explicitly because v2 and v3 encode entity keys
#: differently and a mismatch is a permanent cache miss (Invariant 3).
ENTITY_KEY_SERIALIZATION_VERSION = 3


# --- helpers shared by writer and reader -----------------------------------

def qualified(feature_name: str) -> str:
    """The exact string Feast hashes into a Redis field key.

    The sink calls `_mmh3(qualified(name))`; Feast internally computes
    `_mmh3(f"{feature_view}:{feature_name}")`. Same string, one definition.
    """
    if feature_name not in FEATURES:
        raise KeyError(
            f"{feature_name!r} is not in the feature contract; "
            f"known: {sorted(FEATURES)}"
        )
    return f"{FEATURE_VIEW_NAME}:{feature_name}"


#: Logical dtype -> the ValueProto oneof field the sink must populate.
VALUE_PROTO_FIELD: dict[DType, str] = {
    DType.INT64: "int64_val",
    DType.FLOAT64: "double_val",
    DType.STRING: "string_val",
    DType.FLOAT_VECTOR: "float_list_val",   # takes a FloatList message, not a list
    DType.UNIX_TIMESTAMP: "unix_timestamp_val",
}


def feature_names() -> list[str]:
    return list(FEATURES)


def value_proto_field(feature_name: str) -> str:
    return VALUE_PROTO_FIELD[FEATURES[feature_name]]
