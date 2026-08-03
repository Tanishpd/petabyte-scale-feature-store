"""Encode a feature row exactly the way Feast's Redis online store does.

ADR-003: we do **not** reimplement the format. Every primitive here is imported
from Feast itself — `_redis_key`, `_mmh3`, `ValueProto` — so the encoding is
correct by construction and moves with the pinned version rather than drifting
from it.

What Feast's `online_write_batch` builds, and what this module reproduces:

    KEY    = _redis_key(project, entity_key, entity_key_serialization_version)
    FIELDS = { _mmh3(f"{fv}:{feature}") : ValueProto(...).SerializeToString(),
               ...,
               f"_ts:{fv}"              : Timestamp.SerializeToString() }

Correct *encoding* is not the same as a correct *name*: `online_write_batch`
resolves the view name through `_versioned_fv_name()` before hashing, so
`registry.enable_online_feature_view_versioning` must stay false (Invariant 8).

Imports pyflink nowhere, so the host venv can round-trip-test it against a real
`FeatureStore.get_online_features()` — which is the acceptance criterion for
this entire write path (ADR-005).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Iterable

from google.protobuf.timestamp_pb2 import Timestamp

from feast.infra.online_stores.helpers import _mmh3, _redis_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import FloatList
from feast.protos.feast.types.Value_pb2 import Value as ValueProto

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from contracts.schema import (  # noqa: E402
    ENTITY_JOIN_KEY,
    ENTITY_KEY_SERIALIZATION_VERSION,
    FEATURES,
    PROJECT,
    TS_KEY,
    DType,
    qualified,
)


def entity_key(user_id: int) -> EntityKeyProto:
    """Note `entity_values`, not `values`, and ValueProto rather than a bare int."""
    return EntityKeyProto(
        join_keys=[ENTITY_JOIN_KEY],
        entity_values=[ValueProto(int64_val=int(user_id))],
    )


def redis_key(user_id: int, project: str = PROJECT) -> bytes:
    return _redis_key(
        project,
        entity_key(user_id),
        entity_key_serialization_version=ENTITY_KEY_SERIALIZATION_VERSION,
    )


def to_value_proto(name: str, value: Any) -> ValueProto:
    """Python value -> the ValueProto oneof the contract declares for it."""
    dtype = FEATURES[name]
    if value is None:
        return ValueProto()          # Feast reads an empty Value as NULL
    if dtype is DType.INT64:
        return ValueProto(int64_val=int(value))
    if dtype is DType.FLOAT64:
        return ValueProto(double_val=float(value))
    if dtype is DType.STRING:
        return ValueProto(string_val=str(value))
    if dtype is DType.UNIX_TIMESTAMP:
        return ValueProto(unix_timestamp_val=int(value))
    if dtype is DType.FLOAT_VECTOR:
        # float_list_val takes a FloatList *message*; a bare list raises
        # TypeError("Parameter to initialize message field must be ...").
        return ValueProto(float_list_val=FloatList(val=[float(x) for x in value]))
    raise TypeError(f"no ValueProto mapping for {name} ({dtype})")


def encode_row(
    user_id: int,
    features: dict[str, Any],
    event_ts,
    project: str = PROJECT,
) -> tuple[bytes, dict[Any, bytes]]:
    """One feature row -> (redis key, hash mapping) ready for HSET.

    `features` may be a subset; only the keys present are written, which is what
    lets a partial update leave other fields intact.
    """
    unknown = set(features) - set(FEATURES)
    if unknown:
        raise KeyError(f"not in the feature contract: {sorted(unknown)}")

    ts = Timestamp()
    ts.FromDatetime(event_ts)

    mapping: dict[Any, bytes] = {TS_KEY: ts.SerializeToString()}
    for name, value in features.items():
        mapping[_mmh3(qualified(name))] = to_value_proto(name, value).SerializeToString()

    return redis_key(user_id, project), mapping


def decode_ts(raw: bytes | None):
    """Parse a stored `_ts:` field back to a datetime, for the staleness guard."""
    if not raw:
        return None
    ts = Timestamp()
    ts.ParseFromString(raw)
    return ts.ToDatetime()


def feature_refs(names: Iterable[str] | None = None) -> list[str]:
    """`["fv:feature", ...]` as `get_online_features` wants them."""
    return [qualified(n) for n in (names if names is not None else FEATURES)]
