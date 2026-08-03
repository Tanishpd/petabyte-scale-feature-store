"""Phase 3 tests: the contract, the YAML, and the Feast registry must agree.

The load-bearing test here is `test_contract_hash_matches_what_feast_will_look_up`.
Everything else in this pipeline can be right and reads will still return None
if the string our sink hashes differs by one character from the string Feast
hashes. That test compares the actual bytes.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

REPO_DIR = ROOT / "src" / "feast_repo"
PARQUET = REPO_DIR / "data" / "user_txn_stats_10m.parquet"

from contracts.schema import (  # noqa: E402
    EMBEDDING_DIM,
    ENTITY_JOIN_KEY,
    ENTITY_KEY_SERIALIZATION_VERSION,
    FEATURE_VIEW_NAME,
    FEATURES,
    ONLINE_TTL_SECONDS,
    PROJECT,
    TS_KEY,
    DType,
    qualified,
    value_proto_field,
)

pytestmark = pytest.mark.skipif(
    not (REPO_DIR / "data" / "registry.db").exists(),
    reason="registry not built — run `make seed && make apply`",
)


@pytest.fixture(scope="module")
def store():
    from feast import FeatureStore
    return FeatureStore(repo_path=str(REPO_DIR))


@pytest.fixture(scope="module")
def fv(store):
    return store.get_feature_view(FEATURE_VIEW_NAME)


@pytest.fixture(scope="module")
def yaml_cfg():
    return yaml.safe_load((REPO_DIR / "feature_store.yaml").read_text())


# --- the one that matters --------------------------------------------------

def test_contract_hash_matches_what_feast_will_look_up(store, fv):
    """Invariant 1, proven at the byte level.

    Feast's redis online store computes the field key as
        _mmh3(f"{_versioned_fv_name(table, config)}:{feature_name}")
    Our Flink sink will compute
        _mmh3(qualified(feature_name))
    If those differ, writes succeed and reads return None with no error.
    """
    from feast.infra.online_stores.helpers import _mmh3
    from feast.infra.online_stores.redis import _versioned_fv_name

    feast_name = _versioned_fv_name(fv, store.config)
    assert feast_name == FEATURE_VIEW_NAME, (
        f"Feast resolves the FV as {feast_name!r} but the contract says "
        f"{FEATURE_VIEW_NAME!r} — enable_online_feature_view_versioning is "
        f"probably on (Invariant 8)."
    )

    for name in FEATURES:
        feast_field = _mmh3(f"{feast_name}:{name}")
        our_field = _mmh3(qualified(name))
        assert feast_field == our_field, f"field-key drift on {name!r}"
        assert len(our_field) == 4

    assert f"_ts:{feast_name}" == TS_KEY


def test_versioning_guardrail_is_off(store, yaml_cfg):
    """Invariant 8. A fresh FV is v0 and unsuffixed, so turning this on appears
    to work until the first schema change promotes it to v1 — after which reads
    silently return None."""
    assert yaml_cfg["registry"]["enable_online_feature_view_versioning"] is False
    assert store.config.registry.enable_online_feature_view_versioning is False


# --- config mirrors the contract -------------------------------------------

def test_yaml_mirrors_contract(yaml_cfg):
    assert yaml_cfg["project"] == PROJECT
    assert yaml_cfg["entity_key_serialization_version"] == ENTITY_KEY_SERIALIZATION_VERSION
    assert yaml_cfg["online_store"]["key_ttl_seconds"] == ONLINE_TTL_SECONDS
    assert yaml_cfg["online_store"]["type"] == "redis"


def test_online_expiry_is_configured_at_all(yaml_cfg):
    """FeatureView.ttl does not expire Redis keys. If key_ttl_seconds is absent,
    inactive users serve their last window forever, returned as PRESENT."""
    assert yaml_cfg["online_store"].get("key_ttl_seconds"), (
        "no online expiry configured — ttl on the FeatureView does not do this"
    )


def test_redis_port_is_the_host_port(yaml_cfg):
    """6380, not 6379: this machine runs a system redis. Inside the compose
    network the sink still uses redis:6379."""
    assert yaml_cfg["online_store"]["connection_string"].endswith(":6380")


# --- registry matches the contract -----------------------------------------

def test_registry_features_match_contract_exactly(fv):
    """`fv.features` excludes entity columns; `fv.schema` includes them, because
    Feast folds the join key in at apply time. The contract lists features only."""
    registered = {f.name for f in fv.features}
    expected = set(FEATURES)
    assert registered == expected, f"drift: {registered ^ expected}"

    # And the join key really is registered — just as an entity column.
    assert {f.name for f in fv.entity_columns} == {ENTITY_JOIN_KEY}
    assert {f.name for f in fv.schema} == expected | {ENTITY_JOIN_KEY}


def test_registry_dtypes_match_contract(fv):
    from feast.types import Array, Float32, Float64, Int64, String, UnixTimestamp

    want = {
        DType.INT64: Int64, DType.FLOAT64: Float64, DType.STRING: String,
        DType.FLOAT_VECTOR: Array(Float32), DType.UNIX_TIMESTAMP: UnixTimestamp,
    }
    by_name = {f.name: f.dtype for f in fv.features}
    for name, dtype in FEATURES.items():
        assert by_name[name] == want[dtype], f"{name}: {by_name[name]} != {want[dtype]}"


def test_entity_join_key_matches_contract(fv, store):
    assert fv.entities == ["user"]
    entity = store.get_entity("user")
    assert entity.join_key == ENTITY_JOIN_KEY


def test_ttl_matches_contract(fv):
    assert fv.ttl.total_seconds() == ONLINE_TTL_SECONDS


# --- proto mapping used by the sink ----------------------------------------

def test_every_feature_maps_to_a_value_proto_field():
    from feast.protos.feast.types.Value_pb2 import Value as ValueProto

    valid = set(ValueProto.DESCRIPTOR.fields_by_name)
    for name in FEATURES:
        field = value_proto_field(name)
        assert field in valid, f"{name} -> {field} is not a ValueProto field"


def test_embedding_maps_to_float_list():
    assert value_proto_field("embedding") == "float_list_val"
    assert FEATURES["embedding"] is DType.FLOAT_VECTOR


def test_qualified_rejects_unknown_features():
    with pytest.raises(KeyError):
        qualified("txn_amount_avg_5m")   # plausible typo, must not silently pass


# --- the batch source ------------------------------------------------------

@pytest.mark.skipif(not PARQUET.exists(), reason="run `make seed`")
def test_parquet_has_every_contract_column():
    import pyarrow.parquet as pq

    cols = set(pq.read_schema(PARQUET).names)
    missing = set(FEATURES) - cols
    assert not missing, f"batch source missing {missing}"
    assert ENTITY_JOIN_KEY in cols
    assert {"event_timestamp", "created"} <= cols


@pytest.mark.skipif(not PARQUET.exists(), reason="run `make seed`")
def test_parquet_embeddings_have_contract_dimension():
    import pyarrow.parquet as pq

    row = pq.read_table(PARQUET, columns=["embedding"]).slice(0, 1).to_pylist()[0]
    assert len(row["embedding"]) == EMBEDDING_DIM


@pytest.mark.skipif(not PARQUET.exists(), reason="run `make seed`")
def test_behavior_summary_is_sorted_and_deduped():
    """ADR-010: LISTAGG order is undefined and DISTINCT is unusable under HOP,
    so normalisation happens in Python — offline and online must agree on it."""
    import pyarrow.parquet as pq

    vals = pq.read_table(PARQUET, columns=["behavior_summary"]).to_pylist()[:500]
    for v in vals:
        cats = [c.strip() for c in v["behavior_summary"].split(",")]
        assert cats == sorted(set(cats)), f"not sorted/deduped: {v['behavior_summary']}"
