"""THE load-bearing test (ADR-005).

Every risk introduced by writing Redis directly instead of going through
`online_write_batch` collapses into one question: **does what Flink writes equal
what Feast reads?** This answers it by writing through the production sink's
codec and reading back with a real `FeatureStore.get_online_features()`.

Without this, the failure mode is a pipeline that looks healthy end to end while
serving `None` — writes succeed, reads miss, and nothing errors on either side.

Needs the stack up (`make up`) and the registry applied (`make seed && make apply`).
"""

from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

REPO_DIR = ROOT / "src" / "feast_repo"

from contracts.schema import (  # noqa: E402
    EMBEDDING_DIM,
    ENTITY_JOIN_KEY,
    FEATURE_VIEW_NAME,
    FEATURES,
    ONLINE_TTL_SECONDS,
    TS_KEY,
)
from src.flink.sinks.feast_codec import (  # noqa: E402
    decode_ts,
    encode_row,
    feature_refs,
    redis_key,
)

REDIS_PORT = 6380          # host mapping; inside the network it is redis:6379
TEST_USER_BASE = 990_000   # far outside the generator's id range


def _redis():
    import redis
    c = redis.Redis(host="localhost", port=REDIS_PORT, socket_connect_timeout=2)
    c.ping()
    return c


try:
    _redis().close()
    _REDIS_UP = True
except Exception:
    _REDIS_UP = False

pytestmark = [
    pytest.mark.skipif(not _REDIS_UP, reason=f"redis not on :{REDIS_PORT} — run `make up`"),
    pytest.mark.skipif(not (REPO_DIR / "data" / "registry.db").exists(),
                       reason="registry not built — run `make seed && make apply`"),
]


@pytest.fixture(scope="module")
def store():
    from feast import FeatureStore
    return FeatureStore(repo_path=str(REPO_DIR))


@pytest.fixture
def client():
    c = _redis()
    yield c
    c.close()


def sample_features(seed: int = 0) -> dict:
    return {
        "txn_count_10m": 7 + seed,
        "txn_amount_sum_10m": 163.84 + seed,
        "txn_amount_avg_10m": 23.405714285714286,
        "txn_amount_max_10m": 91.0,
        "txn_amount_stddev_10m": 28.7431,
        "distinct_merchants_10m": 5,
        "behavior_summary": "coffee_shop, grocery, rideshare",
        # Deliberately not round numbers: float32 storage must survive them.
        "embedding": [(i % 97) / 97.0 - 0.5 for i in range(EMBEDDING_DIM)],
        "window_end": int(datetime(2026, 8, 3, 9, 0, tzinfo=timezone.utc).timestamp()),
    }


def write(client, user_id: int, features: dict, ts: datetime) -> bytes:
    key, mapping = encode_row(user_id, features, ts)
    client.hset(key, mapping=mapping)
    client.expire(key, ONLINE_TTL_SECONDS)
    return key


def read(store, user_id: int, names=None) -> dict:
    return store.get_online_features(
        features=feature_refs(names),
        entity_rows=[{ENTITY_JOIN_KEY: user_id}],
    ).to_dict()


# --- the acceptance test ---------------------------------------------------

def test_roundtrip_every_feature(store, client):
    """Write through the sink's codec, read through Feast, compare field by field."""
    uid = TEST_USER_BASE + 1
    feats = sample_features()
    key = write(client, uid, feats, datetime.now(timezone.utc))

    try:
        got = read(store, uid)

        assert got[ENTITY_JOIN_KEY] == [uid]
        for name, expected in feats.items():
            actual = got[name][0]
            assert actual is not None, (
                f"{name} read back as None — the field hash Feast looked up did "
                f"not match the one the codec wrote (Invariant 1)"
            )
            if name == "embedding":
                assert len(actual) == EMBEDDING_DIM
                # Stored as float32, so compare at float32 precision.
                for i, (a, e) in enumerate(zip(actual, expected)):
                    assert math.isclose(a, e, rel_tol=1e-6, abs_tol=1e-6), \
                        f"embedding[{i}]: {a} != {e}"
            elif name == "window_end":
                # Asymmetric on purpose: a UnixTimestamp feature is WRITTEN as
                # int64 seconds (ValueProto.unix_timestamp_val) and READ BACK as
                # a datetime. Same instant, different Python type — a consumer
                # comparing the raw int to the read value would see a spurious
                # mismatch. See test_unix_timestamp_reads_back_as_datetime.
                assert isinstance(actual, datetime)
                assert int(actual.replace(tzinfo=timezone.utc).timestamp()) == expected
            elif isinstance(expected, float):
                assert math.isclose(actual, expected, rel_tol=1e-12), f"{name}"
            else:
                assert actual == expected, f"{name}"
    finally:
        client.delete(key)


def test_every_contract_feature_is_covered_by_the_roundtrip():
    """Guards the test itself: a feature added to the contract but not to
    sample_features would otherwise be silently untested."""
    assert set(sample_features()) == set(FEATURES)


# --- the specific failure modes the design worries about -------------------

def test_partial_write_leaves_other_features_intact(store, client):
    uid = TEST_USER_BASE + 2
    now = datetime.now(timezone.utc)
    key = write(client, uid, sample_features(), now)
    try:
        # A second write of one field only.
        k2, mapping = encode_row(uid, {"txn_count_10m": 999}, now + timedelta(seconds=1))
        assert k2 == key
        client.hset(key, mapping=mapping)

        got = read(store, uid)
        assert got["txn_count_10m"][0] == 999
        assert got["behavior_summary"][0] == "coffee_shop, grocery, rideshare"
    finally:
        client.delete(key)


def test_unix_timestamp_reads_back_as_datetime(store, client):
    """Pins the write/read type asymmetry so it is documented rather than
    rediscovered: int64 seconds in, `datetime` out."""
    uid = TEST_USER_BASE + 7
    when = datetime(2026, 8, 3, 9, 0, tzinfo=timezone.utc)
    feats = {**sample_features(), "window_end": int(when.timestamp())}
    key = write(client, uid, feats, datetime.now(timezone.utc))
    try:
        got = read(store, uid, ["window_end"])["window_end"][0]
        assert isinstance(got, datetime), f"expected datetime, got {type(got).__name__}"
        assert got.replace(tzinfo=timezone.utc) == when
    finally:
        client.delete(key)


def test_unknown_user_reads_as_none_not_error(store):
    got = read(store, TEST_USER_BASE + 12345)
    assert got["txn_amount_avg_10m"][0] is None


def test_key_is_stable_and_user_specific():
    a1 = redis_key(TEST_USER_BASE)
    a2 = redis_key(TEST_USER_BASE)
    b = redis_key(TEST_USER_BASE + 1)
    assert a1 == a2 and a1 != b
    assert isinstance(a1, bytes)


def test_ts_field_is_written_and_parseable(client):
    uid = TEST_USER_BASE + 3
    now = datetime(2026, 8, 3, 9, 30, tzinfo=timezone.utc)
    key = write(client, uid, sample_features(), now)
    try:
        stored = decode_ts(client.hget(key, TS_KEY))
        assert stored is not None
        assert abs((stored.replace(tzinfo=timezone.utc) - now).total_seconds()) < 1
    finally:
        client.delete(key)


def test_expire_is_set_because_ttl_does_not_do_it(client):
    """FeatureView.ttl bounds offline joins only; without the sink's EXPIRE an
    inactive user would serve its last window forever, returned as PRESENT."""
    uid = TEST_USER_BASE + 4
    key = write(client, uid, sample_features(), datetime.now(timezone.utc))
    try:
        ttl = client.ttl(key)
        assert 0 < ttl <= ONLINE_TTL_SECONDS
    finally:
        client.delete(key)


def test_embedding_survives_float32_narrowing(store, client):
    """Feast stores Array(Float32); values must not come back mangled."""
    uid = TEST_USER_BASE + 5
    vec = [1.0, -1.0, 0.0, 0.5, -0.25] + [0.0] * (EMBEDDING_DIM - 5)
    feats = {**sample_features(), "embedding": vec}
    key = write(client, uid, feats, datetime.now(timezone.utc))
    try:
        got = read(store, uid, ["embedding"])["embedding"][0]
        assert len(got) == EMBEDDING_DIM
        assert got[:5] == pytest.approx([1.0, -1.0, 0.0, 0.5, -0.25], abs=1e-7)
    finally:
        client.delete(key)


def test_a_drifted_feature_name_reads_as_none(store, client):
    """Demonstrates the failure this whole design guards against: hashing a
    name that differs by one character writes successfully and reads nothing."""
    from feast.infra.online_stores.helpers import _mmh3
    from feast.protos.feast.types.Value_pb2 import Value as ValueProto

    uid = TEST_USER_BASE + 6
    key = redis_key(uid)
    try:
        # "1Om" instead of "10m" — writes fine, and is invisible to the writer.
        client.hset(key, mapping={
            _mmh3(f"{FEATURE_VIEW_NAME}:txn_amount_avg_1Om"):
                ValueProto(double_val=42.0).SerializeToString(),
        })
        got = read(store, uid, ["txn_amount_avg_10m"])
        assert got["txn_amount_avg_10m"][0] is None, (
            "a drifted name was readable — the contract is not actually load-bearing"
        )
    finally:
        client.delete(key)
