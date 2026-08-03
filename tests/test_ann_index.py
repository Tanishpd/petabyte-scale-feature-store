"""Phase 8 tests for the parallel RediSearch HNSW index.

Feast 0.65 cannot do vector search on Redis (ADR-004), so this index is ours to
maintain and ours to test. The tests that matter are the ones covering how it
broke in practice: a create race that silently disabled indexing on one task
slot, leaving only part of the keyspace searchable.
"""

from __future__ import annotations

import struct
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.flink.sinks.ann_index import (  # noqa: E402
    INDEX_NAME,
    KEY_PREFIX,
    ensure_index,
    index_exists,
    index_mapping,
    knn,
    pack,
    unpack,
    vec_key,
)

REDIS_PORT = int(os.environ.get("REDIS_HOST_PORT", "6380"))
TEST_PREFIX_USER = 980_000


def _redis():
    import redis
    c = redis.Redis(host="localhost", port=REDIS_PORT, socket_connect_timeout=2)
    c.ping()
    return c


try:
    _c = _redis()
    _HAS_SEARCH = True
    try:
        _c.execute_command("FT._LIST")
    except Exception:
        _HAS_SEARCH = False
    _c.close()
except Exception:
    _HAS_SEARCH = False


# --- pure encoding (no Redis needed) ---------------------------------------

def test_pack_roundtrip():
    v = [0.0, 1.0, -1.0, 0.5, -0.25]
    assert unpack(pack(v)) == pytest.approx(v, abs=1e-7)


def test_pack_is_little_endian_float32():
    """RediSearch expects raw LE float32; anything else indexes as garbage
    without erroring."""
    blob = pack([1.0, 2.0])
    assert len(blob) == 8
    assert blob == struct.pack("<2f", 1.0, 2.0)


def test_vec_key_is_distinct_from_the_feast_key():
    """The vector must live under its own key: Feast stores the embedding as a
    serialized ValueProto, which RediSearch cannot index."""
    from src.flink.sinks.feast_codec import redis_key
    assert vec_key(42) == f"{KEY_PREFIX}42"
    assert vec_key(42).encode() != redis_key(42)


def test_index_mapping_shape():
    m = index_mapping(7, [0.1, 0.2], "coffee_shop, grocery")
    assert m["user_id"] == 7
    assert m["behavior_summary"] == "coffee_shop, grocery"
    assert isinstance(m["embedding"], bytes)


def test_index_mapping_tolerates_empty_summary():
    assert index_mapping(1, [0.0], None)["behavior_summary"] == ""


# --- against a live RediSearch ---------------------------------------------

pytestmark_live = pytest.mark.skipif(
    not _HAS_SEARCH, reason=f"RediSearch not available on :{REDIS_PORT} — run `make up`")


@pytestmark_live
def test_ensure_index_is_idempotent():
    """The regression that matters: every task slot calls this from open(), so
    losing the create race must NOT be treated as failure — that disabled
    indexing on one slot and left only 74 of 150 entities searchable."""
    c = _redis()
    try:
        ensure_index(c)
        assert index_exists(c)
        # Repeated calls must neither raise nor report a new index.
        for _ in range(3):
            assert ensure_index(c) is False
        assert index_exists(c)
    finally:
        c.close()


def _await_indexed(client, uid: int, timeout_s: float = 10.0,
                   index: str = INDEX_NAME) -> None:
    """Wait for a freshly written vector to become searchable.

    RediSearch indexes asynchronously: `HSET` returns before the document is in
    the HNSW graph, so writing and immediately querying can legitimately miss
    it. Not a correctness problem for the pipeline (features are read by key,
    not by search), but any test that writes then searches has to wait.
    """
    import time as _t
    deadline = _t.monotonic() + timeout_s
    while _t.monotonic() < deadline:
        res = client.execute_command(
            "FT.SEARCH", index, f"@user_id:[{uid} {uid}]",
            "LIMIT", "0", "1", "RETURN", "1", "user_id", "DIALECT", "2",
        )
        if res and res[0]:
            return
        _t.sleep(0.2)
    raise AssertionError(f"vector for {uid} never became searchable in {timeout_s}s")


@pytestmark_live
def test_knn_orders_by_similarity():
    """Ranking is tested against an ISOLATED index, not the production one.

    HNSW is an *approximate* search: over the ~9,000 real vectors the pipeline
    writes, a synthetic query vector is not reliably matched to its own exact
    duplicate, so asserting "exact match ranks first" against the live index
    tests recall rather than the helper. An earlier version of this test did
    exactly that and failed for a correct reason.
    """
    from src.flink.sinks.ann_index import EMBEDDING_DIM

    c = _redis()
    idx, prefix = "petafs_test_vec_idx", "petafs:testvec:"
    pad = lambda v: (v + [0.0] * EMBEDDING_DIM)[:EMBEDDING_DIM]  # noqa: E731
    base = pad([1.0])
    near = pad([0.95, 0.31])
    far = pad([0.0, 1.0])

    try:
        c.execute_command("FT.DROPINDEX", idx, "DD")
    except Exception:
        pass

    ids = [TEST_PREFIX_USER + i for i in range(3)]
    try:
        assert ensure_index(c, index=idx, prefix=prefix) is True
        for uid, v, label in zip(ids, (base, near, far), ("base", "near", "far")):
            c.hset(f"{prefix}{uid}", mapping=index_mapping(uid, v, label))
        _await_indexed(c, ids[-1], index=idx)

        hits = knn(c, base, k=3, index=idx)
        assert [u for u, _, _ in hits] == ids, f"wrong order: {hits}"

        d = {u: dist for u, dist, _ in hits}
        assert d[ids[0]] < d[ids[1]] < d[ids[2]], d
        assert d[ids[0]] == pytest.approx(0.0, abs=1e-5), "exact match should be distance 0"
    finally:
        try:
            c.execute_command("FT.DROPINDEX", idx, "DD")
        except Exception:
            pass
        c.close()


@pytestmark_live
def test_knn_can_exclude_the_query_entity():
    c = _redis()
    ensure_index(c)
    from src.flink.sinks.ann_index import EMBEDDING_DIM
    uid = TEST_PREFIX_USER + 10
    v = [1.0] + [0.0] * (EMBEDDING_DIM - 1)
    try:
        c.hset(vec_key(uid), mapping=index_mapping(uid, v, "solo"))
        _await_indexed(c, uid)
        assert uid not in [u for u, _, _ in knn(c, v, k=3, exclude_user=uid)]
    finally:
        c.delete(vec_key(uid))
        c.close()
