"""A parallel RediSearch HNSW index for nearest-neighbour search.

ADR-004: Feast 0.65's Redis online store has **no vector search** —
`retrieve_online_documents` is implemented for Milvus, Qdrant, FAISS, pgvector,
Elasticsearch, MongoDB and sqlite-vec, but not Redis. So "fetch this user's
embedding" is a normal Feast read, and "find similar users" needs an index we
maintain ourselves.

It has to be a *separate* key, not the Feast hash. Feast stores each feature as
a serialized `ValueProto`, so the embedding field is protobuf framing wrapped
around the floats — RediSearch cannot index that. This module writes the same
vector a second time as a raw little-endian float32 blob under
`petafs:vec:{user_id}`.

The duplication is the honest cost of Redis not being a vector store: ~1.5 KB
per entity on top of the ~4 KB Feast row. See docs/SCALING.md §8.
"""

from __future__ import annotations

import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from contracts.schema import EMBEDDING_DIM, PROJECT  # noqa: E402

INDEX_NAME = f"{PROJECT}_user_vec_idx"
KEY_PREFIX = f"{PROJECT}:vec:"

#: Embeddings are written unit-normalised (`normalize_embeddings=True` in the
#: UDF), so cosine distance is 1 - dot product and ordering is meaningful.
DISTANCE_METRIC = "COSINE"


def vec_key(user_id: int) -> str:
    return f"{KEY_PREFIX}{int(user_id)}"


def pack(vector) -> bytes:
    """Raw little-endian float32 — the layout RediSearch expects."""
    return struct.pack(f"<{len(vector)}f", *[float(x) for x in vector])


def unpack(blob: bytes) -> list[float]:
    return list(struct.unpack(f"<{len(blob) // 4}f", blob))


def index_exists(client, index: str = INDEX_NAME) -> bool:
    try:
        client.execute_command("FT.INFO", index)
        return True
    except Exception:
        return False


def ensure_index(client, dim: int = EMBEDDING_DIM,
                 index: str = INDEX_NAME, prefix: str = KEY_PREFIX) -> bool:
    """Create the index if absent. Returns True if this call created it.

    Must tolerate the race between task slots. Every slot calls this from
    `open()`, so several can pass the FT.INFO check together, all issue
    FT.CREATE, and all but one get back "Index already exists". Treating that
    as a failure is not harmless: the sink disables its ANN writes on any
    exception, so the losing slots stop indexing for the life of the job and
    only *part* of the keyspace ends up searchable — measured 74 of 150
    entities before this was handled.
    """
    if index_exists(client, index):
        return False

    try:
        client.execute_command(
            "FT.CREATE", index,
            "ON", "HASH", "PREFIX", "1", prefix,
            "SCHEMA",
            "user_id", "NUMERIC",
            "behavior_summary", "TEXT",
            "embedding", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32",
            "DIM", str(dim),
            "DISTANCE_METRIC", DISTANCE_METRIC,
        )
        return True
    except Exception:
        # Lost the race, or the index appeared some other way. Either is fine
        # as long as it is actually there now.
        if index_exists(client, index):
            return False
        raise


def index_mapping(user_id: int, vector, behavior_summary: str) -> dict:
    return {
        "user_id": int(user_id),
        "behavior_summary": behavior_summary or "",
        "embedding": pack(vector),
    }


def knn(client, vector, k: int = 5, exclude_user: int | None = None,
        index: str = INDEX_NAME):
    """K nearest users. Returns [(user_id, distance, behavior_summary), ...].

    DIALECT 2 is required for the `*=>[KNN ...]` syntax.
    """
    over_fetch = k + (1 if exclude_user is not None else 0)
    res = client.execute_command(
        "FT.SEARCH", index,
        f"*=>[KNN {over_fetch} @embedding $q AS score]",
        "PARAMS", "2", "q", pack(vector),
        "SORTBY", "score",
        # LIMIT is required. FT.SEARCH defaults to `LIMIT 0 10` and truncates
        # the response to 10 documents no matter how large the KNN k is — so
        # asking for 20 neighbours silently returns 10, with no error.
        "LIMIT", "0", str(over_fetch),
        "RETURN", "3", "score", "user_id", "behavior_summary",
        "DIALECT", "2",
    )

    out = []
    for i in range(1, len(res), 2):
        fields = res[i + 1]
        d = {fields[j].decode(): fields[j + 1] for j in range(0, len(fields), 2)}
        uid = int(d["user_id"])
        if exclude_user is not None and uid == exclude_user:
            continue
        out.append((uid, float(d["score"]), d.get("behavior_summary", b"").decode()))
    return out[:k]
