"""Embedding UDFs: window aggregate -> behaviour sentence -> 384-dim vector.

Two decisions from ADR-006 and PLAN.md Phase 5 shape this file:

**Embed on window close, not per event.** These UDFs run on the *aggregated*
output — one vector per user per slide interval, never one per transaction. The
governing formula is `active_entities / slide_seconds`, which does not contain
the event rate at all: ~830/s for 50k active users on a 1-minute slide, versus
1,000,000/s if you embed each event. See docs/SCALING.md §3.

**Pandas UDF, not scalar.** `func_type="pandas"` hands `eval` whole Series, so
one `model.encode(...)` call covers a batch. A row-at-a-time scalar UDF pays
Python-boundary overhead per row and forfeits intra-batch tensor parallelism.

The model is baked into the image and runs under `HF_HUB_OFFLINE=1`
(Invariant 6): a UDF that reaches for the network on first call turns a cold
start into a task-manager timeout.
"""

from __future__ import annotations

import os

import pandas as pd
from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf

# Pure logic lives in behavior.py, which imports neither pyflink nor torch,
# so the host venv can unit-test it. Re-exported for callers.
from src.flink.udfs.behavior import render_sentence, sorted_categories  # noqa: F401

MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

#: Measured on this container: throughput is flat at 410–480 emb/s across
#: batch sizes 32–512 and 1/4/8 torch threads — MiniLM here is bound by memory
#: bandwidth, not by batching width or parallelism. 128 sits at the top of that
#: flat region without holding a large Series in memory.
ENCODE_BATCH_SIZE = int(os.environ.get("EMBEDDING_BATCH_SIZE", "128"))


# --- UDFs ------------------------------------------------------------------

@udf(result_type=DataTypes.STRING())
def categories_udf(categories):
    return sorted_categories(categories)


class BehaviorEmbedder(ScalarFunction):
    """Renders each row's sentence and embeds the whole batch in one call.

    Rendering happens here rather than in a separate UDF so a batch crosses the
    Python boundary once instead of twice.
    """

    def open(self, function_context):
        # Imported inside open() so the module can be imported (and the pure
        # functions tested) on a host that has no torch.
        import torch
        from sentence_transformers import SentenceTransformer

        self._torch = torch
        self.model = SentenceTransformer(MODEL_NAME)
        self.model.eval()
        self.rows = 0
        self.batches = 0

    def eval(self, counts, avgs, maxes, categories):
        texts = [
            render_sentence(c, a, m, cats)
            for c, a, m, cats in zip(counts, avgs, maxes, categories)
        ]
        # inference_mode, not just eval(): skips autograd bookkeeping entirely.
        with self._torch.inference_mode():
            vectors = self.model.encode(
                texts,
                batch_size=ENCODE_BATCH_SIZE,
                convert_to_numpy=True,
                # Unit-normalised, so cosine similarity is a dot product — what
                # the optional RediSearch HNSW index in Phase 8 wants.
                normalize_embeddings=True,
                show_progress_bar=False,
            )
        self.rows += len(texts)
        self.batches += 1
        return pd.Series([v.astype("float32").tolist() for v in vectors])


def build_embed_udf():
    """ARRAY<FLOAT> as a pandas-UDF result type works, but only against a real
    source: on a constant-folded query with no input rows it dies inside
    ArrowWriter.finish with `Index 0 out of bounds for length 0`. That is a
    degenerate-query artefact, not a type limitation — do not conclude from it
    that the embedding needs to be serialised to BYTES.
    """
    return udf(
        BehaviorEmbedder(),
        result_type=DataTypes.ARRAY(DataTypes.FLOAT()),
        func_type="pandas",
    )


def register(t_env) -> None:
    t_env.create_temporary_function("sorted_categories", categories_udf)
    t_env.create_temporary_function("embed_behavior", build_embed_udf())
