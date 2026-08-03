"""Pure behaviour-rendering functions, importable anywhere.

Deliberately free of pyflink, torch and sentence-transformers so the host venv
(which has none of them) can unit-test the logic that decides what gets embedded
and what gets stored as `behavior_summary`. `embeddings.py` imports these and
wraps them in UDFs.
"""

from __future__ import annotations


def sorted_categories(categories) -> str:
    """ARRAY<STRING> -> deterministic, sorted, deduplicated string.

    `ARRAY_AGG(DISTINCT ...)` already deduplicates; the set() guards against the
    aggregate being changed to the non-DISTINCT form. Sorting is what makes it
    deterministic: ARRAY_AGG's element order is not defined, and this pipeline
    delivers events out of order on purpose (ADR-010). Without it, two windows
    with identical content could render different sentences and embed to
    different vectors — destroying the similarity property the embedding exists
    for, and making round-trip comparisons irreproducible.
    """
    if categories is None or len(categories) == 0:
        return ""
    return ", ".join(sorted({str(c) for c in categories}))


def render_sentence(count: int, avg: float, mx: float, categories: str) -> str:
    """Render a window aggregate as the sentence that gets embedded.

    Deliberately carries no currency symbol. Amounts are in each user's local
    currency (the producer gives every user one home market), so a "$" would be
    wrong for most rows — and the signal worth embedding is the spending
    *pattern*, not the unit.
    """
    cats = categories or "no categories"
    return (
        f"user made {int(count)} transactions "
        f"averaging {float(avg):.2f}, max {float(mx):.2f}, "
        f"across {cats}"
    )
