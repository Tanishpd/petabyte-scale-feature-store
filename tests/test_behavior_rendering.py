"""Phase 5 tests for the logic that decides what gets embedded.

Runs on the host venv: `behavior.py` imports neither pyflink nor torch, which is
the whole reason it is a separate module from `embeddings.py`.

Determinism is the property under test. If identical window content can render
two different sentences, it embeds to two different vectors — which destroys the
similarity property the embedding exists for and makes the Phase 6 round-trip
non-reproducible.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.flink.udfs.behavior import render_sentence, sorted_categories  # noqa: E402


# --- sorted_categories -----------------------------------------------------

def test_sorts_regardless_of_input_order():
    """ARRAY_AGG's element order is undefined and the producer delivers events
    out of order on purpose, so the same set must always render identically."""
    a = sorted_categories(["rideshare", "coffee_shop", "grocery"])
    b = sorted_categories(["grocery", "rideshare", "coffee_shop"])
    c = sorted_categories(["coffee_shop", "grocery", "rideshare"])
    assert a == b == c == "coffee_shop, grocery, rideshare"


def test_deduplicates_even_though_distinct_should_have():
    """Guards the case where someone drops DISTINCT from the aggregate."""
    assert sorted_categories(["a", "b", "a", "a", "b"]) == "a, b"


@pytest.mark.parametrize("empty", [None, [], ()])
def test_empty_input_is_empty_string(empty):
    assert sorted_categories(empty) == ""


def test_coerces_non_str_elements():
    assert sorted_categories([1, 2, 1]) == "1, 2"


# --- render_sentence -------------------------------------------------------

def test_sentence_shape():
    s = render_sentence(7, 23.4, 91.0, "coffee_shop, rideshare")
    assert s == ("user made 7 transactions averaging 23.40, max 91.00, "
                 "across coffee_shop, rideshare")


def test_amounts_are_two_decimals_so_noise_does_not_change_the_vector():
    """Float jitter far below currency precision must not produce a different
    sentence, or otherwise-identical windows would embed differently."""
    assert render_sentence(3, 10.00000001, 20.0, "x") == render_sentence(3, 10.0, 20.0, "x")


def test_count_is_rendered_as_an_integer():
    # Flink hands BIGINT counts over as int, but a float would render "7.0".
    assert "made 7 transactions" in render_sentence(7.0, 1.0, 2.0, "x")


def test_empty_categories_gets_a_phrase_not_a_dangling_across():
    s = render_sentence(1, 5.0, 5.0, "")
    assert s.endswith("across no categories")


def test_identical_aggregates_render_identically():
    args = (12, 45.678, 300.0, "airline, hotel")
    assert render_sentence(*args) == render_sentence(*args)


def test_different_aggregates_render_differently():
    """Sanity: the sentence must actually carry the aggregate's information,
    or every user would embed to the same vector."""
    base = render_sentence(7, 23.4, 91.0, "coffee_shop")
    assert base != render_sentence(8, 23.4, 91.0, "coffee_shop")
    assert base != render_sentence(7, 23.5, 91.0, "coffee_shop")
    assert base != render_sentence(7, 23.4, 92.0, "coffee_shop")
    assert base != render_sentence(7, 23.4, 91.0, "airline")


def test_no_currency_symbol():
    """Amounts are in each user's local currency, so a '$' would be wrong for
    most rows."""
    assert "$" not in render_sentence(7, 23.4, 91.0, "coffee_shop")
