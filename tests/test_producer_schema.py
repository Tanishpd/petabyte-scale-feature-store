"""Phase 2 tests: the generated stream must have the structure that makes the
rest of the pipeline meaningful. No Kafka required — generation is deliberately
separable from sending.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from contracts.events import (  # noqa: E402
    COUNTRIES,
    CURRENCIES,
    DEVICES,
    EVENT_FIELDS,
    MERCHANT_CATEGORIES,
    ROWTIME_WIRE_FIELD,
)
from src.producer.generate_stream import CATEGORY_MODEL, EventGenerator  # noqa: E402


@pytest.fixture(scope="module")
def events():
    gen = EventGenerator(n_users=5_000, seed=1234)
    evs, _, _ = gen.batch(20_000)
    return evs


# --- contract conformance --------------------------------------------------

def test_fields_match_contract_exactly(events):
    """Producer keys and contracts/events.py must not drift — the Flink source
    DDL is generated from the contract, so a mismatch means NULL columns."""
    expected = set(EVENT_FIELDS)
    for e in events[:200]:
        assert set(e) == expected, f"field drift: {set(e) ^ expected}"


def test_json_types_match_contract(events):
    py_for_json = {
        "string": str, "integer": int, "number": (int, float), "boolean": bool,
    }
    for e in events[:500]:
        for name, (json_type, _flink) in EVENT_FIELDS.items():
            assert isinstance(e[name], py_for_json[json_type]), (
                f"{name}={e[name]!r} is {type(e[name]).__name__}, want {json_type}"
            )


def test_events_are_json_serialisable_and_reasonably_sized(events):
    sizes = [len(json.dumps(e, separators=(",", ":")).encode()) for e in events[:2000]]
    avg = sum(sizes) / len(sizes)
    # docs/SCALING.md derives every volume number from ~350 B/event.
    assert 250 <= avg <= 450, f"avg event size {avg:.0f}B breaks SCALING.md's 350B basis"


def test_timestamp_parses_as_iso8601(events):
    """Flink is configured with json.timestamp-format.standard = ISO-8601;
    if the producer emitted the SQL format instead, rowtimes would be NULL and
    no window would ever fire."""
    for e in events[:500]:
        ts = e[ROWTIME_WIRE_FIELD]
        assert ts.endswith("Z") and "T" in ts, ts
        datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def test_enum_values_are_in_contract(events):
    for e in events[:2000]:
        assert e["merchant_category"] in MERCHANT_CATEGORIES
        assert e["device"] in DEVICES
        assert e["country"] in COUNTRIES
        assert e["currency"] == CURRENCIES[e["country"]]


def test_merchant_id_maps_to_stable_name_and_category(events):
    """A merchant_id must not mean two different merchants, or COUNT(DISTINCT
    merchant_id) in the Flink aggregation would be meaningless."""
    seen: dict[int, tuple[str, str]] = {}
    for e in events:
        ident = (e["merchant_name"], e["merchant_category"])
        prev = seen.setdefault(e["merchant_id"], ident)
        assert prev == ident, f"merchant_id {e['merchant_id']} is both {prev} and {ident}"


# --- the four properties that justify the generator ------------------------

def test_user_ids_are_zipf_skewed_not_uniform(events):
    """Hot keys are the point (docs/SCALING.md §6). Uniform users would give a
    perfectly balanced keyspace that no real workload has."""
    counts = Counter(e["user_id"] for e in events)
    total = sum(counts.values())
    top1pct = max(1, len(counts) // 100)
    share = sum(c for _, c in counts.most_common(top1pct)) / total
    assert share > 0.10, f"top 1% of users hold only {share:.1%} — not skewed"


def test_users_stay_within_the_declared_population():
    """np.random.zipf is unbounded and would invent ids; the CDF approach must
    keep the population closed."""
    gen = EventGenerator(n_users=1_000, seed=7)
    evs, _, _ = gen.batch(20_000)
    ids = {e["user_id"] for e in evs}
    assert min(ids) >= 1 and max(ids) <= 1_000, (min(ids), max(ids))


def test_anomaly_rate_is_near_target_and_moves_the_mean():
    gen = EventGenerator(n_users=2_000, anomaly_rate=0.02, seed=99)
    evs, n_anom, _ = gen.batch(40_000)
    assert 0.015 <= n_anom / len(evs) <= 0.025, n_anom / len(evs)

    # An anomaly must be large enough to actually shift a rolling average.
    amounts = sorted(e["amount"] for e in evs)
    p50 = amounts[len(amounts) // 2]
    p999 = amounts[int(len(amounts) * 0.999)]
    assert p999 > 20 * p50, f"p99.9={p999:.0f} vs median={p50:.0f} — anomalies too tame"


def test_lateness_is_injected_and_bounded():
    """Late events are sent now with an older timestamp. They must not exceed
    the watermark delay, or Flink would legitimately drop them."""
    max_late = 30.0
    gen = EventGenerator(n_users=1_000, late_rate=0.25, max_lateness_s=max_late, seed=5)
    now = 1_800_000_000.0
    evs, _, n_late = gen.batch(8_000, now=now)
    assert 0.20 <= n_late / len(evs) <= 0.30

    lags = []
    for e in evs:
        ts = datetime.strptime(e[ROWTIME_WIRE_FIELD], "%Y-%m-%dT%H:%M:%S.%fZ")
        lags.append(now - ts.replace(tzinfo=timezone.utc).timestamp())
    assert max(lags) <= max_late + 1.0, f"lateness {max(lags):.1f}s exceeds {max_late}s"
    assert min(lags) >= -1.0, "events must not be timestamped in the future"


def test_each_user_transacts_in_exactly_one_currency(events):
    """Country follows the user, not the event. Otherwise txn_amount_avg_10m
    averages USD with BRL and means nothing to a model."""
    seen: dict[int, tuple[str, str]] = {}
    for e in events:
        pair = (e["country"], e["currency"])
        prev = seen.setdefault(e["user_id"], pair)
        assert prev == pair, f"user {e['user_id']} transacts in both {prev} and {pair}"


def test_amounts_are_fx_scaled_to_the_local_currency():
    """A coffee should read as ~R$15 / ~₹350, not ~R$4 / ~₹4."""
    gen = EventGenerator(n_users=4_000, anomaly_rate=0.0, seed=3)
    evs, _, _ = gen.batch(60_000)
    coffee: dict[str, list[float]] = {}
    for e in evs:
        if e["merchant_category"] == "coffee_shop":
            coffee.setdefault(e["currency"], []).append(e["amount"])

    med = {c: sorted(v)[len(v) // 2] for c, v in coffee.items() if len(v) > 30}
    assert "USD" in med and "INR" in med, med
    # INR is ~83x USD; allow a wide band, just prove scaling happened at all.
    assert med["INR"] > 20 * med["USD"], f"INR coffee {med['INR']:.0f} vs USD {med['USD']:.2f}"


def test_amounts_differ_by_category():
    """A coffee and a flight must not cost the same, or the behaviour sentence
    carries no signal for the embedding."""
    gen = EventGenerator(n_users=2_000, anomaly_rate=0.0, seed=11)
    evs, _, _ = gen.batch(60_000)
    by_cat: dict[str, list[float]] = {}
    for e in evs:
        by_cat.setdefault(e["merchant_category"], []).append(e["amount"])
    med = {c: sorted(v)[len(v) // 2] for c, v in by_cat.items() if len(v) > 50}
    assert med["airline"] > 10 * med["coffee_shop"], med


def test_generation_is_reproducible_under_a_seed():
    a, _, _ = EventGenerator(n_users=1_000, seed=42).batch(200, now=1_800_000_000.0)
    b, _, _ = EventGenerator(n_users=1_000, seed=42).batch(200, now=1_800_000_000.0)
    assert a == b


def test_category_model_covers_every_declared_category():
    assert set(CATEGORY_MODEL) == set(MERCHANT_CATEGORIES)
