"""What gets dead-lettered, and what must not.

These cover the decision `--on-bad-record dlq` makes on every record. They need
no Flink and no cluster, which is the point — the routing rule is the part worth
testing continuously, and a test that needs a running topology gets run once.

The pairing that matters throughout: for each way a record can be bad there is
also a case asserting a *good* record still parses. A parser that rejected
everything would satisfy the negative half alone while stopping the pipeline
dead.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from contracts.events import EVENT_FIELDS, ROWTIME_COLUMN  # noqa: E402
from src.flink.event_parser import PARSED_FIELDS, parse_event  # noqa: E402

GOOD = {
    "event_id": "1754300000000-000000001",
    "user_id": 42,
    "merchant_id": 7,
    "merchant_name": "Blue Bottle Coffee",
    "merchant_category": "coffee_shop",
    "amount": 12.75,
    "currency": "USD",
    "country": "US",
    "device": "ios",
    "is_card_present": False,
    "event_timestamp": "2026-08-04T12:00:00.123Z",
}


def _payload(**overrides) -> str:
    d = dict(GOOD)
    d.update(overrides)
    return json.dumps(d)


# --- the happy path, first ------------------------------------------------

def test_a_good_event_parses():
    values = parse_event(_payload())
    assert len(values) == len(PARSED_FIELDS)


def test_field_order_matches_the_contract():
    """The Row is positional, so a reordering here silently shifts every column
    into the wrong one — types would still line up for the four STRINGs."""
    assert PARSED_FIELDS[:-1] == [n for n in EVENT_FIELDS if n != ROWTIME_COLUMN]
    assert PARSED_FIELDS[-1] == "ts_millis"


def test_rowtime_becomes_epoch_millis_in_utc():
    values = parse_event(_payload())
    # 2026-08-04T12:00:00.123Z
    assert values[PARSED_FIELDS.index("ts_millis")] == 1785844800123


def test_an_explicit_offset_is_honoured_not_ignored():
    """+02:00 is 2h EARLIER in UTC. Dropping the offset would shift the event
    two hours and quietly place it in the wrong window."""
    utc = parse_event(_payload(event_timestamp="2026-08-04T12:00:00.000Z"))
    plus2 = parse_event(_payload(event_timestamp="2026-08-04T14:00:00.000+02:00"))
    i = PARSED_FIELDS.index("ts_millis")
    assert utc[i] == plus2[i]


# --- what must be dead-lettered -------------------------------------------

def test_unparseable_bytes_are_rejected():
    with pytest.raises(Exception):
        parse_event("this-is-not-json")


def test_a_json_array_is_rejected():
    """Valid JSON, wrong shape. `[1,2,3]` parses fine and has no fields."""
    with pytest.raises(TypeError):
        parse_event("[1,2,3]")


def test_a_missing_rowtime_is_fatal():
    """Unlike other columns, a NULL rowtime stalls the watermark and the job
    produces nothing while reporting success (ARCHITECTURE §4)."""
    d = dict(GOOD)
    del d["event_timestamp"]
    with pytest.raises(ValueError):
        parse_event(json.dumps(d))


def test_an_unparseable_rowtime_is_fatal():
    with pytest.raises(Exception):
        parse_event(_payload(event_timestamp="last Tuesday"))


@pytest.mark.parametrize("field,bad", [
    ("user_id", "forty-two"),      # string where BIGINT expected
    ("user_id", True),             # bool is an int subclass in Python, not in Flink
    ("amount", "12.75"),           # numeric string is NOT a DOUBLE
    ("is_card_present", "false"),  # the string "false" is not a BOOLEAN
    ("merchant_name", 12345),      # number where STRING expected
])
def test_wrong_json_types_are_rejected(field, bad):
    """Flink will not coerce these either, so the SQL source would drop them —
    invisibly. Here they become dead-letter records instead."""
    with pytest.raises(TypeError):
        parse_event(_payload(**{field: bad}))


# --- what must NOT be dead-lettered ---------------------------------------

def test_a_missing_non_rowtime_field_is_allowed():
    """`json.fail-on-missing-field = false` on the SQL source, so a missing
    optional field is a NULL column. The DLQ path must agree, or it would
    quarantine records the pipeline has always accepted."""
    d = dict(GOOD)
    del d["device"]
    values = parse_event(json.dumps(d))
    assert values[PARSED_FIELDS.index("device")] is None


def test_an_explicit_null_is_allowed():
    values = parse_event(_payload(device=None))
    assert values[PARSED_FIELDS.index("device")] is None


def test_an_integer_is_accepted_for_a_double_column():
    """JSON has one number type; `amount: 13` is legitimate and Flink widens it."""
    values = parse_event(_payload(amount=13))
    assert values[PARSED_FIELDS.index("amount")] == 13.0


def test_unknown_extra_fields_are_ignored():
    """A producer adding a field must not break the consumer — that is the
    whole point of tolerating schema evolution in one direction."""
    values = parse_event(_payload(new_field_from_a_future_producer="whatever"))
    assert len(values) == len(PARSED_FIELDS)


def test_a_missing_entity_key_is_fatal():
    """user_id is the entity key. A NULL survives parsing and the aggregate,
    then kills the sink at int(None) in entity_key() — observed as a restart
    loop with the traceback buried in the SDK harness. Every other column is
    optional; this one cannot be."""
    d = dict(GOOD)
    del d["user_id"]
    with pytest.raises(ValueError):
        parse_event(json.dumps(d))


def test_an_explicitly_null_entity_key_is_fatal():
    with pytest.raises(ValueError):
        parse_event(_payload(user_id=None))
