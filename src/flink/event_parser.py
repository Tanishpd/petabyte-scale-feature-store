"""Payload -> event fields, or an exception. No Flink, no PyFlink, no cluster.

Split out of src/flink/dlq.py deliberately: this is the logic that decides
whether a record is dead-lettered, and it is the part most worth testing. Keeping
it free of pyflink imports means tests/test_event_parser.py runs in CI on the
host interpreter alongside the other 61 — the same reasoning that keeps the Feast
codec round-trip testable without Flink (ADR-003).

The rules here mirror what the SQL source would accept:
  * `json.fail-on-missing-field = false`, so a missing field is NULL, not an error
  * a wrong JSON type IS an error — Flink will not coerce `true` into a BIGINT
  * a missing or unparseable rowtime is fatal even though other NULLs are not
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from contracts.events import EVENT_FIELDS, ROWTIME_COLUMN

#: Field order the parser emits: every contract field except the rowtime, which
#: is replaced by epoch millis (see TS_MILLIS_FIELD in dlq.py for why).
PARSED_FIELDS = [n for n in EVENT_FIELDS if n != ROWTIME_COLUMN] + ["ts_millis"]

#: The entity key. Feast hashes it into the Redis key, so a NULL here has no
#: valid encoding — see contracts/schema.py and Invariant 1.
ENTITY_KEY = "user_id"
ENTITY_KEY_INDEX = PARSED_FIELDS.index(ENTITY_KEY)


def _coerce(name: str, value):
    """Coerce one field, raising on anything the SQL source would also reject."""
    json_type, flink_type = EVENT_FIELDS[name]
    if value is None:
        # `json.fail-on-missing-field = false` on the SQL source, so a missing
        # field is a NULL column rather than an error. Match that.
        return None
    if json_type == "string":
        if not isinstance(value, str):
            raise TypeError(f"{name}: expected string, got {type(value).__name__}")
        return value
    if json_type == "integer":
        # bool is a subclass of int in Python; Flink would not accept `true`
        # for a BIGINT column, so neither do we.
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name}: expected integer, got {type(value).__name__}")
        return value
    if json_type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name}: expected number, got {type(value).__name__}")
        return float(value)
    if json_type == "boolean":
        if not isinstance(value, bool):
            raise TypeError(f"{name}: expected boolean, got {type(value).__name__}")
        return value
    raise TypeError(f"{name}: unhandled contract type {json_type}")


def parse_event(payload: str) -> list:
    """Payload -> field values in PARSED_FIELDS order, or raise.

    The raise is what routes a record to the dead-letter topic.
    """
    obj = json.loads(payload)
    if not isinstance(obj, dict):
        raise TypeError(f"expected a JSON object, got {type(obj).__name__}")

    values = [_coerce(n, obj.get(n)) for n in EVENT_FIELDS if n != ROWTIME_COLUMN]

    # The ENTITY KEY is as load-bearing as the rowtime and gets the same
    # treatment. A NULL user_id survives parsing and the windowed aggregate
    # happily produces a NULL group, and then the sink dies on
    # `int(None)` in entity_key() — observed, as a restart loop with the
    # traceback buried in the SDK harness. `json.fail-on-missing-field = false`
    # makes every other column optional; this one cannot be.
    if values[ENTITY_KEY_INDEX] is None:
        raise ValueError(
            f"{ENTITY_KEY}: missing entity key — the row cannot become a feature "
            f"and would crash the sink at int(None)"
        )

    raw_ts = obj.get(ROWTIME_COLUMN)
    if raw_ts is None:
        # Unlike the other columns, a NULL rowtime is NOT survivable: the
        # watermark would never advance and the job would produce nothing while
        # reporting success. That exact failure is documented in ARCHITECTURE §4.
        raise ValueError(f"{ROWTIME_COLUMN} is missing; a null rowtime stalls the watermark")
    if not isinstance(raw_ts, str):
        raise TypeError(f"{ROWTIME_COLUMN}: expected an ISO-8601 string")
    # Python 3.11+ parses the trailing `Z`; the container runs 3.12.
    dt = datetime.fromisoformat(raw_ts)
    if dt.tzinfo is None:
        # The producer always sends an explicit Z. A naive timestamp here means
        # someone else is producing, and guessing its zone would silently shift
        # every event — assume UTC but do it visibly rather than by accident.
        dt = dt.replace(tzinfo=timezone.utc)
    values.append(int(dt.timestamp() * 1000))
    return values
