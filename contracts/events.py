"""Single source of truth for the Kafka event schema.

The producer writes these field names; the Flink Kafka source DDL reads them.
Two independent string literals that must stay byte-identical, in two different
processes, is the same class of latent bug that `contracts/schema.py` prevents
on the feature side — so the event shape lives here and both sides import it.

Nothing in this module may import kafka, pyflink, or feast: it is shared by the
host venv (3.11) and the Flink container (3.12), which have disjoint deps.
"""

from __future__ import annotations

TOPIC = "transactions"

# Wire field -> (JSON type, Flink SQL type). Order is the DDL column order.
EVENT_FIELDS: dict[str, tuple[str, str]] = {
    "event_id":          ("string",  "STRING"),
    "user_id":           ("integer", "BIGINT"),
    "merchant_id":       ("integer", "INT"),
    "merchant_name":     ("string",  "STRING"),
    "merchant_category": ("string",  "STRING"),
    "amount":            ("number",  "DOUBLE"),
    "currency":          ("string",  "STRING"),
    "country":           ("string",  "STRING"),
    "device":            ("string",  "STRING"),
    "is_card_present":   ("boolean", "BOOLEAN"),
    "event_timestamp":   ("string",  "TIMESTAMP(3)"),
}

#: The rowtime column. ARCHITECTURE.md §4: the wire field is `event_timestamp`,
#: and the DDL exposes it as `event_time` so prose and SQL agree on one name.
ROWTIME_WIRE_FIELD = "event_timestamp"
ROWTIME_COLUMN = "event_time"

#: Producer emits `2026-08-01T12:00:00.123Z`. Flink's JSON format only parses
#: that when told to — the default is the SQL standard (`... 12:00:00.123`),
#: which would silently yield NULL rowtimes and a job that never fires a window.
JSON_TIMESTAMP_FORMAT = "ISO-8601"

#: Matches the producer's injected lateness (see generate_stream.py --max-lateness).
WATERMARK_DELAY = "30' SECOND"

MERCHANT_CATEGORIES: tuple[str, ...] = (
    "coffee_shop", "grocery", "rideshare", "restaurant", "streaming",
    "pharmacy", "electronics", "fuel", "airline", "hotel", "gym", "bookstore",
)

DEVICES: tuple[str, ...] = ("ios", "android", "web", "pos")
COUNTRIES: tuple[str, ...] = ("US", "GB", "IN", "DE", "CA", "AU", "SG", "BR")
CURRENCIES: dict[str, str] = {
    "US": "USD", "GB": "GBP", "IN": "INR", "DE": "EUR",
    "CA": "CAD", "AU": "AUD", "SG": "SGD", "BR": "BRL",
}


def source_ddl_columns() -> str:
    """Column list for the Flink Kafka source DDL, minus the watermark clause.

    The rowtime column is renamed from the wire field, so Phase 4 does not have
    to remember which of the two names belongs where.
    """
    lines = []
    for name, (_json_type, flink_type) in EVENT_FIELDS.items():
        if name == ROWTIME_WIRE_FIELD:
            lines.append(f"  `{ROWTIME_COLUMN}` {flink_type}")
        else:
            lines.append(f"  `{name}` {flink_type}")
    return ",\n".join(lines)


def json_field_mapping() -> dict[str, str]:
    """DDL column -> wire field, for the columns where the two differ."""
    return {ROWTIME_COLUMN: ROWTIME_WIRE_FIELD}
