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
    # TIMESTAMP_LTZ(3), not TIMESTAMP(3). The producer emits a UTC instant with
    # an explicit `Z` (`2026-08-03T06:30:13.695Z`). TIMESTAMP(3) is a timestamp
    # WITHOUT time zone, and Flink's ISO-8601 JSON parser rejects the trailing
    # `Z` for it — silently, yielding NULL rowtimes, a watermark that never
    # advances, and a job that produces zero rows without erroring. Measured:
    # every one of 48,000 events parsed to NaT before this was corrected.
    "event_timestamp":   ("string",  "TIMESTAMP_LTZ(3)"),
}

#: The rowtime column. ONE name end to end: Flink's JSON format matches columns
#: to JSON keys by name and offers no field-mapping option, so renaming would
#: cost a computed column and buy nothing. `event_timestamp` is also Feast's
#: conventional name for the same concept.
ROWTIME_WIRE_FIELD = "event_timestamp"
ROWTIME_COLUMN = ROWTIME_WIRE_FIELD

#: Producer emits `2026-08-01T12:00:00.123Z`. Flink's JSON format only parses
#: that when told to — the default is the SQL standard (`... 12:00:00.123`),
#: which would silently yield NULL rowtimes and a job that never fires a window.
JSON_TIMESTAMP_FORMAT = "ISO-8601"

#: Matches the producer's injected lateness (see generate_stream.py --max-lateness).
WATERMARK_DELAY_SECONDS = 30

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
    """Column list for the Flink Kafka source DDL, minus the watermark clause."""
    return ",\n".join(
        f"  `{name}` {flink_type}"
        for name, (_json_type, flink_type) in EVENT_FIELDS.items()
    )


def watermark_clause() -> str:
    """Event-time watermark. The delay must be >= the producer's injected
    lateness, or Flink drops events the producer deliberately delayed."""
    return (
        f"  WATERMARK FOR `{ROWTIME_COLUMN}` AS "
        f"`{ROWTIME_COLUMN}` - INTERVAL '{WATERMARK_DELAY_SECONDS}' SECOND"
    )
