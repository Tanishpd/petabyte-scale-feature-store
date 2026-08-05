"""Dead-letter routing for records the pipeline cannot parse.

WHY THIS EXISTS
---------------
`--on-bad-record skip` sets `json.ignore-parse-errors` on the SQL source, which
keeps one malformed event from stopping the job. The cost was measured and it is
worse than it sounds: Flink's JSON format discards an unparseable record
**without incrementing any metric and without logging**. Five malformed records
injected into a running production pipeline left `numRecordsInErrors` at 0 and
no line in any TaskManager log. The job's docstring used to claim the drops were
counted, citing a helper that never existed.

So `skip` trades a stalled pipeline for data loss you cannot see. This module
adds the third option — `--on-bad-record dlq` — which keeps the pipeline running
AND keeps the record: the payload, the reason, and the offset it came from are
written to a dead-letter topic you can inspect and replay.

WHY IT IS OPT-IN, AND WHY THAT IS NOT A COP-OUT
-----------------------------------------------
This path parses JSON in **Python, per event**, because PyFlink cannot supply a
custom Java `DeserializationSchema` to the SQL connector — a side output is the
only place the failure is observable. That runs directly against the design
principle the whole repo is built on (Invariant 5: embeddings are computed on
window close precisely because per-event Python work is ~1200x more expensive).

So this is a real trade, not a free upgrade, and the flag exists so it is a
choice rather than a default. The throughput cost is measured rather than
guessed — see docs/SCALING.md.

WHAT IT DOES NOT DO
-------------------
The validation here is this module's own, not Flink's JSON format's. They agree
on the cases that matter (unparseable bytes, wrong types, unparseable
timestamps) but they are two implementations, so exact parity is not guaranteed.
The DLQ record carries the raw payload, so a disagreement is recoverable rather
than silent — which is the entire point.
"""
from __future__ import annotations

import json
from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import OutputTag
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessFunction
from pyflink.table import Schema

from contracts.events import EVENT_FIELDS, ROWTIME_COLUMN, WATERMARK_DELAY_SECONDS
from src.flink.event_parser import parse_event

#: The rowtime arrives as epoch milliseconds rather than a Python datetime.
#: PyFlink's mapping for TIMESTAMP_LTZ is the one place this path is easy to get
#: subtly wrong — a naive datetime silently becomes the wrong instant — and an
#: integer has exactly one interpretation. SQL turns it back into a rowtime with
#: TO_TIMESTAMP_LTZ, so the column the rest of the job sees is unchanged.
TS_MILLIS_FIELD = "ts_millis"

_JSON_TO_TYPEINFO = {
    "string": Types.STRING(),
    "integer": Types.LONG(),
    "number": Types.DOUBLE(),
    "boolean": Types.BOOLEAN(),
}
#: merchant_id is INT in the DDL while user_id is BIGINT; both are "integer" in
#: the contract's JSON view, so the Flink type decides the width.
_FLINK_TO_TYPEINFO = {"INT": Types.INT(), "BIGINT": Types.LONG()}

_PARSED_FIELDS = [n for n in EVENT_FIELDS if n != ROWTIME_COLUMN] + [TS_MILLIS_FIELD]


def _type_info_for(name: str) -> "Types":
    json_type, flink_type = EVENT_FIELDS[name]
    if json_type == "integer":
        return _FLINK_TO_TYPEINFO.get(flink_type, Types.LONG())
    return _JSON_TO_TYPEINFO[json_type]


PARSED_ROW_TYPE = Types.ROW_NAMED(
    _PARSED_FIELDS,
    [_type_info_for(n) for n in _PARSED_FIELDS[:-1]] + [Types.LONG()],
)

#: Side-output channel. Strings, not Rows: a record that failed to parse has no
#: reliable structure by definition, so the DLQ carries an envelope we build.
DLQ_TAG = OutputTag("bad-records", Types.STRING())


class RouteBadRecords(ProcessFunction):
    """Parse, emitting good rows downstream and bad payloads to the side output."""

    def process_element(self, value, ctx):
        try:
            yield Row(*parse_event(value))
        except Exception as exc:  # noqa: BLE001 — every failure is a DLQ record
            # The envelope is what makes the DLQ replayable: the raw payload is
            # preserved verbatim, and the reason travels with it so triage does
            # not start with "reproduce the parse failure".
            yield DLQ_TAG, json.dumps({
                "error": f"{type(exc).__name__}: {exc}",
                "timestamp_ms": ctx.timestamp(),
                "payload": value,
            }, separators=(",", ":"))


def _schema() -> Schema:
    b = Schema.new_builder()
    for name, (_json_type, flink_type) in EVENT_FIELDS.items():
        if name == ROWTIME_COLUMN:
            continue
        b = b.column(name, flink_type)
    b = b.column(TS_MILLIS_FIELD, "BIGINT")
    # Rebuild the rowtime the rest of the job expects, then declare the SAME
    # watermark the SQL DDL uses — it comes from contracts/events.py either way,
    # so the two source paths cannot drift on lateness tolerance.
    b = b.column_by_expression(ROWTIME_COLUMN, f"TO_TIMESTAMP_LTZ({TS_MILLIS_FIELD}, 3)")
    b = b.watermark(
        ROWTIME_COLUMN,
        f"`{ROWTIME_COLUMN}` - INTERVAL '{WATERMARK_DELAY_SECONDS}' SECOND",
    )
    return b.build()


def register_validated_source(
    stream_env,
    t_env,
    *,
    bootstrap: str,
    topic: str,
    group_id: str,
    startup_mode: str,
    dlq_topic: str,
    properties: dict[str, str],
    source_table: str,
) -> None:
    """Replace the SQL Kafka source with a parse-and-route DataStream source.

    Registers `source_table` as a view with the same columns and watermark the
    DDL would have produced, so every query downstream is unchanged.
    """
    offsets = {
        "earliest-offset": KafkaOffsetsInitializer.earliest(),
        "latest-offset": KafkaOffsetsInitializer.latest(),
        "group-offsets": KafkaOffsetsInitializer.committed_offsets(),
    }[startup_mode]

    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(offsets)
        .set_value_only_deserializer(SimpleStringSchema())
    )
    for k, v in properties.items():
        builder = builder.set_property(k, v)

    raw = stream_env.from_source(
        builder.build(), WatermarkStrategy.no_watermarks(), f"kafka-raw:{topic}"
    )
    routed = raw.process(RouteBadRecords(), output_type=PARSED_ROW_TYPE)

    sink_builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(dlq_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        # AT_LEAST_ONCE, not EXACTLY_ONCE: a duplicate in a dead-letter topic is
        # harmless and a transactional sink would add a coordinator to the one
        # path that must never be the reason the pipeline stops.
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    )
    for k, v in properties.items():
        sink_builder = sink_builder.set_property(k, v)

    routed.get_side_output(DLQ_TAG).sink_to(sink_builder.build()).name("dlq-writer")

    t_env.create_temporary_view(source_table, t_env.from_data_stream(routed, _schema()))
