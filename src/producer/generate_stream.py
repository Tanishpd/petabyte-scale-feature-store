#!/usr/bin/env python3
"""High-velocity mock transaction stream -> Kafka.

Deliberately not `while True: send(random())`. Four properties exist because
the downstream pipeline is only interesting if the data has real structure:

1. Zipf-distributed users      -> genuine hot keys, so Flink's keyed state is
                                  skewed the way production is.
2. Per-category amount models  -> a coffee is $5 and a flight is $400, so the
                                  rolling average carries information and the
                                  behaviour sentence embeds meaningfully.
3. Injected anomalies          -> the rolling average visibly moves.
4. Out-of-order events         -> watermarks get exercised instead of assumed.
                                  These are sent NOW carrying an OLDER
                                  timestamp; that is what makes event time
                                  differ from processing time.

Runs on the host venv (3.11). Invariant 4: the host talks to Kafka on
localhost:29092, never kafka:9092 — the latter hangs on metadata fetch rather
than erroring.

    python src/producer/generate_stream.py --eps 5000
    python src/producer/generate_stream.py --eps 20000 --duration 60
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from contracts.events import (  # noqa: E402
    COUNTRIES,
    CURRENCIES,
    DEVICES,
    MERCHANT_CATEGORIES,
    TOPIC,
)

# Per-category lognormal parameters for the amount, and a plausible share of
# traffic. mu/sigma are in log-space: median = exp(mu).
CATEGORY_MODEL: dict[str, tuple[float, float, float]] = {
    # category      mu     sigma  weight
    "coffee_shop":  (1.55, 0.35, 0.16),
    "grocery":      (3.90, 0.55, 0.15),
    "rideshare":    (2.85, 0.50, 0.13),
    "restaurant":   (3.45, 0.60, 0.12),
    "streaming":    (2.35, 0.20, 0.08),
    "pharmacy":     (3.05, 0.65, 0.07),
    "electronics":  (5.10, 0.80, 0.06),
    "fuel":         (3.80, 0.40, 0.08),
    "airline":      (5.95, 0.55, 0.04),
    "hotel":        (5.55, 0.60, 0.04),
    "gym":          (3.70, 0.25, 0.04),
    "bookstore":    (2.95, 0.55, 0.03),
}

# Rough FX rates vs USD. Only used to make local amounts plausible — a coffee
# should read as R$15 or ₹350, not R$4 or ₹4.
FX_VS_USD: dict[str, float] = {
    "US": 1.0, "GB": 0.79, "IN": 83.0, "DE": 0.92,
    "CA": 1.36, "AU": 1.52, "SG": 1.34, "BR": 5.40,
}

MERCHANT_NAMES: dict[str, tuple[str, ...]] = {
    "coffee_shop": ("Blue Bottle Coffee", "Stumptown", "Verve Coffee", "Grind House"),
    "grocery":     ("Whole Foods", "Trader Joe's", "Safeway", "Lidl", "Big Basket"),
    "rideshare":   ("Uber", "Lyft", "Ola", "Bolt"),
    "restaurant":  ("Nobu", "Chipotle", "Dishoom", "Sweetgreen", "Pho 24"),
    "streaming":   ("Netflix", "Spotify", "Disney+", "Max"),
    "pharmacy":    ("CVS", "Walgreens", "Boots", "Apollo Pharmacy"),
    "electronics": ("Apple Store", "Best Buy", "Currys", "Croma"),
    "fuel":        ("Shell", "BP", "Chevron", "Indian Oil"),
    "airline":     ("United", "Delta", "British Airways", "Lufthansa", "IndiGo"),
    "hotel":       ("Marriott", "Hilton", "Premier Inn", "Taj Hotels"),
    "gym":         ("Equinox", "Planet Fitness", "PureGym", "Cult.fit"),
    "bookstore":   ("Barnes & Noble", "Waterstones", "Powell's", "Crossword"),
}


@dataclass
class Stats:
    sent: int = 0
    bytes_sent: int = 0
    anomalies: int = 0
    late: int = 0
    errors: int = 0
    started: float = field(default_factory=time.perf_counter)

    def rate(self) -> float:
        elapsed = time.perf_counter() - self.started
        return self.sent / elapsed if elapsed > 0 else 0.0


class EventGenerator:
    """Vectorised event generation. Kept free of Kafka so it can be unit-tested."""

    def __init__(
        self,
        n_users: int = 50_000,
        zipf_s: float = 1.1,
        anomaly_rate: float = 0.005,
        anomaly_mult: tuple[float, float] = (10.0, 30.0),
        late_rate: float = 0.08,
        max_lateness_s: float = 30.0,
        seed: int | None = None,
    ) -> None:
        self.rng = np.random.default_rng(seed)
        self.n_users = n_users
        self.anomaly_rate = anomaly_rate
        self.anomaly_mult = anomaly_mult
        self.late_rate = late_rate
        self.max_lateness_s = max_lateness_s

        # Zipf over a FIXED user population. np.random.zipf is unbounded and
        # would invent user ids; a normalised rank CDF + searchsorted keeps the
        # population closed and is ~10x faster per draw.
        ranks = np.arange(1, n_users + 1, dtype=np.float64)
        weights = 1.0 / np.power(ranks, zipf_s)
        self._user_cdf = np.cumsum(weights / weights.sum())

        cats = list(CATEGORY_MODEL)
        self._cats = np.array(cats, dtype=object)
        cat_w = np.array([CATEGORY_MODEL[c][2] for c in cats], dtype=np.float64)
        self._cat_p = cat_w / cat_w.sum()
        self._cat_mu = np.array([CATEGORY_MODEL[c][0] for c in cats])
        self._cat_sigma = np.array([CATEGORY_MODEL[c][1] for c in cats])

        # Flat merchant table so a merchant_id maps to one (name, category).
        self._m_name: list[str] = []
        self._m_cat_idx: list[int] = []
        self._cat_to_merchants: dict[int, list[int]] = {}
        for ci, c in enumerate(cats):
            for nm in MERCHANT_NAMES[c]:
                self._cat_to_merchants.setdefault(ci, []).append(len(self._m_name))
                self._m_name.append(nm)
                self._m_cat_idx.append(ci)
        self._cat_merchant_arrays = {
            ci: np.array(ids) for ci, ids in self._cat_to_merchants.items()
        }

        self._countries = np.array(COUNTRIES, dtype=object)
        # Skewed country mix — uniform would be unrealistic.
        cw = np.array([0.42, 0.12, 0.16, 0.08, 0.06, 0.05, 0.05, 0.06])
        self._country_p = cw / cw.sum()

        # A user has ONE home market, assigned once. Drawing country per event
        # would make a single user transact in USD then BRL then SGD, and
        # txn_amount_avg_10m — the headline feature — would be an average over
        # mixed currencies, which means nothing to a model.
        self._user_country = self.rng.choice(
            len(self._countries), size=n_users + 1, p=self._country_p
        )
        # Rough FX vs USD, so a coffee is ~R$15 rather than R$4. Amounts are in
        # the user's local currency; per-user aggregates are therefore coherent.
        # (A production feature would also carry a USD-normalised amount so
        # cross-user comparisons work; out of scope here.)
        self._fx = np.array([FX_VS_USD[c] for c in COUNTRIES])
        self._devices = np.array(DEVICES, dtype=object)
        self._device_p = np.array([0.38, 0.36, 0.18, 0.08])

        self._seq = 0
        self._ts_cache_sec = -1
        self._ts_cache_str = ""

    def _iso_ms(self, epoch: float) -> str:
        """ISO-8601 with milliseconds. datetime.isoformat() is ~5x slower and
        this is the hottest string operation in the loop, so the second-level
        prefix is cached across the many events sharing a second."""
        secs = int(epoch)
        if secs != self._ts_cache_sec:
            self._ts_cache_sec = secs
            self._ts_cache_str = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(secs))
        return f"{self._ts_cache_str}.{int((epoch - secs) * 1000):03d}Z"

    def batch(self, n: int, now: float | None = None) -> tuple[list[dict], int, int]:
        """Return (events, n_anomalies, n_late)."""
        now = time.time() if now is None else now
        rng = self.rng

        users = np.searchsorted(self._user_cdf, rng.random(n)) + 1
        cat_idx = rng.choice(len(self._cats), size=n, p=self._cat_p)
        amounts = rng.lognormal(self._cat_mu[cat_idx], self._cat_sigma[cat_idx])

        is_anom = rng.random(n) < self.anomaly_rate
        amounts = np.where(
            is_anom,
            amounts * rng.uniform(*self.anomaly_mult, size=n),
            amounts,
        )

        is_late = rng.random(n) < self.late_rate
        lateness = np.where(is_late, rng.uniform(0.0, self.max_lateness_s, size=n), 0.0)

        # Country follows the user, not the event (see __init__).
        country_idx = self._user_country[users]
        amounts = amounts * self._fx[country_idx]

        device_idx = rng.choice(len(self._devices), size=n, p=self._device_p)
        card_present = rng.random(n) < 0.35

        merchants = np.empty(n, dtype=np.int64)
        for ci in np.unique(cat_idx):
            mask = cat_idx == ci
            pool = self._cat_merchant_arrays[int(ci)]
            merchants[mask] = rng.choice(pool, size=int(mask.sum()))

        out: list[dict] = []
        for i in range(n):
            self._seq += 1
            mid = int(merchants[i])
            country = self._countries[country_idx[i]]
            out.append(
                {
                    "event_id": f"{int(now * 1000):013d}-{self._seq:09d}",
                    "user_id": int(users[i]),
                    "merchant_id": mid,
                    "merchant_name": self._m_name[mid],
                    "merchant_category": str(self._cats[self._m_cat_idx[mid]]),
                    "amount": round(float(amounts[i]), 2),
                    "currency": CURRENCIES[country],
                    "country": str(country),
                    "device": str(self._devices[device_idx[i]]),
                    "is_card_present": bool(card_present[i]),
                    # Late events are sent now but timestamped in the past.
                    "event_timestamp": self._iso_ms(now - float(lateness[i])),
                }
            )
        return out, int(is_anom.sum()), int(is_late.sum())


def build_producer(bootstrap: str, acks: str, compression: str | None):
    from kafka import KafkaProducer

    # SASL only when the environment asks for it: the dev stack is PLAINTEXT and
    # the production topology sets KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT.
    security = {}
    if os.environ.get("KAFKA_SECURITY_PROTOCOL"):
        security = dict(
            security_protocol=os.environ["KAFKA_SECURITY_PROTOCOL"],
            sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_plain_username=os.environ.get("KAFKA_USER", ""),
            sasl_plain_password=os.environ.get("KAFKA_PASSWORD", ""),
        )

    return KafkaProducer(
        **security,
        bootstrap_servers=bootstrap,
        acks=int(acks) if acks.isdigit() else acks,
        linger_ms=20,
        batch_size=256 * 1024,
        compression_type=compression,
        max_block_ms=15_000,
        request_timeout_ms=15_000,
        # value/key are pre-encoded in the loop; a serializer here would add a
        # per-record Python call for no benefit.
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:29092",
                    help="Invariant 4: host uses localhost:29092, not kafka:9092")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--eps", type=float, default=5000, help="target events/sec")
    ap.add_argument("--duration", type=float, default=0, help="seconds; 0 = until Ctrl-C")
    ap.add_argument("--users", type=int, default=50_000)
    ap.add_argument("--zipf-s", type=float, default=1.1, help="higher = hotter keys")
    ap.add_argument("--anomaly-rate", type=float, default=0.005)
    ap.add_argument("--late-rate", type=float, default=0.08)
    ap.add_argument("--max-lateness", type=float, default=30.0,
                    help="must not exceed the Flink watermark delay")
    ap.add_argument("--acks", default="1", choices=["0", "1", "all"])
    ap.add_argument("--compression", default=None,
                    choices=[None, "gzip", "snappy", "lz4", "zstd"])
    ap.add_argument("--report-interval", type=float, default=5.0)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="generate and print a few events without touching Kafka")
    ap.add_argument("--backfill", type=float, default=0, metavar="SECONDS",
                    help="emit a historical span of this many seconds, sorted by "
                         "event time, as fast as possible, then exit. Sorting is "
                         "required: out-of-order history would advance the "
                         "watermark past earlier events and Flink would drop them.")
    args = ap.parse_args()

    gen = EventGenerator(
        n_users=args.users, zipf_s=args.zipf_s, anomaly_rate=args.anomaly_rate,
        late_rate=args.late_rate, max_lateness_s=args.max_lateness, seed=args.seed,
    )

    if args.dry_run:
        events, _, _ = gen.batch(3)
        for e in events:
            print(json.dumps(e))
        return 0

    producer = build_producer(args.bootstrap, args.acks, args.compression)
    stats = Stats()
    running = True

    if args.backfill:
        n = int(args.backfill * args.eps)
        print(f"backfilling {n:,} events over {args.backfill:.0f}s of history "
              f"-> {args.bootstrap} topic={args.topic}")
        now = time.time()
        events, n_anom, n_late = gen.batch(n, now=now)
        # Re-stamp across the span, then sort ascending so the watermark only
        # ever moves forward and nothing is dropped as late.
        span = np.linspace(now - args.backfill, now, num=n)
        for ev, ts in zip(events, span):
            ev["event_timestamp"] = gen._iso_ms(ts)
        events.sort(key=lambda e: e["event_timestamp"])
        for e in events:
            payload = json.dumps(e, separators=(",", ":")).encode()
            producer.send(args.topic, key=str(e["user_id"]).encode(), value=payload)
            stats.bytes_sent += len(payload)
        stats.sent = len(events)
        stats.anomalies, stats.late = n_anom, n_late
        producer.flush(timeout=60)
        producer.close(timeout=10)
        print(f"backfilled {stats.sent:,} events, "
              f"{stats.bytes_sent / (1 << 20):.1f} MiB, "
              f"span {events[0]['event_timestamp']} .. {events[-1]['event_timestamp']}")
        return 0

    def stop(_sig, _frm):
        nonlocal running
        running = False
        print("\ninterrupted — flushing...", file=sys.stderr)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # ~50 batches/sec keeps pacing granularity near 20 ms.
    batch_n = max(1, int(args.eps // 50))
    next_report = time.perf_counter() + args.report_interval

    print(f"producing -> {args.bootstrap} topic={args.topic} "
          f"target={args.eps:,.0f} eps batch={batch_n} users={args.users:,}")

    while running:
        events, n_anom, n_late = gen.batch(batch_n)
        for e in events:
            payload = json.dumps(e, separators=(",", ":")).encode()
            try:
                producer.send(args.topic, key=str(e["user_id"]).encode(), value=payload)
                stats.bytes_sent += len(payload)
            except Exception:
                stats.errors += 1
        stats.sent += len(events)
        stats.anomalies += n_anom
        stats.late += n_late

        # Closed-loop pacing: correct against cumulative drift, not per-batch.
        elapsed = time.perf_counter() - stats.started
        target_elapsed = stats.sent / args.eps
        if target_elapsed > elapsed:
            time.sleep(target_elapsed - elapsed)

        now = time.perf_counter()
        if now >= next_report:
            next_report = now + args.report_interval
            achieved = stats.rate()
            pct = 100.0 * achieved / args.eps if args.eps else 0.0
            print(f"  sent={stats.sent:>10,}  {achieved:>9,.0f} eps "
                  f"({pct:5.1f}% of target)  {stats.bytes_sent / (1 << 20):>7.1f} MiB  "
                  f"anomalies={stats.anomalies:,}  late={stats.late:,}"
                  + (f"  errors={stats.errors:,}" if stats.errors else ""))

        if args.duration and elapsed >= args.duration:
            break

    producer.flush(timeout=30)
    producer.close(timeout=10)

    achieved = stats.rate()
    avg_size = stats.bytes_sent / stats.sent if stats.sent else 0
    print(f"\n--- final ---")
    print(f"sent          : {stats.sent:,} events in {time.perf_counter() - stats.started:.1f}s")
    print(f"achieved rate : {achieved:,.0f} eps  (target {args.eps:,.0f}, "
          f"{100.0 * achieved / args.eps:.1f}%)")
    print(f"volume        : {stats.bytes_sent / (1 << 20):.1f} MiB, avg {avg_size:.0f} B/event")
    print(f"anomalies     : {stats.anomalies:,} ({100.0 * stats.anomalies / max(stats.sent,1):.2f}%)")
    print(f"late events   : {stats.late:,} ({100.0 * stats.late / max(stats.sent,1):.2f}%)")
    if stats.errors:
        print(f"errors        : {stats.errors:,}")
    return 1 if stats.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
