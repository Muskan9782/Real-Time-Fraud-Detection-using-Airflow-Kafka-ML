#!/usr/bin/env python3
"""Phase 8 job: stateful impossible-travel detector (Haversine speed).

Per customer, compare each transaction with the customer's *previous*
transaction (stateful, cross-event). Compute the great-circle distance
(Haversine) between the two locations and the implied travel speed; flag the
second transaction when the speed exceeds the plausible maximum
(``impossible_travel_speed_kmh`` = 800 km/h by default, +30 risk points).

Implementation notes:

- uses the Spark 4 ``StatefulProcessor`` API (``transformWithState``) with a
  per-key ``ValueState`` holding the previous ``(lat, lon, ts_ms)`` - the
  arbitrary-stateful successor of ``flatMapGroupsWithState``,
- event timestamps travel as epoch milliseconds (``unix_millis``) so no time
  zone conversion can interfere with the speed math,
- rows for a key are sorted by event time inside ``handleInputRows`` so the
  previous-event chain is deterministic within a micro-batch,
- ``update`` output mode: alerts are emitted as soon as the speed breach is
  seen (no watermark needed).

Usage:
    python spark/streaming/impossible_travel.py --topic transactions
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running as a script as well as ``python -m ...``.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, unix_millis
from pyspark.sql.streaming.stateful_processor import (
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark.common import REPO_ROOT, get_spark
from spark.schemas import EVENT_SCHEMAS
from spark.streaming.runner import run_console_and_memory
from spark.streaming.sources import kafka_event_stream

IMPOSSIBLE_TRAVEL_ALERT_TYPE = "IMPOSSIBLE_TRAVEL"
IMPOSSIBLE_TRAVEL_RISK_POINTS = 30  # config/phase1.json risk_points

DEFAULT_SPEED_KMH = 800.0  # impossible_travel_speed_kmh

EARTH_RADIUS_KM = 6371.0

# Per-key state: the previous transaction's location and event time.
STATE_SCHEMA = StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ts_ms", LongType()),
])

# Alert row produced by the processor (customer_id is added by the processor).
ALERT_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("prev_lat", DoubleType()),
    StructField("prev_lon", DoubleType()),
    StructField("cur_lat", DoubleType()),
    StructField("cur_lon", DoubleType()),
    StructField("distance_km", DoubleType()),
    StructField("time_gap_seconds", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("ts_ms", LongType()),
    StructField("alert_type", StringType()),
    StructField("risk_points", IntegerType()),
])


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points, in kilometres."""
    import math

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


class _ImpossibleTravelProcessor(StatefulProcessor):
    """Compares each transaction with the previous one for its customer."""

    def __init__(self, speed_kmh: float = DEFAULT_SPEED_KMH):
        self._speed_kmh = speed_kmh

    def init(self, handle: StatefulProcessorHandle) -> None:
        self._prev = handle.getValueState("prev_tx", STATE_SCHEMA)

    def handleInputRows(self, key, rows, timer_values: TimerValues):
        for row in sorted(rows, key=lambda r: r.ts_ms):
            prev = self._prev.get()
            if prev is not None:
                prev_lat, prev_lon, prev_ts_ms = prev
                gap_seconds = (row.ts_ms - prev_ts_ms) / 1000.0
                distance = haversine_km(prev_lat, prev_lon, row.lat, row.lon)
                speed = distance / (gap_seconds / 3600.0) if gap_seconds > 0 \
                    else float("inf")
                if speed > self._speed_kmh:
                    yield Row(
                        transaction_id=row.transaction_id,
                        customer_id=row.customer_id,
                        prev_lat=prev_lat,
                        prev_lon=prev_lon,
                        cur_lat=row.lat,
                        cur_lon=row.lon,
                        distance_km=distance,
                        time_gap_seconds=gap_seconds,
                        speed_kmh=speed,
                        ts_ms=row.ts_ms,
                        alert_type=IMPOSSIBLE_TRAVEL_ALERT_TYPE,
                        risk_points=IMPOSSIBLE_TRAVEL_RISK_POINTS,
                    )
            self._prev.update((row.lat, row.lon, row.ts_ms))


def impossible_travel_alert_stream(spark: SparkSession, topic: str,
                                   speed_kmh: float = DEFAULT_SPEED_KMH,
                                   starting_offsets: str = "earliest") -> DataFrame:
    """Transactions -> per-customer stateful speed check -> IMPOSSIBLE_TRAVEL
    alerts. Callers use ``update`` output mode."""
    parsed = kafka_event_stream(spark, topic, starting_offsets,
                                event_type="transactions")
    prepared = parsed.select(
        "transaction_id",
        "customer_id",
        "lat",
        "lon",
        unix_millis(col("event_ts")).alias("ts_ms"),
    )
    alerts = (
        prepared
        .groupBy("customer_id")
        .transformWithState(
            statefulProcessor=_ImpossibleTravelProcessor(speed_kmh),
            outputStructType=ALERT_SCHEMA,
            outputMode="update",
            timeMode="ProcessingTime",
        )
    )
    return alerts.withColumn("event_time", (col("ts_ms") / 1000.0).cast("timestamp"))


def run_impossible_travel(topic: str, speed_kmh: float = DEFAULT_SPEED_KMH,
                          duration: int = 20, trigger: int = 5,
                          checkpoint: Path | None = None,
                          starting_offsets: str = "earliest",
                          memory_name: str | None = None,
                          validate=None) -> dict:
    """Run the impossible-travel detector for ``duration`` seconds (update mode).

    ``validate`` (optional) is called with the in-memory alert table before
    the Spark session stops; its return value is stored under ``validation``.
    """
    spark = get_spark(f"phase8-{topic}-v{speed_kmh}")
    alerts = impossible_travel_alert_stream(
        spark, topic, speed_kmh, starting_offsets=starting_offsets,
    )

    print(f"\nImpossible-travel alert schema "
          f"(speed > {speed_kmh} km/h between consecutive transactions):")
    alerts.printSchema()

    totals, table = run_console_and_memory(
        spark, alerts,
        label=f"phase8 impossible travel >{speed_kmh} km/h on '{topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase8"),
        output_mode="update",
        memory_name=memory_name,
    )
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "speed_kmh": speed_kmh,
        "alert_type": IMPOSSIBLE_TRAVEL_ALERT_TYPE,
        "topic": topic,
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 8: stateful impossible-travel detector "
                    "(Haversine speed > max)")
    parser.add_argument("--topic", default="transactions", choices=EVENT_SCHEMAS)
    parser.add_argument("--speed", type=float, default=DEFAULT_SPEED_KMH,
                        help="max plausible travel speed in km/h")
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase8"))
    args = parser.parse_args()

    totals = run_impossible_travel(
        topic=args.topic,
        speed_kmh=args.speed,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 8 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
