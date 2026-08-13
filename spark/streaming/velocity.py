#!/usr/bin/env python3
"""Phase 7 job: high-transaction-velocity detector.

Flags customers whose transaction count inside a 2-minute event-time window
exceeds a threshold (``> 5`` by default, matching ``config/phase1.json``:
``velocity_window_seconds`` / ``velocity_max_transactions``).

Implementation notes:

- stateful per ``(customer_id, window)`` counting via ``groupBy`` + ``window()``,
- event-time watermark + ``append`` output mode, so every finalized window
  emits its alert exactly once (no duplicate alarms as the count grows),
- per-window semantics: 6 transactions split 3/3 across two windows do NOT
  trigger; 6 transactions inside one window DO.

Usage:
    python spark/streaming/velocity.py --topic transactions --threshold 5
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running as a script as well as ``python -m ...``.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, sum as _sum, window

from spark.common import REPO_ROOT, get_spark
from spark.schemas import EVENT_SCHEMAS
from spark.streaming.runner import run_console_and_memory
from spark.streaming.sources import kafka_event_stream

VELOCITY_ALERT_TYPE = "HIGH_TRANSACTION_VELOCITY"
VELOCITY_RISK_POINTS = 25  # config/phase1.json risk_points

DEFAULT_WINDOW = 120      # velocity_window_seconds
DEFAULT_THRESHOLD = 5     # velocity_max_transactions -> flag when count > 5


def velocity_alert_stream(spark: SparkSession, topic: str,
                          window_seconds: int = DEFAULT_WINDOW,
                          threshold: int = DEFAULT_THRESHOLD,
                          watermark_seconds: int | None = None,
                          starting_offsets: str = "earliest") -> DataFrame:
    """Transactions -> per-(customer, window) count; keep only count > threshold.

    Callers use ``append`` output mode: each finalized window produces its
    alert row exactly once.
    """
    watermark_seconds = watermark_seconds or window_seconds
    parsed = kafka_event_stream(spark, topic, starting_offsets,
                                event_type="transactions")
    alerts = (
        parsed
        .withWatermark("event_ts", f"{watermark_seconds} seconds")
        .groupBy(
            "customer_id",
            window(col("event_ts"), f"{window_seconds} seconds"),
        )
        .agg(
            count("transaction_id").alias("tx_count"),
            _sum("amount").alias("amount_sum"),
        )
        .filter(col("tx_count") > threshold)
        .select(
            "customer_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "tx_count",
            "amount_sum",
            lit(VELOCITY_ALERT_TYPE).alias("alert_type"),
            lit(VELOCITY_RISK_POINTS).alias("risk_points"),
        )
    )
    return alerts


def run_velocity(topic: str, window_seconds: int = DEFAULT_WINDOW,
                 threshold: int = DEFAULT_THRESHOLD,
                 duration: int = 20, trigger: int = 5,
                 checkpoint: Path | None = None,
                 starting_offsets: str = "earliest",
                 memory_name: str | None = None,
                 validate=None) -> dict:
    """Run the velocity detector for ``duration`` seconds (append mode).

    ``validate`` (optional) is called with the in-memory alert table before
    the Spark session stops; its return value is stored under ``validation``.
    """
    spark = get_spark(f"phase7-{topic}-w{window_seconds}-t{threshold}")
    alerts = velocity_alert_stream(
        spark, topic, window_seconds, threshold,
        starting_offsets=starting_offsets,
    )

    print(f"\nVelocity alert schema "
          f"({window_seconds}s window, flag when tx_count > {threshold}):")
    alerts.printSchema()

    totals, table = run_console_and_memory(
        spark, alerts,
        label=f"phase7 velocity >{threshold} tx / {window_seconds}s on '{topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase7"),
        output_mode="append",
        memory_name=memory_name,
    )
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "window_seconds": window_seconds,
        "threshold": threshold,
        "alert_type": VELOCITY_ALERT_TYPE,
        "topic": topic,
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 7: high-transaction-velocity detector "
                    "(>threshold tx in a window)")
    parser.add_argument("--topic", default="transactions", choices=EVENT_SCHEMAS)
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW,
                        help="velocity window in seconds")
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD,
                        help="flag when a customer exceeds this many tx/window")
    parser.add_argument("--watermark", type=int, default=None,
                        help="allowed lateness in seconds (default = window)")
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase7"))
    parser.add_argument("--write-kafka", action="store_true",
                        help="also write alerts to the 'fraud_alerts' topic")
    args = parser.parse_args()

    totals = run_velocity(
        topic=args.topic,
        window_seconds=args.window,
        threshold=args.threshold,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 7 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
