#!/usr/bin/env python3
"""Phase 10 job: card-testing (payment-attack) detector.

Flags customers whose number of *failed* payments inside a 60-second
event-time window exceeds a threshold (``> 10`` by default, matching
``config/phase1.json``: ``payment_attack_window_seconds`` /
``payment_attack_max_failures``), emitting ``CARD_TESTING_ATTACK`` (+30 risk
points). Card-testing is a burst of small failed attempts on one card.

Implementation notes:

- stateful per ``(customer_id, window)`` counting via ``groupBy`` + ``window()``
  over ``status == 'FAILED'`` payments only (successful attempts never count),
- event-time watermark + ``append`` output mode, so every finalized window
  emits its alert exactly once,
- per-window semantics: 12 failures split 6/6 across two windows do NOT
  trigger; 12 failures inside one window DO.

Usage:
    python spark/streaming/payment_attack.py --topic payments --threshold 10
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

CARD_TESTING_ALERT_TYPE = "CARD_TESTING_ATTACK"
CARD_TESTING_RISK_POINTS = 30  # config/phase1.json risk_points

DEFAULT_WINDOW = 60       # payment_attack_window_seconds
DEFAULT_THRESHOLD = 10    # payment_attack_max_failures -> flag when count > 10


def payment_attack_alert_stream(spark: SparkSession, topic: str,
                                window_seconds: int = DEFAULT_WINDOW,
                                threshold: int = DEFAULT_THRESHOLD,
                                watermark_seconds: int | None = None,
                                starting_offsets: str = "earliest") -> DataFrame:
    """Failed payments -> per-(customer, window) count; keep only count > threshold.

    Callers use ``append`` output mode: each finalized window produces its
    alert row exactly once.
    """
    watermark_seconds = watermark_seconds or window_seconds
    parsed = kafka_event_stream(spark, topic, starting_offsets,
                                event_type="payments")
    alerts = (
        parsed
        .filter(col("status") == "FAILED")
        .withWatermark("event_ts", f"{watermark_seconds} seconds")
        .groupBy(
            "customer_id",
            window(col("event_ts"), f"{window_seconds} seconds"),
        )
        .agg(
            count("payment_id").alias("failure_count"),
            _sum("amount").alias("amount_sum"),
        )
        .filter(col("failure_count") > threshold)
        .select(
            "customer_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "failure_count",
            "amount_sum",
            lit(CARD_TESTING_ALERT_TYPE).alias("alert_type"),
            lit(CARD_TESTING_RISK_POINTS).alias("risk_points"),
        )
    )
    return alerts


def run_payment_attack(topic: str, window_seconds: int = DEFAULT_WINDOW,
                       threshold: int = DEFAULT_THRESHOLD,
                       duration: int = 20, trigger: int = 5,
                       checkpoint: Path | None = None,
                       starting_offsets: str = "earliest",
                       memory_name: str | None = None,
                       validate=None) -> dict:
    """Run the card-testing detector for ``duration`` seconds (append mode).

    ``validate`` (optional) is called with the in-memory alert table before
    the Spark session stops; its return value is stored under ``validation``.
    """
    spark = get_spark(f"phase10-{topic}-w{window_seconds}-t{threshold}")
    alerts = payment_attack_alert_stream(
        spark, topic, window_seconds, threshold,
        starting_offsets=starting_offsets,
    )

    print(f"\nCard-testing alert schema "
          f"({window_seconds}s window, flag when failure_count > {threshold}):")
    alerts.printSchema()

    totals, table = run_console_and_memory(
        spark, alerts,
        label=f"phase10 card testing >{threshold} failed / {window_seconds}s "
              f"on '{topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase10"),
        output_mode="append",
        memory_name=memory_name,
    )
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "window_seconds": window_seconds,
        "threshold": threshold,
        "alert_type": CARD_TESTING_ALERT_TYPE,
        "topic": topic,
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 10: card-testing (payment-attack) detector "
                    "(>threshold failed payments in a window)")
    parser.add_argument("--topic", default="payments", choices=EVENT_SCHEMAS)
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW,
                        help="attack window in seconds")
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD,
                        help="flag when a customer exceeds this many failures/window")
    parser.add_argument("--watermark", type=int, default=None,
                        help="allowed lateness in seconds (default = window)")
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase10"))
    args = parser.parse_args()

    totals = run_payment_attack(
        topic=args.topic,
        window_seconds=args.window,
        threshold=args.threshold,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 10 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
