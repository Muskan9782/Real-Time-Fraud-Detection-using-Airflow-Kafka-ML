#!/usr/bin/env python3
"""Phase 12 job: risk engine (detector alerts -> fraud_alerts risk levels).

The five detectors (Phases 7-11) each emit alerts carrying an ``alert_type``
and ``risk_points``. This job is the downstream *risk engine*: it combines the
alerts for the same customer inside a window into one risk score and
classifies the customer's risk into a level, producing the ``fraud_alerts``
output.

Implementation notes:

- reads unified alert envelopes (``ALERTS_SCHEMA``) from a Kafka topic, so any
  detector - current or future - can feed the engine by emitting the same
  envelope; the output row is the ``fraud_alerts`` payload,
- event-time tumbling window (``risk_window_seconds`` = 5 min by default) with
  a matching watermark and ``append`` output mode: a (customer, window) risk
  record is emitted exactly once, when the watermark passes the window end,
- ``total_points`` = sum of the window's alert points; ``alert_types`` is the
  sorted, deduplicated set of detectors that fired for the customer (a
  customer hit by several detectors is scored on the *combination*, not on
  each alert in isolation),
- risk-level bands (``config/phase1.json`` ``risk_levels``):

      CRITICAL >= 76     HIGH >= 51     MEDIUM >= 26     else LOW

  With per-alert points 20/25/30 this means: a single 25-point alert is LOW, a
  single 30-point alert is MEDIUM, two strong alerts (>= 51 points) are HIGH,
  and three overlapping detectors push a customer to CRITICAL.

Usage:
    python spark/streaming/risk_engine.py --topic fraud_alerts --window 300
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running as a script as well as ``python -m ...``.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    array_join,
    array_sort,
    col,
    collect_set,
    count,
    lit,
    sum as _sum,
    when,
    window,
)

from spark.common import REPO_ROOT, get_spark
from spark.schemas import EVENT_SCHEMAS
from spark.streaming.runner import run_console_and_memory
from spark.streaming.sources import kafka_event_stream

RISK_WINDOW_SECONDS = 300  # risk_window_seconds - combine alerts within 5 min

# (level, min_points), highest band first. config/phase1.json `risk_levels`.
RISK_LEVELS: list[tuple[str, int]] = [
    ("CRITICAL", 76),
    ("HIGH", 51),
    ("MEDIUM", 26),
    ("LOW", 0),
]


def risk_level_expr(total_points: Column) -> Column:
    """Column expression mapping ``total_points`` to its risk level band."""
    expr: Column = lit(RISK_LEVELS[-1][0])  # lowest band, the fallback
    for level, minimum in RISK_LEVELS[-2::-1]:  # ascending, excluding LOW
        expr = when(total_points >= minimum, lit(level)).otherwise(expr)
    return expr


def risk_engine_stream(spark: SparkSession, topic: str,
                       window_seconds: int = RISK_WINDOW_SECONDS,
                       watermark_seconds: int | None = None,
                       starting_offsets: str = "earliest") -> DataFrame:
    """Alert envelopes -> per-(customer, window) risk score + level.

    Callers use ``append`` output mode: each finalized window emits its risk
    record exactly once.
    """
    watermark_seconds = watermark_seconds or window_seconds
    parsed = kafka_event_stream(spark, topic, starting_offsets,
                                event_type="alerts")
    risk = (
        parsed
        .withWatermark("event_ts", f"{watermark_seconds} seconds")
        .groupBy(
            "customer_id",
            window(col("event_ts"), f"{window_seconds} seconds"),
        )
        .agg(
            _sum("risk_points").alias("total_points"),
            count("alert_id").alias("alert_count"),
            array_join(
                array_sort(collect_set("alert_type")), "+",
            ).alias("alert_types"),
        )
        .select(
            "customer_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "alert_count",
            "alert_types",
            "total_points",
            risk_level_expr(col("total_points")).alias("risk_level"),
        )
    )
    return risk.withColumn("event_time", col("window_start"))


def run_risk_engine(topic: str, window_seconds: int = RISK_WINDOW_SECONDS,
                    duration: int = 20, trigger: int = 5,
                    checkpoint: Path | None = None,
                    starting_offsets: str = "earliest",
                    memory_name: str | None = None,
                    validate=None) -> dict:
    """Run the risk engine for ``duration`` seconds (append mode).

    ``validate`` (optional) is called with the in-memory risk table before
    the Spark session stops; its return value is stored under ``validation``.
    """
    spark = get_spark(f"phase12-{topic}-w{window_seconds}")
    risk = risk_engine_stream(
        spark, topic, window_seconds, starting_offsets=starting_offsets,
    )

    print(f"\nRisk-engine schema ({window_seconds}s window, "
          f"levels {dict((lv, mn) for lv, mn in RISK_LEVELS)}):")
    risk.printSchema()

    totals, table = run_console_and_memory(
        spark, risk,
        label=f"phase12 risk engine on '{topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase12"),
        output_mode="append",
        memory_name=memory_name,
    )
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "window_seconds": window_seconds,
        "risk_levels": dict(RISK_LEVELS),
        "topic": topic,
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 12: risk engine (alert points -> "
                    "LOW/MEDIUM/HIGH/CRITICAL per customer per window)")
    parser.add_argument("--topic", default="fraud_alerts")
    parser.add_argument("--window", type=int, default=RISK_WINDOW_SECONDS,
                        help="risk aggregation window in seconds")
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase12"))
    args = parser.parse_args()

    totals = run_risk_engine(
        topic=args.topic,
        window_seconds=args.window,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 12 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
