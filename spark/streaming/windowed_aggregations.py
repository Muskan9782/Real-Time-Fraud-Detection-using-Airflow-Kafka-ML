#!/usr/bin/env python3
"""Phase 5 job: windowed aggregations over transactions.

Groups the ``transactions`` stream by event-time windows and computes
count / sum / avg of ``amount`` per window:

- tumbling  = fixed, non-overlapping windows (2 minutes)
- sliding   = overlapping windows (2 minutes long, 30 second slide)

Uses ``update`` output mode, which is the mode for streaming aggregations
(append mode needs a watermark - that is Phase 6).

Usage:
    python spark/streaming/windowed_aggregations.py --window 120 --slide 30
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
from pyspark.sql.functions import avg, col, count, sum as _sum, window

from spark.common import REPO_ROOT, get_spark
from spark.schemas import EVENT_SCHEMAS
from spark.streaming.runner import run_console_and_memory
from spark.streaming.sources import kafka_event_stream

DEFAULT_WINDOW = 120
DEFAULT_SLIDE = 30


def aggregated_stream(spark: SparkSession, topic: str,
                      window_seconds: int, slide_seconds: int,
                      starting_offsets: str = "earliest") -> DataFrame:
    """Parsed events -> per-window count / sum / avg of amount."""
    parsed = kafka_event_stream(spark, topic, starting_offsets,
                                event_type="transactions")
    agg = (
        parsed
        .groupBy(window(col("event_ts"),
                        f"{window_seconds} seconds", f"{slide_seconds} seconds"))
        .agg(
            count("transaction_id").alias("tx_count"),
            _sum("amount").alias("amount_sum"),
            avg("amount").alias("amount_avg"),
        )
    )
    return (
        agg.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "tx_count",
            "amount_sum",
            "amount_avg",
        )
    )


def run_windowed(topic: str, window_seconds: int, slide_seconds: int,
                 duration: int, trigger: int, checkpoint: Path,
                 starting_offsets: str = "earliest",
                 memory_name: str | None = None,
                 validate=None) -> dict:
    """Run one windowed aggregation for ``duration`` seconds.

    ``validate`` (optional) is called with the in-memory result table before
    the Spark session stops; its return value is stored under ``validation``.
    """
    spark = get_spark(f"phase5-{topic}-w{window_seconds}-s{slide_seconds}")
    agg = aggregated_stream(spark, topic, window_seconds, slide_seconds,
                            starting_offsets)

    print(f"\nWindowed aggregation schema "
          f"({window_seconds}s window / {slide_seconds}s slide):")
    agg.printSchema()

    totals, table = run_console_and_memory(
        spark, agg,
        label=f"phase5 {window_seconds}s/{slide_seconds}s on '{topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint,
        output_mode="update",
        memory_name=memory_name,
    )
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "window_seconds": window_seconds,
        "slide_seconds": slide_seconds,
        "topic": topic,
        "checkpoint": str(checkpoint),
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 5: windowed aggregations (count/sum/avg of amount)")
    parser.add_argument("--topic", default="transactions", choices=EVENT_SCHEMAS)
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW,
                        help="window length in seconds")
    parser.add_argument("--slide", type=int, default=DEFAULT_SLIDE,
                        help="slide (0 or == window => tumbling)")
    parser.add_argument("--duration", type=int, default=25)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase5"))
    args = parser.parse_args()

    if args.slide == 0 or args.slide == args.window:
        args.slide = args.window  # tumbling

    totals = run_windowed(
        topic=args.topic,
        window_seconds=args.window,
        slide_seconds=args.slide,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 5 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
