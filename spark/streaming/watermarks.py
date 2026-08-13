#!/usr/bin/env python3
"""Phase 6 job: watermarks + late events over windowed aggregations.

Extends the Phase 5 aggregation with ``withWatermark("event_ts", N)`` and
``append`` output mode: a window's aggregate is emitted exactly once, when the
watermark (max event time seen - N) passes the end of the window. Late events
fall into two buckets:

- event_time newer than the watermark -> still included in its window
- event_time older than the watermark -> dropped (never counted)

The controlled late-event staging lives in ``run_phase6.py``; this module is
the reusable stream + a plain CLI job (read a topic for ``--duration`` s).

Usage:
    python spark/streaming/watermarks.py --topic transactions --watermark 60
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
DEFAULT_SLIDE = 120
DEFAULT_WATERMARK = 60


def watermarked_aggregated_stream(
    spark: SparkSession, topic: str, window_seconds: int, slide_seconds: int,
    watermark_seconds: int, starting_offsets: str = "earliest",
) -> DataFrame:
    """Transactions -> per-window count/sum/avg guarded by an event watermark.

    Callers use ``append`` output mode so each finalized window is emitted once
    and records older than the watermark are dropped.
    """
    parsed = kafka_event_stream(spark, topic, starting_offsets,
                                event_type="transactions")
    agg = (
        parsed
        .withWatermark("event_ts", f"{watermark_seconds} seconds")
        .groupBy(window(col("event_ts"),
                        f"{window_seconds} seconds", f"{slide_seconds} seconds"))
        .agg(
            count("transaction_id").alias("tx_count"),
            _sum("amount").alias("amount_sum"),
            avg("amount").alias("amount_avg"),
        )
    )
    return agg.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "tx_count",
        "amount_sum",
        "amount_avg",
    )


def run_watermarked(topic: str, window_seconds: int, slide_seconds: int,
                    watermark_seconds: int, duration: int, trigger: int,
                    checkpoint: Path, starting_offsets: str = "earliest",
                    memory_name: str | None = None) -> dict:
    """Run the watermarked aggregation for ``duration`` seconds (append mode)."""
    spark = get_spark(
        f"phase6-{topic}-w{window_seconds}-wm{watermark_seconds}"
    )
    stream = watermarked_aggregated_stream(
        spark, topic, window_seconds, slide_seconds, watermark_seconds,
        starting_offsets,
    )

    print(f"\nWatermarked aggregation schema "
          f"({window_seconds}s window / {slide_seconds}s slide / "
          f"{watermark_seconds}s watermark):")
    stream.printSchema()

    totals, table = run_console_and_memory(
        spark, stream,
        label=f"phase6 {window_seconds}s/{slide_seconds}s "
              f"wm {watermark_seconds}s on '{topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint,
        output_mode="append",
        memory_name=memory_name,
    )
    if memory_name:
        totals["rows_in_memory"] = table.count()
    spark.stop()

    totals.update({
        "window_seconds": window_seconds,
        "slide_seconds": slide_seconds,
        "watermark_seconds": watermark_seconds,
        "topic": topic,
        "checkpoint": str(checkpoint),
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 6: watermarks + late events (append output)")
    parser.add_argument("--topic", default="transactions", choices=EVENT_SCHEMAS)
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW,
                        help="window length in seconds")
    parser.add_argument("--slide", type=int, default=DEFAULT_SLIDE,
                        help="slide (0 or == window => tumbling)")
    parser.add_argument("--watermark", type=int, default=DEFAULT_WATERMARK,
                        help="allowed lateness in seconds")
    parser.add_argument("--duration", type=int, default=25)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase6"))
    args = parser.parse_args()

    if args.slide == 0 or args.slide == args.window:
        args.slide = args.window  # tumbling

    totals = run_watermarked(
        topic=args.topic,
        window_seconds=args.window,
        slide_seconds=args.slide,
        watermark_seconds=args.watermark,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 6 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
