"""Sink lifecycle + measurement helpers shared by every streaming job.

A job builds a streaming ``DataFrame``, then hands it to
:func:`run_console_and_memory`, which:

- prints micro-batches to the console (the "look at it" sink),
- optionally stores the output in an in-memory table so the caller can count
  and validate rows *before* the Spark session is stopped,
- runs for a fixed number of seconds, stops cleanly, and reports totals from
  the streaming query progress (how many input rows Kafka delivered).
"""

from __future__ import annotations

import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def run_console_and_memory(spark: SparkSession, stream: DataFrame, *,
                           label: str, duration: int, trigger: int,
                           checkpoint: Path, output_mode: str = "append",
                           memory_name: str | None = None,
                           num_rows: int = 10) -> tuple[dict, DataFrame | None]:
    """Run ``stream`` for ``duration`` seconds with console (+memory) sinks.

    Returns ``(totals, memory_table)``. The Spark session is left running so
    the caller can inspect ``memory_table`` (validation) and stop it after.
    """
    console = (
        stream.writeStream.outputMode(output_mode)
        .format("console")
        .option("truncate", "false")
        .option("numRows", num_rows)
        .trigger(processingTime=f"{trigger} seconds")
        .option("checkpointLocation", str(checkpoint))
        .start()
    )
    queries = [console]
    if memory_name:
        # The memory sink is not resumable, so it needs its own checkpoint dir
        # alongside the console sink's shared one.
        memory = (
            stream.writeStream.outputMode(output_mode)
            .format("memory")
            .queryName(memory_name)
            .trigger(processingTime=f"{trigger} seconds")
            .option("checkpointLocation", str(checkpoint / "memory" / memory_name))
            .start()
        )
        queries.append(memory)

    print(f"\n[{label}] running for {duration}s (trigger {trigger}s) ...")
    time.sleep(duration)

    for query in queries:
        query.stop()

    batches = [p for p in console.recentProgress if p is not None]
    totals: dict = {
        "batches": len(batches),
        "rows_read_from_kafka": sum(p["numInputRows"] for p in batches),
    }
    table = spark.table(memory_name) if memory_name else None
    if table is not None:
        totals["rows_in_memory"] = table.count()
    return totals, table
