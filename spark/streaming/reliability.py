#!/usr/bin/env python3
"""Phase 16 job: reliability-hardened streaming ingestion.

Milestone (spec Phase 16): the streaming consumer never crashes on bad input
and never emits duplicate events.

Three guarantees:

1. Dead-letter. A record that does not parse as a transaction is quarantined
   to the ``dead_letter`` topic with its raw key and value preserved
   byte-for-byte, while the rest of the stream keeps flowing. Spark 4.2's
   ``from_json`` returns a *non-null* struct whose fields are null for
   malformed (non-empty) JSON, so a record counts as unparseable when the
   struct itself or its ``transaction_id`` is null.

2. Dedup. ``withWatermark(event_ts, delay)`` +
   ``dropDuplicatesWithinWatermark(["transaction_id"])`` emit each
   transaction_id exactly once; a duplicate that arrives within the watermark
   delay never produces a second real row (Spark 4.2 may additionally surface
   it as an all-null placeholder row, but that emission is an
   execution-dependent artifact, not part of the contract). The dedup window
   is the watermark delay: "Events are deduplicated as long as the time
   distance of earliest and latest events are smaller than the delay
   threshold of watermark".

3. Late events. An event whose event_time is older than the watermark when it
   arrives is dropped by the dedup operator ("too late data older than
   watermark will be dropped"), so replays cannot double-process a
   transaction after its window closed.

Usage:
    python spark/streaming/reliability.py --topic phase16_controlled
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp

from kafka.config import BOOTSTRAP_SERVERS
from spark.common import REPO_ROOT, get_spark
from spark.schemas import EVENT_TIME_FORMAT, parse_event

WATERMARK_DELAY = "30 seconds"


def reliability_streams(spark: SparkSession, topic: str, *,
                        watermark: str = WATERMARK_DELAY,
                        dead_letter_topic: str = "dead_letter",
                        starting_offsets: str = "earliest",
                        event_type: str = "transactions"
                        ) -> tuple[object, DataFrame]:
    """Build the two output streams for ``topic``.

    Returns ``(bad_writer, deduped)``: ``bad_writer`` is the un-started Kafka
    sink that quarantines unparseable records (raw key + value preserved);
    ``deduped`` is the watermarked, deduplicated stream of valid events.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    parsed = raw.select(
        col("key"),
        col("value"),
        parse_event(event_type, col("value")).alias("parsed"),
    )
    bad = parsed.filter(
        "parsed IS NULL OR parsed.transaction_id IS NULL"
    ).select("key", "value")
    bad_writer = (
        bad.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", dead_letter_topic)
    )
    good = (
        parsed.filter("parsed.transaction_id IS NOT NULL")
        .select("parsed.*")
        .withColumn("event_ts", to_timestamp(col("event_time"), EVENT_TIME_FORMAT))
    )
    deduped = (
        good.withWatermark("event_ts", watermark)
        .dropDuplicatesWithinWatermark(["transaction_id"])
    )
    return bad_writer, deduped


class ReliabilityCollector:
    """Driver-side foreachBatch sink for the deduped stream.

    ``dropDuplicatesWithinWatermark`` outputs one row per input event: a real
    row for each unique transaction_id, nothing for events older than the
    watermark, and (execution-dependent) an all-null placeholder row for each
    dropped duplicate. ``rows()`` exposes every emitted row so a runner can
    verify the dedup + late-event contract; null placeholders are captured
    but never relied upon.
    """

    def __init__(self) -> None:
        self._rows: list[dict] = []
        self._batch_counts: dict[int, int] = {}

    def collect(self, df: DataFrame, epoch_id: int) -> None:
        batch = df.select("transaction_id", "event_ts").collect()
        for row in batch:
            self._rows.append({
                "batch_id": epoch_id,
                "transaction_id": row["transaction_id"],
                "event_ts": str(row["event_ts"]) if row["event_ts"] else None,
            })
        self._batch_counts[epoch_id] = len(batch)

    def rows(self) -> list[dict]:
        return list(self._rows)

    def batch_counts(self) -> dict[int, int]:
        return dict(self._batch_counts)


def run_reliability(topic: str, *, watermark: str = WATERMARK_DELAY,
                    dead_letter_topic: str = "dead_letter",
                    checkpoint: Path | None = None,
                    trigger: int = 1, max_batches: int = 6,
                    on_batch=None) -> dict:
    """Run the reliability job on ``topic`` until ``max_batches`` micro-batches.

    ``on_batch(query, batch_id)`` is called from the driver after each
    completed micro-batch so the caller can produce later-stage records while
    the stream is live (used by run_phase16 for the late-event demo).

    Returns a summary with the collected deduped rows, per-batch input
    counts, and the final health of both output queries.
    """
    spark = get_spark("phase16-reliability")
    bad_writer, deduped = reliability_streams(
        spark, topic, watermark=watermark,
        dead_letter_topic=dead_letter_topic)
    collector = ReliabilityCollector()

    checkpoint = Path(checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase16"))
    checkpoint.mkdir(parents=True, exist_ok=True)
    bad_query = (
        bad_writer.option("checkpointLocation", str(checkpoint / "bad")).start()
    )
    good_query = (
        deduped.writeStream.foreachBatch(collector.collect)
        .trigger(processingTime=f"{trigger} seconds")
        .option("checkpointLocation", str(checkpoint / "good"))
        .start()
    )

    healthy = good_query.isActive and bad_query.isActive
    last_seen = -1
    deadline = time.time() + 180
    try:
        while time.time() < deadline:
            progress = good_query.lastProgress
            batch_id = progress.batchId if progress is not None else -1
            if batch_id > last_seen:
                for number in range(last_seen + 1, batch_id + 1):
                    if on_batch is not None:
                        on_batch(good_query, number)
                last_seen = batch_id
            healthy = good_query.isActive and bad_query.isActive
            if batch_id >= max_batches:
                break
            time.sleep(0.5)
    finally:
        good_query.stop()
        bad_query.stop()

    summary = {
        "topic": topic,
        "batches_seen": last_seen,
        "rows_per_batch": collector.batch_counts(),
        "emitted_rows": collector.rows(),
        "queries_alive_at_stop": healthy,
        "good_exception": repr(good_query.exception()) if healthy else None,
    }
    spark.stop()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 16: reliability-hardened streaming ingestion")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--dead-letter-topic", default="dead_letter")
    parser.add_argument("--watermark", default=WATERMARK_DELAY)
    parser.add_argument("--max-batches", type=int, default=6)
    parser.add_argument("--trigger", type=int, default=1)
    args = parser.parse_args()

    summary = run_reliability(
        topic=args.topic, watermark=args.watermark,
        dead_letter_topic=args.dead_letter_topic,
        max_batches=args.max_batches, trigger=args.trigger,
    )
    print("\nPhase 16 reliability run:")
    print(f"  batches: {summary['batches_seen']}")
    print(f"  rows_per_batch: {summary['rows_per_batch']}")
    print(f"  emitted: {len(summary['emitted_rows'])} "
          f"(queries alive: {summary['queries_alive_at_stop']})")


if __name__ == "__main__":
    sys.exit(main())
