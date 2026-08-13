#!/usr/bin/env python3
"""Phase 17 job: checkpoint-driven state recovery.

Milestone (spec Phase 17): a stateful streaming query survives a driver
restart - the checkpoint directory restores exactly what was lost (Kafka
source offsets, the RocksDB state store, the streaming watermark, the batch
counter), so the restarted job resumes where the old one stopped: no data
loss, no duplicate re-processing, no lost state.

How it is proven on this box:

- ``recovery_streams`` builds the same watermarked, deduplicated operator
  that Phase 16 used (``withWatermark`` + ``dropDuplicatesWithinWatermark``,
  RocksDB-backed). Its dedup table is *keyed* state: after a restart, a
  duplicate of a transaction that was seen before the restart must still be
  suppressed - that can only happen if the RocksDB state was restored.
- The watermark is operator state too. Run A pushes it to 20 s; if run B
  started with a fresh watermark it would accept TX-0004@15 s (batch-2 max
  38 s -> a fresh watermark would be 8 s), so dropping it after the restart
  proves the watermark was restored.
- The Kafka source offsets and the batch counter live in the checkpoint:
  run B must start at run A's last committed offset (no overlap) and must
  continue the batch ids (no reset).

Usage:
    python spark/streaming/checkpoint_recovery.py --topic phase17_controlled \
        --checkpoint spark/checkpoints/phase17/demo-1
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp

from kafka.config import BOOTSTRAP_SERVERS
from spark.common import REPO_ROOT, get_spark
from spark.schemas import EVENT_TIME_FORMAT, parse_event

WATERMARK_DELAY = "30 seconds"


def recovery_streams(spark: SparkSession, topic: str, *,
                     watermark: str = WATERMARK_DELAY,
                     starting_offsets: str = "earliest",
                     event_type: str = "transactions") -> DataFrame:
    """Build the stateful (dedup) stream for ``topic``.

    Raw Kafka bytes -> explicit transactions schema -> real ``event_ts`` ->
    ``withWatermark`` + ``dropDuplicatesWithinWatermark``. The dedup operator
    is RocksDB-backed keyed state, so it is the right probe for checkpoint /
    state recovery across a restart.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    parsed = raw.select(parse_event(event_type, col("value")).alias("parsed"))
    good = (
        parsed.filter("parsed.transaction_id IS NOT NULL")
        .select("parsed.*")
    )
    deduped = (
        good.withColumn("event_ts",
                        to_timestamp(col("event_time"), EVENT_TIME_FORMAT))
        .withWatermark("event_ts", watermark)
        .dropDuplicatesWithinWatermark(["transaction_id"])
    )
    return deduped


class RecoveryCollector:
    """Driver-side foreachBatch sink: rows emitted by the dedup operator.

    Captures every emitted row (real row per unique transaction_id, plus the
    execution-dependent all-null placeholder rows for dropped duplicates).
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

    def real_ids(self) -> list[str]:
        return [r["transaction_id"] for r in self._rows
                if r["transaction_id"]]

    def null_placeholder_count(self) -> int:
        return sum(1 for r in self._rows if not r["transaction_id"])


def _normalize_progress(progress) -> dict:
    """Reduce a StreamingQueryProgress to the fields Phase 17 verifies."""
    sources = progress.get("sources") or []
    source = sources[0] if sources else {}

    def offsets(value):
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return value
        return value

    return {
        "batch_id": progress.get("batchId"),
        "num_input_rows": progress.get("numInputRows"),
        "input_rows_per_second": progress.get("inputRowsPerSecond"),
        "watermark_ms": (progress.get("eventTime") or {}).get("watermark"),
        "source_start_offset": offsets(source.get("startOffset")),
        "source_end_offset": offsets(source.get("endOffset")),
    }


def run_recovery(topic: str, *, checkpoint: Path, new_batches: int = 2,
                 watermark: str = WATERMARK_DELAY, trigger: int = 1,
                 starting_offsets: str = "earliest",
                 timeout: float = 180.0) -> dict:
    """Run one query lifecycle on ``topic`` with ``checkpoint`` as the
    checkpoint directory, processing ``new_batches`` micro-batches.

    Calling this twice with the *same* checkpoint simulates a driver restart:
    the second call starts a fresh SparkSession and must resume from the
    offsets / state / watermark the first call committed.

    Returns a summary with the per-batch progress (including Kafka source
    start/end offsets and the watermark) and every row the dedup operator
    emitted.
    """
    spark = get_spark("phase17-recovery")
    deduped = recovery_streams(
        spark, topic, watermark=watermark,
        starting_offsets=starting_offsets)
    collector = RecoveryCollector()

    checkpoint = Path(checkpoint)
    checkpoint.mkdir(parents=True, exist_ok=True)
    query = (
        deduped.writeStream.foreachBatch(collector.collect)
        .trigger(processingTime=f"{trigger} seconds")
        .option("checkpointLocation", str(checkpoint))
        .start()
    )

    progress_log: list[dict] = []
    batch_ids: list[int] = []
    first_id: int | None = None
    healthy = query.isActive
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            progress = query.lastProgress
            if progress is not None:
                batch_id = progress.get("batchId", -1)
                if batch_id >= 0:
                    if first_id is None:
                        first_id = batch_id
                    if batch_id not in batch_ids:
                        batch_ids.append(batch_id)
                        progress_log.append(_normalize_progress(progress))
                    if batch_id - first_id + 1 >= new_batches:
                        break
            healthy = query.isActive
            time.sleep(0.5)
    finally:
        query.stop()

    summary = {
        "topic": topic,
        "checkpoint": str(checkpoint),
        "batch_ids": batch_ids,
        "progress": progress_log,
        "num_input_rows": sum(
            p.get("num_input_rows") or 0 for p in progress_log),
        "emitted_rows": collector.rows(),
        "real_ids": collector.real_ids(),
        "null_placeholder_count": collector.null_placeholder_count(),
        "queries_alive_at_stop": healthy,
        "exception": repr(query.exception()) if healthy else None,
    }
    spark.stop()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 17: checkpoint-driven state recovery")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--watermark", default=WATERMARK_DELAY)
    parser.add_argument("--new-batches", type=int, default=2)
    parser.add_argument("--trigger", type=int, default=1)
    args = parser.parse_args()

    summary = run_recovery(
        topic=args.topic, checkpoint=Path(args.checkpoint),
        watermark=args.watermark, new_batches=args.new_batches,
        trigger=args.trigger,
    )
    print("\nPhase 17 recovery lifecycle:")
    print(f"  batch_ids: {summary['batch_ids']}")
    print(f"  num_input_rows: {summary['num_input_rows']}")
    print(f"  real_ids: {summary['real_ids']} "
          f"(alive at stop: {summary['queries_alive_at_stop']})")


if __name__ == "__main__":
    sys.exit(main())
