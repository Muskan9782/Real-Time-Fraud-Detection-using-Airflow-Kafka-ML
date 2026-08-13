#!/usr/bin/env python3
"""Phase 4 job: Kafka -> PySpark Structured Streaming -> console.

Reads one event topic from Kafka, parses each message's JSON ``value`` with an
*explicit* schema, converts ``event_time`` to a real timestamp, and prints
every micro-batch to the console. Checkpointing means a restart resumes at the
committed offsets - no messages lost, none re-read.

Usage:
    python spark/streaming/kafka_to_console.py --topic transactions \
        --duration 30 --trigger 5
    python spark/streaming/kafka_to_console.py --duration 15 --starting-offsets latest

Note: on Windows run Spark once first with ``python run_phase4.py`` - that
downloads the spark-sql-kafka connector jar and verifies the JDK/winutils
setup in one place.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running as a script (``python spark/streaming/kafka_to_console.py``)
# as well as ``python -m spark.streaming.kafka_to_console``.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql.functions import col

from spark.common import REPO_ROOT, get_spark
from spark.schemas import EVENT_SCHEMAS
from spark.streaming.runner import run_console_and_memory
from spark.streaming.sources import kafka_event_stream


def run_streaming(topic: str, duration: int, trigger: int,
                  checkpoint: Path, starting_offsets: str = "earliest",
                  memory_name: str | None = None) -> dict:
    """Run the streaming query for ``duration`` seconds and report totals.

    ``checkpoint`` drives exactly-once-ish resume semantics; pass a fresh
    directory for a first run, the same one again to resume from committed
    offsets. With ``memory_name`` the parsed rows are also stored in an
    in-memory table so the caller can count/validate them.
    """
    spark = get_spark(f"phase4-{topic}")
    parsed = kafka_event_stream(spark, topic, starting_offsets)

    print("\nParsed stream schema (explicit, no guessing):")
    parsed.printSchema()

    totals, table = run_console_and_memory(
        spark, parsed,
        label=f"phase4 {topic}",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint,
        output_mode="append",
        memory_name=memory_name,
        num_rows=5,
    )

    consumed = totals["rows_in_memory"]
    bad_rows = 0
    if table is not None:
        # Every row must have parsed event_time -> event_ts; any null means the
        # explicit schema or timestamp format did not match the Kafka payload.
        bad_rows = table.where(col("event_ts").isNull()).count()
        print(f"Sample rows consumed ({min(3, consumed)} of {consumed}):")
        table.show(3, truncate=False)
    spark.stop()

    return {
        "topic": topic,
        "duration_s": duration,
        "batches": totals["batches"],
        "rows_read_from_kafka": totals["rows_read_from_kafka"],
        "rows_in_memory": consumed,
        "bad_rows": bad_rows,
        "checkpoint": str(checkpoint),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 4: Kafka -> console streaming")
    parser.add_argument("--topic", default="transactions", choices=EVENT_SCHEMAS)
    parser.add_argument("--duration", type=int, default=30,
                        help="seconds to run before stopping")
    parser.add_argument("--trigger", type=int, default=5,
                        help="processing-time trigger interval in seconds")
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(REPO_ROOT / "spark/checkpoints/phase4"),
                        help="checkpoint directory (same dir = resume)")
    args = parser.parse_args()

    totals = run_streaming(
        topic=args.topic,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 4 summary:")
    print(json.dumps(totals, indent=2))
    if totals["rows_read_from_kafka"] == 0 and totals["batches"] == 0:
        print("Note: 0 batches - check the topic has data and Kafka is reachable.")


if __name__ == "__main__":
    sys.exit(main())
