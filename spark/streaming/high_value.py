#!/usr/bin/env python3
"""Phase 11 job: historical-anomaly detector (7d / 30d averages, >5x).

Per customer, keeps a stateful record of their past transactions and computes
a rolling average transaction amount over the trailing 7-day and 30-day
windows. A new transaction is flagged as ``HIGH_VALUE_ANOMALY`` (+25 risk
points) when its amount exceeds ``high_value_multiplier`` (5x by default) times
the customer's historical baseline - never by an absolute threshold, so a
consistently high-spending customer is not flagged for the same amount that
alerts a low-spending one.

Implementation notes:

- uses the Spark 4 ``StatefulProcessor`` API (``transformWithState``) with a
  per-key ``ValueState`` holding the trailing 30-day history, packed as the
  small string ``"ts_ms,amount;..."``; entries older than 30 days are pruned
  as each new event arrives,
- ``ListState`` is the natural fit for a growing history, but on PySpark
  4.2.0 (the latest release) any ``transformWithState`` query that declares a
  ``ListState`` fails before the first batch with ``java.io.OptionalDataException``
  while the JVM deserializes the task closure (reproduced with a minimal
  ``rate``-source query: ``TransformWithStateInPySparkStateServer: No more data
  to read from the socket`` followed by a ``HashMap.readObject`` failure). The
  packed-``ValueState`` design below is deterministic and avoids the broken
  code path entirely; switch back to ``ListState`` once the Spark closure
  serialization is fixed,
- ``timeMode="ProcessingTime"`` (no watermark needed) and ``update`` output
  mode: alerts are emitted as soon as the anomaly is seen,
- baseline = ``max(avg_7d, avg_30d)``: the longer window keeps older behavior
  in the average, so a one-off spike 20 days ago raises the 30-day baseline
  and suppresses a borderline alert that the 7-day window alone would fire
  (demonstrated in the Phase 11 runner),
- the current transaction is excluded from its own average, and an alert needs
  at least ``min_history`` prior transactions in the window (a customer with
  (almost) no history is never flagged).

Usage:
    python spark/streaming/high_value.py --topic transactions --multiplier 5
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running as a script as well as ``python -m ...``.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pyspark.sql import DataFrame, Row, SparkSession
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

HIGH_VALUE_ALERT_TYPE = "HIGH_VALUE_ANOMALY"
HIGH_VALUE_RISK_POINTS = 25  # config/phase1.json risk_points

DEFAULT_MULTIPLIER = 5.0   # high_value_multiplier -> flag when amount > 5x avg
DEFAULT_MIN_HISTORY = 2    # transactions required in the window before alerting

WINDOW_7D_SECONDS = 7 * 24 * 3600      # 604800
WINDOW_30D_SECONDS = 30 * 24 * 3600    # 2592000

# Per-key state: the trailing 30-day history packed as "ts_ms,amount;...".
# A single-field ValueState (ListState crashes in the Spark 4.2.0 closure).
STATE_SCHEMA = StructType([
    StructField("payload", StringType()),
])

# Alert row produced by the processor (customer_id comes from the input row).
ALERT_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("avg_7d", DoubleType()),
    StructField("avg_30d", DoubleType()),
    StructField("baseline", DoubleType()),
    StructField("ratio", DoubleType()),
    StructField("ts_ms", LongType()),
    StructField("alert_type", StringType()),
    StructField("risk_points", IntegerType()),
])


class _HighValueProcessor(StatefulProcessor):
    """Rolling 7d/30d average baseline, per customer."""

    def __init__(self, multiplier: float = DEFAULT_MULTIPLIER,
                 min_history: int = DEFAULT_MIN_HISTORY,
                 window_7d_seconds: int = WINDOW_7D_SECONDS,
                 window_30d_seconds: int = WINDOW_30D_SECONDS):
        self._multiplier = multiplier
        self._min_history = min_history
        self._win7 = window_7d_seconds * 1000
        self._win30 = window_30d_seconds * 1000

    def init(self, handle: StatefulProcessorHandle) -> None:
        self._history = handle.getValueState("history", STATE_SCHEMA)

    @staticmethod
    def _unpack(payload: str) -> list:
        entries = []
        if payload:
            for part in payload.split(";"):
                if not part:
                    continue
                t, a = part.split(",")
                entries.append((int(t), float(a)))
        return entries

    @staticmethod
    def _pack(entries) -> str:
        return ";".join(f"{t},{a}" for t, a in entries)

    def handleInputRows(self, key, rows, timer_values: TimerValues):
        for row in sorted(rows, key=lambda r: r.ts_ms):
            cutoff_30 = row.ts_ms - self._win30
            cutoff_7 = row.ts_ms - self._win7

            prev = self._history.get()
            history = self._unpack(prev[0]) if prev is not None else []
            history = [(t, a) for (t, a) in history if t >= cutoff_30]
            amounts_30 = [a for _t, a in history]
            amounts_7 = [a for (t, a) in history if t >= cutoff_7]
            avg_7 = sum(amounts_7) / len(amounts_7) if amounts_7 else 0.0
            avg_30 = sum(amounts_30) / len(amounts_30) if amounts_30 else 0.0
            baseline = max(avg_7, avg_30)

            if (len(amounts_30) >= self._min_history and baseline > 0.0
                    and row.amount > self._multiplier * baseline):
                yield Row(
                    transaction_id=row.transaction_id,
                    customer_id=row.customer_id,
                    amount=float(row.amount),
                    avg_7d=avg_7,
                    avg_30d=avg_30,
                    baseline=baseline,
                    ratio=float(row.amount) / baseline,
                    ts_ms=row.ts_ms,
                    alert_type=HIGH_VALUE_ALERT_TYPE,
                    risk_points=HIGH_VALUE_RISK_POINTS,
                )
            history.append((row.ts_ms, float(row.amount)))
            self._history.update((self._pack(history),))


def high_value_alert_stream(spark: SparkSession, topic: str,
                            multiplier: float = DEFAULT_MULTIPLIER,
                            min_history: int = DEFAULT_MIN_HISTORY,
                            starting_offsets: str = "earliest") -> DataFrame:
    """Transactions -> per-customer rolling-average anomaly check -> alerts.

    Callers use ``update`` output mode.
    """
    parsed = kafka_event_stream(spark, topic, starting_offsets,
                                event_type="transactions")
    prepared = parsed.select(
        "transaction_id",
        "customer_id",
        "amount",
        unix_millis(col("event_ts")).alias("ts_ms"),
    )
    alerts = (
        prepared
        .groupBy("customer_id")
        .transformWithState(
            statefulProcessor=_HighValueProcessor(multiplier, min_history),
            outputStructType=ALERT_SCHEMA,
            outputMode="update",
            timeMode="ProcessingTime",
        )
    )
    return alerts.withColumn("event_time", (col("ts_ms") / 1000.0).cast("timestamp"))


def run_high_value(topic: str, multiplier: float = DEFAULT_MULTIPLIER,
                   min_history: int = DEFAULT_MIN_HISTORY,
                   duration: int = 20, trigger: int = 5,
                   checkpoint: Path | None = None,
                   starting_offsets: str = "earliest",
                   memory_name: str | None = None,
                   validate=None) -> dict:
    """Run the historical-anomaly detector for ``duration`` seconds.

    ``validate`` (optional) is called with the in-memory alert table before
    the Spark session stops; its return value is stored under ``validation``.
    """
    spark = get_spark(f"phase11-{topic}-x{multiplier}-h{min_history}")
    alerts = high_value_alert_stream(
        spark, topic, multiplier, min_history,
        starting_offsets=starting_offsets,
    )

    print(f"\nHistorical-anomaly alert schema "
          f"(amount > {multiplier}x 7d/30d average, min history {min_history}):")
    alerts.printSchema()

    totals, table = run_console_and_memory(
        spark, alerts,
        label=f"phase11 high-value anomaly >{multiplier}x avg on '{topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase11"),
        output_mode="update",
        memory_name=memory_name,
    )
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "multiplier": multiplier,
        "min_history": min_history,
        "alert_type": HIGH_VALUE_ALERT_TYPE,
        "topic": topic,
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 11: historical-anomaly detector "
                    "(amount > multiplier x 7d/30d rolling average)")
    parser.add_argument("--topic", default="transactions", choices=EVENT_SCHEMAS)
    parser.add_argument("--multiplier", type=float, default=DEFAULT_MULTIPLIER,
                        help="flag when amount > this many x the baseline")
    parser.add_argument("--min-history", type=int, default=DEFAULT_MIN_HISTORY,
                        help="transactions required in the window before alerting")
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase11"))
    args = parser.parse_args()

    totals = run_high_value(
        topic=args.topic,
        multiplier=args.multiplier,
        min_history=args.min_history,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 11 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
