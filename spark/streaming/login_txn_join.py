#!/usr/bin/env python3
"""Phase 9 job: stream-stream join - login correlated with a transaction.

Flags a transaction when the customer has a *successful* login within the
preceding ``login_transaction_max_gap_seconds`` (default 300 s / 5 minutes),
emitting a ``LOGIN_TRANSACTION_CORRELATION`` alert (+20 risk points). This is
the login+transaction fraud scenario: an unfamiliar-device login followed by a
transaction from that device.

Implementation notes:

- two live Kafka streams (``logins``, ``transactions``) joined **as streams**
  (no batch materialization) on ``customer_id`` with an event-time range
  condition ``login_ts <= tx_ts <= login_ts + gap``,
- both sides are watermarked (``event_ts``); the join range must be at least
  as large as each side's watermark delay, so the engine can bound and evict
  state,
- ``append`` output mode: a matched pair is emitted exactly once, only after
  both watermarks advance past the point where that pair could still gain new
  matches (standard stream-stream join finalization semantics),
- only ``success`` logins participate, matching the injected scenario.

Usage:
    python spark/streaming/login_txn_join.py
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
from pyspark.sql.functions import col, lit, unix_millis
from pyspark.sql.functions import expr

from spark.common import REPO_ROOT, get_spark
from spark.streaming.runner import run_console_and_memory
from spark.streaming.sources import kafka_event_stream

LOGIN_TXN_ALERT_TYPE = "LOGIN_TRANSACTION_CORRELATION"
LOGIN_TXN_RISK_POINTS = 20  # config/phase1.json risk_points

DEFAULT_MAX_GAP_SECONDS = 300  # login_transaction_max_gap_seconds (5 min)

LOGIN_TOPIC = "logins"
TXN_TOPIC = "transactions"


def login_txn_correlation_stream(spark: SparkSession, *,
                                 login_topic: str = LOGIN_TOPIC,
                                 txn_topic: str = TXN_TOPIC,
                                 max_gap_seconds: int = DEFAULT_MAX_GAP_SECONDS,
                                 watermark_seconds: int | None = None,
                                 starting_offsets: str = "earliest") -> DataFrame:
    """Successful logins stream-joined with transactions within ``gap``.

    Callers use ``append`` output mode: each finalized (login, transaction)
    pair is appended exactly once.
    """
    watermark_seconds = watermark_seconds or max_gap_seconds

    logins = kafka_event_stream(spark, login_topic, starting_offsets,
                                event_type="logins")
    txns = kafka_event_stream(spark, txn_topic, starting_offsets,
                              event_type="transactions")

    login_side = (
        logins
        .filter(col("success"))
        .withWatermark("event_ts", f"{watermark_seconds} seconds")
        .select(
            col("login_id"),
            col("customer_id"),
            col("event_ts").alias("login_ts"),
        )
        .alias("l")
    )
    txn_side = (
        txns
        .withWatermark("event_ts", f"{watermark_seconds} seconds")
        .select(
            col("transaction_id"),
            col("customer_id"),
            col("event_ts").alias("tx_ts"),
        )
        .alias("t")
    )

    # Time-range stream-stream join. For a given login the transactions that
    # can match lie in [login_ts, login_ts + gap]; for a given transaction the
    # logins lie in [tx_ts - gap, tx_ts], so both sides are bounded and can be
    # cleaned up once the watermarks pass the join range.
    matched = txn_side.join(
        login_side,
        expr(
            "t.customer_id = l.customer_id"
            f" AND t.tx_ts >= l.login_ts"
            f" AND t.tx_ts <= l.login_ts + interval {max_gap_seconds} seconds"
        ),
        how="inner",
    )

    alerts = matched.select(
        col("t.transaction_id"),
        col("t.customer_id"),
        col("l.login_id"),
        unix_millis(col("l.login_ts")).alias("login_ts_ms"),
        unix_millis(col("t.tx_ts")).alias("tx_ts_ms"),
        ((unix_millis(col("t.tx_ts")) - unix_millis(col("l.login_ts"))) / 1000.0)
        .alias("gap_seconds"),
        lit(LOGIN_TXN_ALERT_TYPE).alias("alert_type"),
        lit(LOGIN_TXN_RISK_POINTS).alias("risk_points"),
        col("t.tx_ts").alias("event_time"),
    )
    return alerts


def run_login_txn_correlation(topic: tuple[str, str] = (LOGIN_TOPIC, TXN_TOPIC),
                              max_gap_seconds: int = DEFAULT_MAX_GAP_SECONDS,
                              duration: int = 30, trigger: int = 5,
                              checkpoint: Path | None = None,
                              starting_offsets: str = "earliest",
                              memory_name: str | None = None,
                              validate=None) -> dict:
    """Run the login->transaction correlation join for ``duration`` seconds.

    ``validate`` (optional) is called with the in-memory alert table before
    the Spark session stops; its return value is stored under ``validation``.
    """
    login_topic, txn_topic = topic
    spark = get_spark(f"phase9-{login_topic}-{txn_topic}-g{max_gap_seconds}")
    alerts = login_txn_correlation_stream(
        spark,
        login_topic=login_topic,
        txn_topic=txn_topic,
        max_gap_seconds=max_gap_seconds,
        starting_offsets=starting_offsets,
    )

    print(f"\nLogin->transaction correlation alert schema "
          f"(tx within {max_gap_seconds}s of a successful login):")
    alerts.printSchema()

    totals, table = run_console_and_memory(
        spark, alerts,
        label=f"phase9 login->txn join (gap <= {max_gap_seconds}s) "
              f"on '{login_topic}' + '{txn_topic}'",
        duration=duration,
        trigger=trigger,
        checkpoint=checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase9"),
        output_mode="append",
        memory_name=memory_name,
    )
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "max_gap_seconds": max_gap_seconds,
        "alert_type": LOGIN_TXN_ALERT_TYPE,
        "login_topic": login_topic,
        "txn_topic": txn_topic,
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 9: stream-stream join - login -> transaction "
                    "(transaction within 5 min of a successful login)")
    parser.add_argument("--login-topic", default=LOGIN_TOPIC)
    parser.add_argument("--txn-topic", default=TXN_TOPIC)
    parser.add_argument("--gap", type=int, default=DEFAULT_MAX_GAP_SECONDS,
                        help="max seconds between login and transaction")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase9"))
    args = parser.parse_args()

    totals = run_login_txn_correlation(
        topic=(args.login_topic, args.txn_topic),
        max_gap_seconds=args.gap,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 9 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
