#!/usr/bin/env python3
"""Phase 14 job: streaming ML inference with the Phase 13 XGBoost model.

Every incoming transaction / payment is scored online with the offline model
(``ml/model/xgb_model.json``): the 14 features from ``ml/features`` are
recomputed from the customer's accumulated event history (strictly-prior
events only, exactly like the offline dataset) and the model outputs a
``fraud_probability`` per event.

Implementation notes:

- scoring runs in a driver-side ``foreachBatch`` callback instead of a
  ``transformWithState`` ``StatefulProcessor``: PySpark 4.2.0's Python state
  server is broken on this box (the Phase 11 bug family - the task closure /
  state server socket dies on *any* transformWithState query, even a minimal
  rate-source repro, with ``java.io.OptionalDataException`` and "No more data
  to read from the socket"). ``foreachBatch`` needs no state server.
- per micro-batch the callback re-appends the batch's events and recomputes
  features over the *full* per-customer history with
  ``ml.features._customer_features`` (the same function that built the offline
  training data), so the online feature values reproduce the training-time
  values exactly; every event is emitted exactly once (one scored row per
  input event).
- the model, the customer reference table and the feature order are loaded
  once on the driver; per batch all accumulated rows are predicted in one
  ``predict_proba`` call.
- ``foreachBatch`` runs a Spark job from inside the streaming thread, which
  needs the ``-Xss32m`` JVM thread stack set by ``spark.common.get_spark``.

Usage:
    python spark/streaming/ml_inference.py --tx-topic transactions \
        --pay-topic payments --threshold 0.5
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Allow running as a script as well as ``python -m ...``.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import numpy as np
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, lit, unix_millis
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark.common import REPO_ROOT, get_spark
from spark.streaming.sources import kafka_event_stream

DEFAULT_THRESHOLD = 0.5
DEFAULT_MODEL = REPO_ROOT / "ml" / "model" / "xgb_model.json"
DEFAULT_CUSTOMERS = REPO_ROOT / "data" / "raw" / "customers.csv"

# Scored row produced for every input event (one row per event).
OUTPUT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("ts_ms", LongType()),
    StructField("amount", DoubleType()),
    StructField("is_payment", IntegerType()),
    StructField("is_failed", IntegerType()),
    StructField("tx_count_120s", IntegerType()),
    StructField("failed_pay_count_60s", IntegerType()),
    StructField("event_count_300s", IntegerType()),
    StructField("seconds_since_prev_event", DoubleType()),
    StructField("is_new_device", DoubleType()),
    StructField("is_new_merchant", IntegerType()),
    StructField("dist_km_from_home", DoubleType()),
    StructField("amount_vs_customer_avg", DoubleType()),
    StructField("fraud_probability", DoubleType()),
    StructField("ml_prediction", IntegerType()),
])


def _features_module():
    """Offline feature builder - single source of truth for features."""
    from ml.features import FEATURE_COLUMNS, _customer_features, load_customers
    return FEATURE_COLUMNS, _customer_features, load_customers


# ---- driver-side singleton (the model is loaded once, on the driver) ----
_MODEL = None


def _get_model(model_path: Path):
    global _MODEL
    if _MODEL is None:
        import xgboost as xgb
        model = xgb.XGBClassifier()
        model.load_model(str(model_path))
        _MODEL = model
    return _MODEL


class _StreamingScorer:
    """Driver-side accumulator + scorer used as the ``foreachBatch`` callback.

    Holds the full per-customer event history seen so far, recomputes the
    offline features over it on every micro-batch, and collects one scored
    row per input event (``emitted_rows``).
    """

    def __init__(self, model_path: Path, customers_path: Path,
                 threshold: float = DEFAULT_THRESHOLD):
        self._feature_columns, self._feature_fn, _load_customers = \
            _features_module()
        self._model = _get_model(Path(model_path))
        self._customers = _load_customers(Path(customers_path))
        self._threshold = threshold
        self._events: list[dict] = []
        self._amount_by_id: dict[str, float] = {}
        self._scored: set[str] = set()
        self._emitted: list[dict] = []
        self._stats = {"batches": 0, "rows_read_from_kafka": 0}

    def _to_event(self, row) -> dict:
        ev = {
            "event_id": row.event_id,
            "customer_id": row.customer_id,
            "event_type": row.event_type,
            "ts": datetime.fromtimestamp(row.ts_ms / 1000.0, tz=timezone.utc),
            "amount": float(row.amount),
            "lat": row.lat,
            "lon": row.lon,
            "device_id": row.device_id,
            "merchant_id": row.merchant_id,
            "payment_method": row.payment_method,
            "status": row.status,
        }
        self._amount_by_id[ev["event_id"]] = ev["amount"]
        return ev

    def _scored_rows(self) -> tuple[list[dict], list[list[float]]]:
        by_customer: dict[str, list[dict]] = defaultdict(list)
        for ev in self._events:
            by_customer[ev["customer_id"]].append(ev)
        for cust_events in by_customer.values():
            cust_events.sort(key=lambda e: e["ts"])

        rows: list[dict] = []
        for customer_id, cust_events in by_customer.items():
            for fr in self._feature_fn(
                    cust_events, self._customers.get(customer_id), set()):
                ts_ms = int(datetime.fromisoformat(
                    fr["event_time"]).timestamp() * 1000)
                rows.append({
                    "event_id": fr["event_id"],
                    "customer_id": fr["customer_id"],
                    "event_type": fr["event_type"],
                    "ts_ms": ts_ms,
                    "amount": float(self._amount_by_id[fr["event_id"]]),
                    "is_payment": int(fr["is_payment"]),
                    "is_failed": int(fr["is_failed"]),
                    "tx_count_120s": int(fr["tx_count_120s"]),
                    "failed_pay_count_60s": int(fr["failed_pay_count_60s"]),
                    "event_count_300s": int(fr["event_count_300s"]),
                    "seconds_since_prev_event": float(
                        fr["seconds_since_prev_event"]),
                    "is_new_device": float(fr["is_new_device"]),
                    "is_new_merchant": int(fr["is_new_merchant"]),
                    "dist_km_from_home": float(fr["dist_km_from_home"]),
                    "amount_vs_customer_avg": float(
                        fr["amount_vs_customer_avg"]),
                })
                # prediction needs the raw feature vector (feature order)
                rows[-1]["_features"] = [
                    fr[name] for name in self._feature_columns]
        return rows

    def score_batch(self, df: DataFrame, epoch_id: int) -> None:
        batch = df.collect()
        for row in batch:
            self._events.append(self._to_event(row))
        self._stats["batches"] += 1
        self._stats["rows_read_from_kafka"] += len(batch)

        rows = self._scored_rows()
        if not rows:
            return
        x = np.asarray([r.pop("_features") for r in rows], dtype="float32")
        proba = self._model.predict_proba(x)[:, 1]

        emitted: list[dict] = []
        for row, p in zip(rows, proba):
            if row["event_id"] in self._scored:
                continue
            self._scored.add(row["event_id"])
            row["fraud_probability"] = float(p)
            row["ml_prediction"] = 1 if float(p) >= self._threshold else 0
            emitted.append(row)
        self._emitted.extend(emitted)

    def emitted_rows(self) -> list[dict]:
        return list(self._emitted)

    def totals(self) -> dict:
        return {
            "batches": self._stats["batches"],
            "rows_read_from_kafka": self._stats["rows_read_from_kafka"],
            "rows_in_memory": len(self._scored),
        }


def ml_inference_stream(spark: SparkSession, tx_topic: str, pay_topic: str, *,
                        starting_offsets: str = "earliest") -> DataFrame:
    """Transactions + payments topics -> one normalized event stream.

    Every row carries ``event_id, customer_id, event_type, amount, status,
    lat, lon, merchant_id, payment_method, device_id, ts_ms`` (payments get
    null lat/lon/device_id, like the offline files).
    """
    tx = kafka_event_stream(spark, tx_topic, starting_offsets,
                            event_type="transactions")
    txp = tx.select(
        col("transaction_id").alias("event_id"),
        "customer_id",
        lit("transactions").alias("event_type"),
        "amount", "status", "lat", "lon", "merchant_id", "payment_method",
        "device_id",
        unix_millis(col("event_ts")).alias("ts_ms"),
    )
    pay = kafka_event_stream(spark, pay_topic, starting_offsets,
                             event_type="payments")
    payp = pay.select(
        col("payment_id").alias("event_id"),
        "customer_id",
        lit("payments").alias("event_type"),
        "amount", "status",
        lit(None).cast(DoubleType()).alias("lat"),
        lit(None).cast(DoubleType()).alias("lon"),
        "merchant_id", "payment_method",
        lit(None).cast(StringType()).alias("device_id"),
        unix_millis(col("event_ts")).alias("ts_ms"),
    )
    return txp.unionByName(payp)


def run_ml_inference(tx_topic: str, pay_topic: str, *,
                     model_path: Path = DEFAULT_MODEL,
                     customers_path: Path = DEFAULT_CUSTOMERS,
                     threshold: float = DEFAULT_THRESHOLD,
                     duration: int = 20, trigger: int = 5,
                     checkpoint: Path | None = None,
                     starting_offsets: str = "earliest",
                     validate=None) -> dict:
    """Run online ML scoring for ``duration`` seconds.

    Scores every incoming event in a driver-side ``foreachBatch`` callback.
    ``validate`` (optional) is called with a static DataFrame holding one
    scored row per input event, before the Spark session stops; its return
    value is stored under ``validation``.
    """
    model_path = Path(model_path)
    if not model_path.exists():
        raise FileNotFoundError(
            f"Phase 13 model not found at {model_path} - run 'python "
            f"run_phase13.py' first")

    spark = get_spark(f"phase14-ml-t{threshold}")
    combined = ml_inference_stream(spark, tx_topic, pay_topic,
                                   starting_offsets=starting_offsets)
    scorer = _StreamingScorer(model_path, customers_path, threshold)

    print(f"\nML-inference scored schema "
          f"(XGBoost from {model_path.name}, threshold {threshold}):")
    print("\n".join(f"  {f.name:<26} {f.dataType.typeName()}"
                    for f in OUTPUT_SCHEMA))

    checkpoint = checkpoint or (REPO_ROOT / "spark" / "checkpoints" / "phase14")
    query = (
        combined.writeStream.foreachBatch(scorer.score_batch)
        .trigger(processingTime=f"{trigger} seconds")
        .option("checkpointLocation", str(checkpoint))
        .start()
    )
    print(f"\n[phase14 streaming ML inference on '{tx_topic}' + '{pay_topic}']"
          f" running for {duration}s (trigger {trigger}s) ...")
    time.sleep(duration)
    query.stop()

    totals = scorer.totals()
    table = None
    if scorer.emitted_rows():
        table = spark.createDataFrame(
            [Row(**r) for r in scorer.emitted_rows()], OUTPUT_SCHEMA)
    if table is not None:
        totals["rows_in_memory"] = table.count()
    if validate is not None and table is not None:
        totals["validation"] = validate(table)
    spark.stop()

    totals.update({
        "threshold": threshold,
        "model_file": str(model_path),
        "tx_topic": tx_topic,
        "pay_topic": pay_topic,
    })
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 14: streaming ML inference with the offline model")
    parser.add_argument("--tx-topic", default="transactions")
    parser.add_argument("--pay-topic", default="payments")
    parser.add_argument("--model", default=str(DEFAULT_MODEL),
                        help="XGBoost model file (Phase 13 artifact)")
    parser.add_argument("--customers", default=str(DEFAULT_CUSTOMERS))
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--trigger", type=int, default=5)
    parser.add_argument("--starting-offsets", default="earliest",
                        choices=["earliest", "latest"])
    parser.add_argument("--checkpoint", default=str(
        REPO_ROOT / "spark/checkpoints/phase14"))
    args = parser.parse_args()

    totals = run_ml_inference(
        tx_topic=args.tx_topic,
        pay_topic=args.pay_topic,
        model_path=Path(args.model),
        customers_path=Path(args.customers),
        threshold=args.threshold,
        duration=args.duration,
        trigger=args.trigger,
        checkpoint=Path(args.checkpoint),
        starting_offsets=args.starting_offsets,
    )
    print("\nPhase 14 summary:")
    print(json.dumps(totals, indent=2))


if __name__ == "__main__":
    sys.exit(main())
