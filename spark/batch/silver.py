"""Silver layer: conformed, deduplicated, enriched Delta tables.

Silver consumes Bronze and produces the analytics-ready tables:

- per event type: id-column dedupe, ``event_time`` parsed to a real
  ``event_ts`` timestamp, and enrichment joins against the reference tables
  (customers, merchants) and the fraud labels (``data/labels/labels.jsonl``).
- ``events``: one unified envelope across all four event types (transactions,
  logins, payments, customer_locations) with ``fraud_label``, ``fraud_scenario``
  and ``fraud_probability`` - the Phase 13 XGBoost model re-scored offline on
  the exact same behavior-driven features the streaming path uses (Phase 14).

The unified ``events`` table is the layer analytics and Gold read from.
"""

from __future__ import annotations

from pathlib import Path

import xgboost as xgb
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ml.features import FEATURE_COLUMNS, build_dataset
from ml.train import MODEL_PATH
from spark.batch.bronze import EVENT_SOURCES, LAKE_ROOT, RAW_DIR, read_delta
from spark.schemas import EVENT_TIME_FORMAT

LABELS_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("customer_id", StringType()),
    StructField("scenario", StringType()),
    StructField("alert_type", StringType()),
    StructField("risk_points", IntegerType()),
    StructField("label", IntegerType()),
])

UNIFIED_TYPES = {
    "event_id": StringType(),
    "event_type": StringType(),
    "customer_id": StringType(),
    "event_ts": TimestampType(),
    "amount": DoubleType(),
    "currency": StringType(),
    "merchant_id": StringType(),
    "payment_method": StringType(),
    "device_id": StringType(),
    "status": StringType(),
    "lat": DoubleType(),
    "lon": DoubleType(),
    "location": StringType(),
    "merchant_name": StringType(),
    "category": StringType(),
    "_ingestion_ts": TimestampType(),
    "fraud_label": IntegerType(),
    "fraud_scenario": StringType(),
    "alert_type": StringType(),
    "risk_points": IntegerType(),
    "fraud_probability": DoubleType(),
}


def _score_events() -> dict[str, float]:
    """Re-score every transaction/payment with the Phase 13 model.

    ``build_dataset`` reproduces the offline feature matrix (strictly-prior
    features, same leakage discipline as the streaming scorer), so the
    probability attached in Silver matches what Phase 14 emits online.
    """
    df = build_dataset()
    model = xgb.XGBClassifier()
    model.load_model(str(MODEL_PATH))
    probs = model.predict_proba(df[FEATURE_COLUMNS].astype("float32"))[:, 1]
    return {str(eid): float(p) for eid, p in zip(df["event_id"], probs)}


def _load_labels(spark: SparkSession) -> DataFrame:
    labels = spark.read.schema(LABELS_SCHEMA).json(
        str(RAW_DIR.parent / "labels" / "labels.jsonl"))
    return labels.select(
        F.col("event_id"),
        F.col("scenario").alias("fraud_scenario"),
        F.col("alert_type"),
        F.col("risk_points"),
        F.col("label").alias("fraud_label"),
    )


def _enrich(spark: SparkSession, df: DataFrame, id_col: str,
            event_type: str, prob_map: dict[str, float]) -> DataFrame:
    """Dedupe by id, parse event_ts, join labels + reference + ML score."""
    customers = read_delta(spark, LAKE_ROOT / "bronze" / "customers")
    merchants = read_delta(spark, LAKE_ROOT / "bronze" / "merchants")
    labels = _load_labels(spark)

    out = df.dropDuplicates([id_col])
    out = out.withColumn(
        "event_id", F.col(id_col)).drop(id_col)
    out = out.withColumn("event_type", F.lit(event_type))
    out = out.withColumn(
        "event_ts", F.to_timestamp(F.col("event_time"), EVENT_TIME_FORMAT))

    out = out.join(labels, on="event_id", how="left")
    out = out.withColumn("fraud_label",
                         F.coalesce(F.col("fraud_label"), F.lit(0)))

    if "merchant_id" in out.columns:
        out = out.join(
            merchants.select(
                F.col("merchant_id"),
                F.col("merchant_name"),
                F.col("category"),
                F.col("city").alias("merchant_city"),
                F.col("country").alias("merchant_country"),
            ),
            on="merchant_id", how="left")
    out = out.join(
        customers.select(
            F.col("customer_id"),
            F.col("age").alias("customer_age"),
            F.col("country").alias("customer_country"),
            F.col("currency").alias("customer_currency"),
            F.col("avg_transaction"),
            F.col("home_city"),
            F.col("home_lat"),
            F.col("home_lon"),
        ),
        on="customer_id", how="left")

    if prob_map:
        prob_pd = pd.DataFrame({
            "event_id": list(prob_map.keys()),
            "fraud_probability": list(prob_map.values()),
        })
        prob_df = spark.createDataFrame(
            prob_pd, schema="event_id string, fraud_probability double")
        out = out.join(prob_df, on="event_id", how="left")

    return out


def _unified_select(df: DataFrame) -> DataFrame:
    """Normalize one per-type Silver table into the unified events envelope."""
    select: list = []
    for col_name, col_type in UNIFIED_TYPES.items():
        source = col_name
        if col_name == "location" and "location" not in df.columns \
                and "city" in df.columns:
            source = "city"
        if source in df.columns:
            select.append(F.col(source).alias(col_name))
        else:
            select.append(F.lit(None).cast(col_type).alias(col_name))
    return df.select(select)


def build_silver(spark: SparkSession, lake_root: Path = LAKE_ROOT) -> dict:
    """Build the Silver layer from Bronze; return measured facts."""
    silver_root = lake_root / "silver"
    silver_root.mkdir(parents=True, exist_ok=True)

    prob_map = _score_events()
    summary: dict = {}
    per_type: list[DataFrame] = []

    for event_type, (_file, _schema, id_col) in EVENT_SOURCES.items():
        bronze = read_delta(spark, lake_root / "bronze" / event_type)
        silver = _enrich(spark, bronze, id_col, event_type, prob_map)
        out = silver_root / event_type
        silver.write.format("delta").mode("append").save(str(out))
        per_type.append(silver)
        summary[event_type] = {
            "rows": int(silver.count()),
            "path": str(out),
        }

    unified = _unified_select(per_type[0])
    for rest in per_type[1:]:
        unified = unified.unionByName(_unified_select(rest))
    unified = unified.select(list(UNIFIED_TYPES.keys())).cache()
    unified.count()  # materialize the cache once; all counts below reuse it
    unified_out = silver_root / "events"
    unified.write.format("delta").mode("append").save(str(unified_out))

    n_total = int(unified.count())
    n_distinct = int(unified.select("event_id").distinct().count())
    n_labeled = int(unified.filter(F.col("fraud_scenario").isNotNull()).count())
    n_scored = int(unified.filter(F.col("fraud_probability").isNotNull()).count())
    labeled_probs = unified.filter(F.col("fraud_scenario").isNotNull()) \
        .agg(F.avg("fraud_probability").alias("avg")).collect()[0]["avg"]

    summary["events"] = {
        "rows": n_total,
        "distinct_event_ids": n_distinct,
        "labeled": n_labeled,
        "scored": n_scored,
        "avg_labeled_probability":
            round(float(labeled_probs), 4) if labeled_probs is not None else None,
        "path": str(unified_out),
    }
    unified.unpersist()
    return summary
