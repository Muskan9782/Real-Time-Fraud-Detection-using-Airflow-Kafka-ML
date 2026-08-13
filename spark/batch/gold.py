"""Gold layer: analytics-ready aggregated Delta tables.

Gold is the presentation layer - pre-aggregated facts a dashboard or analyst
queries directly. Built from the unified Silver ``events`` table:

- ``customer_risk_summary``  one row per customer (left-joined from the
  reference table, so customers with zero events still appear): event mix,
  fraud counts, summed risk points and the risk band
  (CRITICAL >= 76 / HIGH >= 51 / MEDIUM >= 26 / LOW).
- ``merchant_fraud_summary`` one row per merchant: event/fraud counts,
  fraud rate, total and fraud amounts, distinct customers.
- ``fraud_events``          the curated fraud fact table: every labeled event
  with context (amount, merchant, category, scenario, risk points) plus the
  model probability.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.batch.bronze import LAKE_ROOT, read_delta

_COUNT_COLS = [
    "n_events", "n_transactions", "n_payments", "n_logins", "n_locations",
    "n_fraud", "total_risk_points", "tx_amount_sum", "distinct_merchants",
    "distinct_devices",
]
_MERCHANT_COUNT_COLS = [
    "n_events", "n_transactions", "n_payments", "n_fraud", "total_amount",
    "fraud_amount", "n_customers",
]


def build_gold(spark: SparkSession, lake_root: Path = LAKE_ROOT) -> dict:
    """Build the Gold layer from Silver; return measured facts."""
    gold_root = lake_root / "gold"
    gold_root.mkdir(parents=True, exist_ok=True)

    events = read_delta(spark, lake_root / "silver" / "events")
    customers = read_delta(spark, lake_root / "bronze" / "customers")
    merchants = read_delta(spark, lake_root / "bronze" / "merchants")

    is_tx = F.col("event_type") == "transactions"
    is_pay = F.col("event_type") == "payments"
    is_login = F.col("event_type") == "logins"
    is_loc = F.col("event_type") == "customer_locations"
    is_fraud = F.col("fraud_label") == 1

    # --- customer_risk_summary -------------------------------------------------
    agg = events.groupBy("customer_id").agg(
        F.count("*").alias("n_events"),
        F.sum(F.when(is_tx, 1).otherwise(0)).alias("n_transactions"),
        F.sum(F.when(is_pay, 1).otherwise(0)).alias("n_payments"),
        F.sum(F.when(is_login, 1).otherwise(0)).alias("n_logins"),
        F.sum(F.when(is_loc, 1).otherwise(0)).alias("n_locations"),
        F.sum("fraud_label").alias("n_fraud"),
        F.sum(F.when(F.col("risk_points").isNotNull(),
                     F.col("risk_points")).otherwise(0)) \
            .alias("total_risk_points"),
        F.max("risk_points").alias("max_risk_points"),
        F.sum(F.when(is_tx, F.col("amount")).otherwise(0)).alias("tx_amount_sum"),
        F.avg(F.when(is_tx, F.col("amount"))).alias("tx_amount_avg"),
        F.countDistinct("merchant_id").alias("distinct_merchants"),
        F.countDistinct("device_id").alias("distinct_devices"),
        F.min("event_ts").alias("first_event_ts"),
        F.max("event_ts").alias("last_event_ts"),
    )
    risk_level = F.when(F.col("total_risk_points") >= 76, "CRITICAL") \
        .when(F.col("total_risk_points") >= 51, "HIGH") \
        .when(F.col("total_risk_points") >= 26, "MEDIUM") \
        .otherwise("LOW")
    customer_summary = customers.select(
        "customer_id", "age", "country", "currency", "avg_transaction",
        "home_city") \
        .join(agg, on="customer_id", how="left") \
        .fillna(0, subset=_COUNT_COLS) \
        .withColumn("risk_level", risk_level) \
        .orderBy(F.desc("total_risk_points"), "customer_id")
    customer_out = gold_root / "customer_risk_summary"
    customer_summary.write.format("delta").mode("append").save(str(customer_out))

    # --- merchant_fraud_summary ------------------------------------------------
    merchant_agg = events.filter(F.col("merchant_id").isNotNull()) \
        .groupBy("merchant_id").agg(
            F.count("*").alias("n_events"),
            F.sum(F.when(is_tx, 1).otherwise(0)).alias("n_transactions"),
            F.sum(F.when(is_pay, 1).otherwise(0)).alias("n_payments"),
            F.sum("fraud_label").alias("n_fraud"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(is_fraud, F.col("amount")).otherwise(0))
                .alias("fraud_amount"),
            F.countDistinct("customer_id").alias("n_customers"),
        )
    merchant_summary = merchants.select(
        "merchant_id", "merchant_name", "category", "country", "city") \
        .join(merchant_agg, on="merchant_id", how="left") \
        .fillna(0, subset=_MERCHANT_COUNT_COLS) \
        .withColumn("fraud_rate",
                    F.when(F.col("n_events") > 0,
                           F.round(F.col("n_fraud") / F.col("n_events"), 4))
                     .otherwise(F.lit(0.0))) \
        .orderBy(F.desc("n_fraud"), "merchant_id")
    merchant_out = gold_root / "merchant_fraud_summary"
    merchant_summary.write.format("delta").mode("append").save(str(merchant_out))

    # --- fraud_events ----------------------------------------------------------
    fraud_events = events.filter(is_fraud).select(
        "event_id", "event_type", "customer_id", "event_ts", "amount",
        "currency", "payment_method", "status", "location", "merchant_name",
        "category", "fraud_scenario", "alert_type", "risk_points",
        "fraud_probability",
    ).orderBy("event_ts", "event_id")
    fraud_out = gold_root / "fraud_events"
    fraud_events.write.format("delta").mode("append").save(str(fraud_out))

    return {
        "customer_risk_summary": {
            "rows": int(customer_summary.count()),
            "path": str(customer_out),
            "with_fraud": int(customer_summary.filter(
                F.col("n_fraud") > 0).count()),
            "with_events": int(customer_summary.filter(
                F.col("n_events") > 0).count()),
            "levels": {r["risk_level"]: int(r["count"]) for r in
                       customer_summary.groupBy("risk_level").count()
                       .collect()},
        },
        "merchant_fraud_summary": {
            "rows": int(merchant_summary.count()),
            "with_fraud": int(merchant_summary.filter(
                F.col("n_fraud") > 0).count()),
            "path": str(merchant_out),
        },
        "fraud_events": {
            "rows": int(fraud_events.count()),
            "path": str(fraud_out),
        },
    }
