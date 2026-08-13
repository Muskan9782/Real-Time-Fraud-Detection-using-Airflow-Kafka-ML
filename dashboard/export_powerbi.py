"""Export Delta Gold tables into stable CSV tables for Power BI.

Power BI's standard file connectors do not consume a Delta table directory as
reliably as Spark does. This keeps Delta as the source of truth and publishes
small, refreshable presentation tables under ``data/dashboard``.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Allow the documented ``python dashboard/export_powerbi.py`` invocation to
# import the repository's existing ``spark`` package.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from spark.batch.bronze import LAKE_ROOT, read_delta
from spark.common import get_spark


def _write_csv(frame: DataFrame, output: Path, name: str) -> int:
    path = output / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    frame.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path))
    # Publish a stable filename instead of Spark's UUID-based part filename.
    parts = list(path.glob("part-*.csv"))
    if len(parts) != 1:
        raise RuntimeError(f"Expected one CSV part for {name}, found {len(parts)}")
    stable_file = path / f"{name}.csv"
    parts[0].replace(stable_file)
    for item in path.iterdir():
        if item == stable_file:
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    return int(frame.count())


def export_dashboard(lake_root: Path = LAKE_ROOT,
                     output: Path = Path("data/dashboard")) -> dict:
    """Create the Power BI presentation tables and return their row counts."""
    spark = get_spark("powerbi-gold-export", use_delta=True)
    try:
        gold = lake_root / "gold"
        alerts = read_delta(spark, gold / "fraud_events")
        customers = read_delta(spark, gold / "customer_risk_summary")

        tables = {
            "kpis": alerts.agg(
                F.count("*").alias("fraud_alerts"),
                F.countDistinct("customer_id").alias("risky_customers"),
                F.countDistinct("merchant_name").alias("affected_merchants"),
                F.round(F.avg("fraud_probability"), 4).alias("avg_fraud_probability"),
                F.round(F.avg("risk_points"), 2).alias("avg_risk_points"),
            ),
            "alert_details": alerts.select(
                "event_id", "event_type", "customer_id", "event_ts", "amount",
                "currency", "payment_method", "status", "location",
                "merchant_name", "category", "fraud_scenario", "alert_type",
                "risk_points", "fraud_probability",
            ).withColumn("alert_hour", F.date_trunc("hour", "event_ts")),
            "alerts_by_hour": alerts.withColumn(
                "alert_hour", F.date_trunc("hour", "event_ts")
            ).groupBy("alert_hour").agg(
                F.count("*").alias("alert_count"),
                F.round(F.sum("amount"), 2).alias("alert_amount"),
            ).orderBy("alert_hour"),
            "alerts_by_type": alerts.groupBy(
                "alert_type", "fraud_scenario"
            ).agg(
                F.count("*").alias("alert_count"),
                F.round(F.sum("amount"), 2).alias("alert_amount"),
                F.round(F.avg("fraud_probability"), 4).alias("avg_probability"),
            ).orderBy(F.desc("alert_count")),
            "top_risky_customers": customers.filter(
                F.col("n_fraud") > 0
            ).select(
                "customer_id", "country", "risk_level", "n_events", "n_fraud",
                "total_risk_points", "tx_amount_sum", "tx_amount_avg",
                "distinct_merchants", "distinct_devices", "first_event_ts",
                "last_event_ts",
            ).orderBy(F.desc("total_risk_points")),
        }
        counts = {name: _write_csv(frame, output, name)
                  for name, frame in tables.items()}
        manifest = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": str(gold),
            "tables": counts,
            "refresh_note": "Refresh the folder data source in Power BI after export.",
        }
        output.mkdir(parents=True, exist_ok=True)
        (output / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        return manifest
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-root", type=Path, default=LAKE_ROOT)
    parser.add_argument("--output", type=Path, default=Path("data/dashboard"))
    args = parser.parse_args()
    print(json.dumps(export_dashboard(args.lake_root, args.output), indent=2))


if __name__ == "__main__":
    main()
