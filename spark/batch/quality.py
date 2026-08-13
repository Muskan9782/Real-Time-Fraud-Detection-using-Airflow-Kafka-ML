"""Quality module: data-quality checks over the Delta lake.

The quality DAG (``dags/fraud_quality_dag.py``) runs these checks against the
lake the backfill DAG just rebuilt (``data/lake``). Each ``check_*`` function
returns ``{"checks": {name: bool}, "context": {measured facts}}``; the DAG
aggregates them into ``data/airflow/quality_report.json`` and the milestone
runner asserts the whole report.

The thresholds mirror ``run_phase15.py``: Bronze row counts match the raw
files exactly, Silver is one unified / unique / labeled / scored ``events``
table, and the Gold aggregates match the known data shape (200 customers,
50 merchants, 230 labeled fraud events).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.batch.bronze import (
    EVENT_SOURCES,
    LAKE_ROOT,
    RAW_DIR,
    delta_count,
    read_delta,
)

LABELS_FILE = RAW_DIR.parent / "labels" / "labels.jsonl"


def _count_lines(path: Path, header: bool = False) -> int:
    n = sum(1 for _ in path.open(encoding="utf-8"))
    return max(0, n - (1 if header else 0))


def check_raw_layer(spark: SparkSession, lake_root: Path = LAKE_ROOT,
                    raw_dir: Path = RAW_DIR) -> dict:
    """Bronze row counts must match the raw source files exactly."""
    checks: dict[str, bool] = {}
    context: dict = {}
    for name, (file, _schema, _id) in EVENT_SOURCES.items():
        expected = _count_lines(raw_dir / file)
        got = delta_count(spark, lake_root / "bronze" / name)
        context[f"bronze {name}"] = got
        checks[f"bronze {name} == raw ({expected})"] = got == expected
    for name, file in (("customers", "customers.csv"),
                       ("merchants", "merchants.csv")):
        expected = _count_lines(raw_dir / file, header=True)
        got = delta_count(spark, lake_root / "bronze" / name)
        context[f"bronze {name}"] = got
        checks[f"bronze {name} == raw ({expected})"] = got == expected
    return {"checks": checks, "context": context}


def check_silver_layer(spark: SparkSession,
                       lake_root: Path = LAKE_ROOT) -> dict:
    """Silver events must be unified, unique, labeled and scored."""
    events = read_delta(spark, lake_root / "silver" / "events").cache()
    n_total = int(events.count())
    n_distinct = int(events.select("event_id").distinct().count())
    n_labeled = int(events.filter(F.col("fraud_scenario").isNotNull()).count())
    n_scored = int(events.filter(F.col("fraud_probability").isNotNull()).count())
    n_null_ts = int(events.filter(F.col("event_ts").isNull()).count())
    labeled_by_type = {r["event_type"]: int(r["count"]) for r in
                       events.filter(F.col("fraud_scenario").isNotNull())
                       .groupBy("event_type").count().collect()}
    events.unpersist()

    label_types: dict[str, int] = {}
    n_labels = 0
    for line in LABELS_FILE.open(encoding="utf-8"):
        et = json.loads(line)["event_type"]
        label_types[et] = label_types.get(et, 0) + 1
        n_labels += 1

    checks = {
        "silver events == 3060 (1111+549+610+790)": n_total == 3060,
        "silver event_ids are unique": n_distinct == n_total,
        "silver event_ts parsed for every event": n_null_ts == 0,
        f"silver labeled == labels.jsonl ({n_labels})": n_labeled == n_labels,
        "silver scored == transactions+payments (1660)": n_scored == 1660,
    }
    for et, n in label_types.items():
        checks[f"silver {et} labels == labels.jsonl ({n})"] = \
            labeled_by_type.get(et, 0) == n
    return {"checks": checks, "context": {
        "events": n_total,
        "distinct_event_ids": n_distinct,
        "labeled": n_labeled,
        "scored": n_scored,
        "labeled_by_type": labeled_by_type,
    }}


def check_gold_layer(spark: SparkSession,
                     lake_root: Path = LAKE_ROOT) -> dict:
    """Gold aggregates must match the known data shape."""
    crs = read_delta(spark, lake_root / "gold" / "customer_risk_summary")
    mfs = read_delta(spark, lake_root / "gold" / "merchant_fraud_summary")
    fev = read_delta(spark, lake_root / "gold" / "fraud_events")
    n_crs = int(crs.count())
    n_fraud_sum = int(crs.agg(F.sum("n_fraud")).collect()[0][0])
    n_mfs = int(mfs.count())
    n_fev = int(fev.count())
    return {"checks": {
        "gold customer_risk_summary == 200": n_crs == 200,
        "gold customer_risk_summary fraud sum == 230": n_fraud_sum == 230,
        "gold merchant_fraud_summary == 50": n_mfs == 50,
        "gold fraud_events == 230": n_fev == 230,
    }, "context": {
        "customer_risk_summary": n_crs,
        "merchant_fraud_summary": n_mfs,
        "fraud_events": n_fev,
        "customer_fraud_events": n_fraud_sum,
    }}


def write_report(report_path: Path, layers: dict[str, dict]) -> dict:
    """Combine per-layer results and persist data/airflow/quality_report.json."""
    checks = {f"{layer}.{name}": value
              for layer, result in layers.items()
              for name, value in result["checks"].items()}
    report = {
        "ok": all(checks.values()),
        "checks": checks,
        "context": {layer: result["context"]
                    for layer, result in layers.items()},
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
