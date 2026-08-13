from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

REPO_ROOT = Path("/repo")
ARTIFACT_DIR = REPO_ROOT / "data" / "airflow"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_ARGS = {
    "owner": "fraud",
    "start_date": datetime(2026, 8, 1, tzinfo=timezone.utc),
    "retries": 0,
}


def _quality_raw() -> dict:
    from spark.batch.quality import check_raw_layer
    from spark.common import get_spark
    spark = get_spark("quality-raw", use_delta=True)
    try:
        return check_raw_layer(spark)
    finally:
        spark.stop()


def _quality_silver() -> dict:
    from spark.batch.quality import check_silver_layer
    from spark.common import get_spark
    spark = get_spark("quality-silver", use_delta=True)
    try:
        return check_silver_layer(spark)
    finally:
        spark.stop()


def _quality_gold() -> dict:
    from spark.batch.quality import check_gold_layer
    from spark.common import get_spark
    spark = get_spark("quality-gold", use_delta=True)
    try:
        return check_gold_layer(spark)
    finally:
        spark.stop()


def _publish_report(ti) -> dict:
    from spark.batch.quality import write_report
    layers = {
        "raw": ti.xcom_pull(task_ids="quality_raw"),
        "silver": ti.xcom_pull(task_ids="quality_silver"),
        "gold": ti.xcom_pull(task_ids="quality_gold"),
    }
    return write_report(ARTIFACT_DIR / "quality_report.json", layers)


with DAG(
    dag_id="fraud_quality_dag",
    description="Data-quality gates over the rebuilt Delta lake",
    schedule="@daily",
    start_date=DEFAULT_ARGS["start_date"],
    catchup=False,
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=False,
    tags=["phase18", "quality"],
) as dag:
    quality_raw = PythonOperator(
        task_id="quality_raw", python_callable=_quality_raw)
    quality_silver = PythonOperator(
        task_id="quality_silver", python_callable=_quality_silver)
    quality_gold = PythonOperator(
        task_id="quality_gold", python_callable=_quality_gold)
    publish_report = PythonOperator(
        task_id="publish_report", python_callable=_publish_report)

    [quality_raw, quality_silver, quality_gold] >> publish_report
