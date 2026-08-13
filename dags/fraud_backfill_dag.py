from __future__ import annotations

import json
import shutil
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


def _clear_lake() -> dict:
    from spark.batch.bronze import LAKE_ROOT
    if LAKE_ROOT.exists():
        shutil.rmtree(LAKE_ROOT)
    LAKE_ROOT.mkdir(parents=True, exist_ok=True)
    return {"lake_root": str(LAKE_ROOT), "cleared": True}


def _rebuild_bronze() -> dict:
    from spark.batch.bronze import build_bronze
    from spark.common import get_spark
    spark = get_spark("backfill-bronze", use_delta=True)
    try:
        return build_bronze(spark)
    finally:
        spark.stop()


def _rebuild_silver() -> dict:
    from spark.batch.silver import build_silver
    from spark.common import get_spark
    spark = get_spark("backfill-silver", use_delta=True)
    try:
        return build_silver(spark)
    finally:
        spark.stop()


def _rebuild_gold() -> dict:
    from spark.batch.gold import build_gold
    from spark.common import get_spark
    spark = get_spark("backfill-gold", use_delta=True)
    try:
        return build_gold(spark)
    finally:
        spark.stop()


def _write_manifest(ti) -> dict:
    bronze = ti.xcom_pull(task_ids="rebuild_bronze")
    silver = ti.xcom_pull(task_ids="rebuild_silver")
    gold = ti.xcom_pull(task_ids="rebuild_gold")
    ev = silver["events"]
    ok = (
        all(v["rows"] > 0 for v in bronze.values())
        and ev["rows"] == 3060
        and ev["distinct_event_ids"] == ev["rows"]
        and ev["labeled"] == 230
        and ev["scored"] == 1660
        and gold["customer_risk_summary"]["rows"] == 200
        and gold["merchant_fraud_summary"]["rows"] == 50
        and gold["fraud_events"]["rows"] == 230
    )
    manifest = {
        "ok": ok,
        "backfilled_at": datetime.now(timezone.utc).isoformat(),
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
    }
    (ARTIFACT_DIR / "backfill_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


with DAG(
    dag_id="fraud_backfill_dag",
    description="Rebuild the Delta lake (Bronze/Silver/Gold) from raw data",
    schedule="@daily",
    start_date=DEFAULT_ARGS["start_date"],
    catchup=False,
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=False,
    tags=["phase18", "batch", "delta"],
) as dag:
    clear_lake = PythonOperator(
        task_id="clear_lake", python_callable=_clear_lake)
    rebuild_bronze = PythonOperator(
        task_id="rebuild_bronze", python_callable=_rebuild_bronze)
    rebuild_silver = PythonOperator(
        task_id="rebuild_silver", python_callable=_rebuild_silver)
    rebuild_gold = PythonOperator(
        task_id="rebuild_gold", python_callable=_rebuild_gold)
    write_manifest = PythonOperator(
        task_id="write_manifest", python_callable=_write_manifest)

    clear_lake >> rebuild_bronze >> rebuild_silver >> rebuild_gold \
        >> write_manifest
