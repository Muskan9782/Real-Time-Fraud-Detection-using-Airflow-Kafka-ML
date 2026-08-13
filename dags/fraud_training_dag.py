from __future__ import annotations

import json
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


def _train() -> dict:
    from ml.train import train
    return train(train_fraction=0.8, seed=42)


def _evaluate() -> dict:
    from ml.evaluate import evaluate
    return evaluate(threshold=0.5)


def _publish(ti) -> dict:
    train_summary = ti.xcom_pull(task_ids="train_model")
    metrics = ti.xcom_pull(task_ids="evaluate_model")
    report = {
        "train": train_summary,
        "evaluation": metrics,
        "model_file": str(train_summary["model_file"]),
        "metadata_file": str(train_summary["metadata_file"]),
        "ok": metrics["roc_auc"] > 0.9 and metrics["f1"] > 0.8,
    }
    (ARTIFACT_DIR / "training_metrics.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8")
    return report


with DAG(
    dag_id="fraud_training_dag",
    description="Retrain + evaluate the Phase 13 XGBoost fraud model",
    schedule="@daily",
    start_date=DEFAULT_ARGS["start_date"],
    catchup=False,
    default_args=DEFAULT_ARGS,
    is_paused_upon_creation=False,
    tags=["phase18", "ml"],
) as dag:
    train_model = PythonOperator(
        task_id="train_model", python_callable=_train)
    evaluate_model = PythonOperator(
        task_id="evaluate_model", python_callable=_evaluate)
    publish_metrics = PythonOperator(
        task_id="publish_metrics", python_callable=_publish)

    train_model >> evaluate_model >> publish_metrics
