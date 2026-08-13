"""Evaluate the trained XGBoost model on the held-out test set.

Loads ``ml/model/xgb_model.json`` + ``ml/model/test_set.parquet`` (both
produced by ``ml/train.py``) and reports precision / recall / F1 at the 0.5
decision threshold plus ROC-AUC and PR-AUC from predicted probabilities. The
metrics are written to ``ml/model/evaluation.json`` for the milestone runner.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from .features import FEATURE_COLUMNS
from .train import MODEL_PATH, TEST_PATH

MODEL_DIR = Path(__file__).resolve().parent / "model"
EVALUATION_PATH = MODEL_DIR / "evaluation.json"


def evaluate(threshold: float = 0.5) -> dict:
    """Score the saved model on the held-out test set; return metrics."""
    model = xgb.XGBClassifier()
    model.load_model(str(MODEL_PATH))

    test = pd.read_parquet(TEST_PATH)
    X_test = test[FEATURE_COLUMNS].astype("float32")
    y_test = test["label"].astype("int32").values

    proba = model.predict_proba(X_test)[:, 1]
    pred = (proba >= threshold).astype(int)

    tn, fp, fn, tp = confusion_matrix(y_test, pred).ravel()
    metrics = {
        "threshold": threshold,
        "n_test": int(len(y_test)),
        "n_positive": int((y_test == 1).sum()),
        "n_negative": int((y_test == 0).sum()),
        "tp": int(tp),
        "fp": int(fp),
        "fn": int(fn),
        "tn": int(tn),
        "precision": float(precision_score(y_test, pred)),
        "recall": float(recall_score(y_test, pred)),
        "f1": float(f1_score(y_test, pred)),
        "roc_auc": float(roc_auc_score(y_test, proba)),
        "pr_auc": float(average_precision_score(y_test, proba)),
    }

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with EVALUATION_PATH.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Phase 13 model")
    parser.add_argument("--threshold", type=float, default=0.5)
    args = parser.parse_args()
    metrics = evaluate(threshold=args.threshold)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
