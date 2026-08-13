"""Train the offline XGBoost fraud model.

Consumes ``ml/features.build_dataset()`` (one row per transaction/payment,
label 1 for injected fraud, features computed from strictly-prior events),
splits stratified 80/20, trains a binary XGBoost classifier and persists:

- ``ml/model/xgb_model.json``   the trained model
- ``ml/model/test_set.parquet`` the held-out test rows (for evaluation)
- ``ml/model/metadata.json``    features, params, class counts
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split

from .features import FEATURE_COLUMNS, build_dataset

MODEL_DIR = Path(__file__).resolve().parent / "model"
MODEL_PATH = MODEL_DIR / "xgb_model.json"
TEST_PATH = MODEL_DIR / "test_set.parquet"
METADATA_PATH = MODEL_DIR / "metadata.json"

DEFAULT_PARAMS = {
    "n_estimators": 400,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "min_child_weight": 1,
    "eval_metric": "aucpr",
    "random_state": 42,
    "n_jobs": 1,
}


def train(train_fraction: float = 0.8, seed: int = 42,
          params: dict | None = None) -> dict:
    """Train on the labeled dataset; save model + test set; return summary."""
    df = build_dataset()
    X = df[FEATURE_COLUMNS].astype("float32")
    y = df["label"].astype("int32")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, train_size=train_fraction, stratify=y, random_state=seed,
    )

    pos_train = int((y_train == 1).sum())
    neg_train = int((y_train == 0).sum())
    scale_pos_weight = neg_train / pos_train if pos_train else 1.0

    effective = dict(DEFAULT_PARAMS)
    if params:
        effective.update(params)
    effective["scale_pos_weight"] = scale_pos_weight

    model = xgb.XGBClassifier(**effective)
    model.fit(X_train, y_train)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model.save_model(str(MODEL_PATH))
    pd.DataFrame(X_test).assign(label=y_test.values).to_parquet(TEST_PATH)
    with METADATA_PATH.open("w", encoding="utf-8") as fh:
        json.dump({
            "features": FEATURE_COLUMNS,
            "model_file": str(MODEL_PATH),
            "test_file": str(TEST_PATH),
            "train_fraction": train_fraction,
            "seed": seed,
            "params": effective,
            "n_events": int(len(df)),
            "n_positive": int((df["label"] == 1).sum()),
            "n_negative": int((df["label"] == 0).sum()),
            "n_train": int(len(X_train)),
            "n_train_positive": pos_train,
            "n_test": int(len(X_test)),
            "n_test_positive": int((y_test == 1).sum()),
        }, fh, indent=2)

    return {
        "dataset_rows": int(len(df)),
        "dataset_positive": int((df["label"] == 1).sum()),
        "dataset_negative": int((df["label"] == 0).sum()),
        "train_rows": int(len(X_train)),
        "train_positive": pos_train,
        "train_negative": neg_train,
        "test_rows": int(len(X_test)),
        "test_positive": int((y_test == 1).sum()),
        "test_negative": int((y_test == 0).sum()),
        "scale_pos_weight": round(float(scale_pos_weight), 3),
        "model_file": str(MODEL_PATH),
        "test_file": str(TEST_PATH),
        "metadata_file": str(METADATA_PATH),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Phase 13 XGBoost model")
    parser.add_argument("--train-fraction", type=float, default=0.8)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    summary = train(train_fraction=args.train_fraction, seed=args.seed)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
