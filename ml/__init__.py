"""ML layer.

Offline XGBoost on labeled synthetic events - engineered
features (amount vs customer avg, burst counts, home distance, device/merchant
novelty, ...) trained stratified 80/20 with measured precision/recall/F1,
ROC-AUC and PR-AUC on a held-out test set. Artifacts under ml/model/:
xgb_model.json + test_set.parquet + metadata.json + evaluation.json.
Phase 14 (next): load the model into the streaming app (online scoring of
incoming events with the same features).
"""

from __future__ import annotations
