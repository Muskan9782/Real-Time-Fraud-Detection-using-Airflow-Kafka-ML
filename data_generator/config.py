"""Defaults live here. A JSON file at ``config/phase1.json`` (repo root) can
override any of these values. Use ``load_config()`` everywhere so that a
single source of truth is used by the generators, the fraud engine, the
CLI runner and the tests.
"""

from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LABELS_DIR = DATA_DIR / "labels"
CONFIG_FILE = PROJECT_ROOT / "config" / "phase1.json"

DEFAULTS: dict = {
    # Determinism
    "seed": 42,
    # Reference data sizes
    "n_customers": 200,
    "n_merchants": 50,
    # Normal (non-fraudulent) event volumes
    "n_normal_transactions": 1000,
    "n_normal_logins": 600,
    "n_normal_payments": 400,
    "n_normal_locations": 800,
    # Events are spread across the trailing time window (hours)
    "time_window_hours": 24,
    # Number of injected fraud scenario instances (deliberately small,
    # never randomly generated labels -- behavior is what triggers them)
    "scenario_counts": {
        "velocity": 10,
        "impossible_travel": 10,
        "login_transaction": 10,
        "payment_attack": 10,
        "high_value": 10,
    },
    # Detection thresholds (see project spec section 7)
    "velocity_window_seconds": 120,            # 2 minutes
    "velocity_max_transactions": 5,            # flag when count > 5
    "payment_attack_window_seconds": 60,       # 60 seconds
    "payment_attack_max_failures": 10,         # flag when failures > 10
    "login_transaction_max_gap_seconds": 300,  # 5 minutes
    "impossible_travel_speed_kmh": 800.0,      # max plausible travel speed
    "high_value_multiplier": 5.0,              # flag when amount > 5x average
    # Risk-engine settings (see project spec section 9)
    "risk_window_seconds": 300,                # combine alerts within 5 minutes
    "risk_levels": {                           # total_points -> level bands
        "CRITICAL": 76,
        "HIGH": 51,
        "MEDIUM": 26,
        "LOW": 0,
    },
    # Risk-engine points (see project spec section 9)
    "risk_points": {
        "HIGH_TRANSACTION_VELOCITY": 25,
        "IMPOSSIBLE_TRAVEL": 30,
        "LOGIN_TRANSACTION_CORRELATION": 20,
        "CARD_TESTING_ATTACK": 30,
        "HIGH_VALUE_ANOMALY": 25,
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge ``override`` into a copy of ``base``."""
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_config(path: str | Path | None = None) -> dict:
    """Return the effective configuration (defaults + optional JSON overrides)."""
    path = Path(path) if path else CONFIG_FILE
    cfg = dict(DEFAULTS)
    if path.exists():
        with path.open(encoding="utf-8") as fh:
            cfg = _deep_merge(cfg, json.load(fh))
    return cfg
