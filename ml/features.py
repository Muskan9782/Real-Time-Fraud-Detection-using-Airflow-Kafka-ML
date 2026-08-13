"""Build the labeled offline dataset and engineered features.

The raw event files (``data/raw/transactions.jsonl``,
``data/raw/payments.jsonl``) carry no labels; the fraud labels live in
``data/labels/labels.jsonl`` (only the injected fraud events, label=1).
This module unifies both event types, marks every other event label=0, and
computes per-event features.

Leakage discipline: every temporal / novelty feature is computed from events
that happened *strictly before* the current event for the same customer. The
current event is added to the per-customer history *after* its features are
computed, exactly as an online scorer would see it.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from data_generator.config import DATA_DIR, LABELS_DIR, RAW_DIR

FEATURE_COLUMNS = [
    "is_payment",
    "is_failed",
    "amount_log",
    "amount_vs_customer_avg",
    "hour",
    "dow",
    "dist_km_from_home",
    "tx_count_120s",
    "failed_pay_count_60s",
    "event_count_300s",
    "seconds_since_prev_event",
    "is_new_device",
    "is_new_merchant",
    "payment_method_is_card",
]

# Trailing windows used by the detectors (config/phase1.json) - feature
# counters mirror them so the model learns the same burst signals.
TX_WINDOW_S = 120
FAIL_WINDOW_S = 60
EVENT_WINDOW_S = 300


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres between two lat/lon points."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def load_customers(path: Path = RAW_DIR / "customers.csv") -> dict[str, dict]:
    """customer_id -> {avg_transaction, home_lat, home_lon}."""
    out: dict[str, dict] = {}
    with path.open(encoding="utf-8") as fh:
        header = fh.readline().strip().split(",")
        for line in fh:
            if not line.strip():
                continue
            vals = dict(zip(header, line.strip().split(",")))
            out[vals["customer_id"]] = {
                "avg_transaction": float(vals["avg_transaction"]),
                "home_lat": float(vals["home_lat"]),
                "home_lon": float(vals["home_lon"]),
            }
    return out


def load_labels(path: Path = LABELS_DIR / "labels.jsonl") -> set[str]:
    with path.open(encoding="utf-8") as fh:
        return {json.loads(line)["event_id"] for line in fh}


def _load_event_file(path: Path, event_type: str) -> list[dict]:
    events: list[dict] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            raw = json.loads(line)
            id_field = "transaction_id" if event_type == "transactions" else "payment_id"
            events.append({
                "event_id": raw[id_field],
                "customer_id": raw["customer_id"],
                "event_type": event_type,
                "ts": datetime.fromisoformat(raw["event_time"]),
                "amount": float(raw["amount"]),
                "lat": raw.get("lat"),
                "lon": raw.get("lon"),
                "device_id": raw.get("device_id"),
                "merchant_id": raw.get("merchant_id"),
                "payment_method": raw.get("payment_method"),
                "status": raw.get("status"),
            })
    return events


def _customer_features(events: list[dict], customer: dict | None,
                       labels: set[str]) -> list[dict]:
    """Compute features for one customer's events (already time-sorted)."""
    rows: list[dict] = []
    tx_history: deque = deque()
    fail_history: deque = deque()
    event_history: deque = deque()
    seen_devices: set = set()
    seen_merchants: set = set()

    avg = customer["avg_transaction"] if customer else None
    home = (customer["home_lat"], customer["home_lon"]) if customer else None

    for ev in events:
        ts: datetime = ev["ts"]
        # 1) expire history that is now outside its window (strictly prior only)
        while tx_history and (ts - tx_history[0]).total_seconds() > TX_WINDOW_S:
            tx_history.popleft()
        while fail_history and (ts - fail_history[0]).total_seconds() > FAIL_WINDOW_S:
            fail_history.popleft()
        while event_history and (ts - event_history[0]).total_seconds() > EVENT_WINDOW_S:
            event_history.popleft()

        # 2) features from the *prior* state only
        is_failed = 1 if ev["status"] == "FAILED" else 0
        amount_ratio = (ev["amount"] / avg) if avg else np.nan
        dist = np.nan
        if ev["lat"] is not None and ev["lon"] is not None and home is not None:
            dist = haversine_km(ev["lat"], ev["lon"], home[0], home[1])
        prev_ts = event_history[-1] if event_history else None
        gap = (ts - prev_ts).total_seconds() if prev_ts else np.nan

        rows.append({
            "event_id": ev["event_id"],
            "customer_id": ev["customer_id"],
            "event_type": ev["event_type"],
            "event_time": ts.isoformat(),
            "label": 1 if ev["event_id"] in labels else 0,
            "is_payment": 1 if ev["event_type"] == "payments" else 0,
            "is_failed": is_failed,
            "amount_log": math.log1p(ev["amount"]),
            "amount_vs_customer_avg": amount_ratio,
            "hour": ts.hour,
            "dow": ts.weekday(),
            "dist_km_from_home": dist,
            "tx_count_120s": len(tx_history),
            "failed_pay_count_60s": len(fail_history),
            "event_count_300s": len(event_history),
            "seconds_since_prev_event": gap,
            "is_new_device": (0 if ev["device_id"] in seen_devices else 1)
                             if ev["device_id"] else np.nan,
            "is_new_merchant": 0 if ev["merchant_id"] in seen_merchants else 1,
            "payment_method_is_card": 1 if ev["payment_method"] == "CARD" else 0,
        })

        # 3) add the current event to history (after its features were read)
        if ev["event_type"] == "transactions":
            tx_history.append(ts)
        if is_failed:
            fail_history.append(ts)
        event_history.append(ts)
        if ev["device_id"]:
            seen_devices.add(ev["device_id"])
        if ev["merchant_id"]:
            seen_merchants.add(ev["merchant_id"])

    return rows


def build_dataset(raw_dir: Path = RAW_DIR,
                  labels_path: Path = LABELS_DIR / "labels.jsonl") -> pd.DataFrame:
    """Return the labeled feature matrix (one row per transaction/payment)."""
    events = (
        _load_event_file(raw_dir / "transactions.jsonl", "transactions")
        + _load_event_file(raw_dir / "payments.jsonl", "payments")
    )
    by_customer: dict[str, list[dict]] = defaultdict(list)
    for ev in events:
        by_customer[ev["customer_id"]].append(ev)
    for cust_events in by_customer.values():
        cust_events.sort(key=lambda e: e["ts"])

    customers = load_customers(raw_dir / "customers.csv")
    labels = load_labels(labels_path)

    rows: list[dict] = []
    for customer_id, cust_events in by_customer.items():
        rows.extend(_customer_features(cust_events, customers.get(customer_id), labels))

    df = pd.DataFrame(rows)
    df["event_time"] = pd.to_datetime(df["event_time"])
    return df.sort_values(["customer_id", "event_time"]).reset_index(drop=True)


def main() -> None:
    df = build_dataset()
    pos = int((df["label"] == 1).sum())
    neg = int((df["label"] == 0).sum())
    print(f"dataset rows={len(df)} positive={pos} negative={neg}")
    print(f"features={len(FEATURE_COLUMNS)} columns={FEATURE_COLUMNS}")


if __name__ == "__main__":
    main()
