"""End-to-end pipeline.

What it produces (default output root: ``<project>/data``)::

    data/
    ├── raw/
    │   ├── customers.csv            static customer reference data
    │   ├── merchants.csv            static merchant reference data
    │   ├── transactions.jsonl       transaction events (normal + injected)
    │   ├── logins.jsonl             login events
    │   ├── payments.jsonl           payment attempt events
    │   └── customer_locations.jsonl location ping events
    └── labels/
        ├── labels.jsonl             event_id -> injected fraud scenario
        └── summary.json             counts + seed (the run manifest)

Raw event files contain *clean* events (no label leakage). Labels live in a
sidecar file so later phases can use them for ML training while the streaming
pipeline stays label-free, exactly like the real production feeds would be.

Milestone (spec section 1): Python generates valid normal and fraudulent
events.
"""

from __future__ import annotations

import csv
import json
import random
from pathlib import Path

from .config import LABELS_DIR, RAW_DIR, load_config
from .customer_generator import CustomerGenerator
from .fraud_scenarios import FraudScenarioEngine
from .login_generator import LoginGenerator
from .location_generator import LocationGenerator
from .merchant_generator import MerchantGenerator
from .payment_generator import PaymentGenerator
from .schemas import CUSTOMER_FIELDS, EVENT_SCHEMAS, MERCHANT_FIELDS
from .transaction_generator import TransactionGenerator

# engine-internal key -> schema / output-file key
ENGINE_TO_SCHEMA = {
    "transactions": "transactions",
    "logins": "logins",
    "payments": "payments",
    "locations": "customer_locations",
}


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def generate_phase1(config: dict | None = None, out_dir: str | Path | None = None) -> dict:
    """Generate all Phase 1 data and return a summary manifest."""
    config = config or load_config()
    rng = random.Random(config["seed"])

    # 1. Static reference data
    customers = CustomerGenerator(rng, config["n_customers"]).generate()
    merchants = MerchantGenerator(rng, config["n_merchants"]).generate()

    # 2. Generators (they share ID sequences, so all IDs stay unique)
    tx_gen = TransactionGenerator(rng, customers, merchants, config)
    login_gen = LoginGenerator(rng, customers, config)
    pay_gen = PaymentGenerator(rng, customers, merchants, config)
    loc_gen = LocationGenerator(rng, customers, config)
    generators = {
        "transactions": tx_gen,
        "logins": login_gen,
        "payments": pay_gen,
        "locations": loc_gen,
    }

    # 3. Normal (non-fraudulent) streams
    normal_tx = tx_gen.generate_normal(config["n_normal_transactions"])
    normal_logins = login_gen.generate_normal(config["n_normal_logins"])
    normal_payments = pay_gen.generate_normal(config["n_normal_payments"], transactions=normal_tx)
    normal_locations = loc_gen.generate_normal(config["n_normal_locations"])

    # 4. Inject the five fraud scenarios (labels created here, not at random)
    engine = FraudScenarioEngine(generators, customers, merchants, rng, config)
    fraud_events, labels = engine.inject_all()

    # 5. Merge and time-order every stream
    normal = {
        "transactions": normal_tx,
        "logins": normal_logins,
        "payments": normal_payments,
        "locations": normal_locations,
    }
    events_by_type: dict[str, list[dict]] = {
        schema_key: normal[engine_key] + fraud_events[engine_key]
        for engine_key, schema_key in ENGINE_TO_SCHEMA.items()
    }
    for event_list in events_by_type.values():
        event_list.sort(key=lambda e: e["event_time"])

    # 6. Write outputs
    raw_dir = Path(out_dir) / "raw" if out_dir else RAW_DIR
    labels_dir = Path(out_dir) / "labels" if out_dir else LABELS_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(raw_dir / "customers.csv", CUSTOMER_FIELDS, customers)
    _write_csv(raw_dir / "merchants.csv", MERCHANT_FIELDS, merchants)
    for event_type, fields in EVENT_SCHEMAS.items():
        _write_jsonl(raw_dir / f"{event_type}.jsonl", events_by_type[event_type])

    labels.sort(key=lambda lbl: lbl["event_id"])
    _write_jsonl(labels_dir / "labels.jsonl", labels)

    # 7. Summary manifest
    label_counts: dict[str, int] = {}
    for lbl in labels:
        key = f"{lbl['alert_type']} ({lbl['scenario']})"
        label_counts[key] = label_counts.get(key, 0) + 1

    summary = {
        "phase": 1,
        "seed": config["seed"],
        "counts": {
            "customers": len(customers),
            "merchants": len(merchants),
            "transactions_normal": len(normal_tx),
            "transactions_fraud": len(fraud_events["transactions"]),
            "transactions_total": len(events_by_type["transactions"]),
            "logins": len(events_by_type["logins"]),
            "payments": len(events_by_type["payments"]),
            "locations": len(events_by_type["customer_locations"]),
            "labels_total": len(labels),
        },
        "labels_by_scenario": dict(sorted(label_counts.items())),
        "output_dir": str(raw_dir.parent),
    }
    with (labels_dir / "summary.json").open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    return summary
