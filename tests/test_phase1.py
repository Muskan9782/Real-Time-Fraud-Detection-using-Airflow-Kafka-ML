"""Phase 1 (raw data creation) milestone tests.

Run with (stdlib only, no third-party install needed)::

    python -m unittest discover -s tests -v

These validate the Phase 1 milestone:
  * customers.csv / merchants.csv exist with the expected columns
  * >= 1,000 normal transactions are generated
  * every event matches its schema and references real IDs
  * all five fraud scenarios are injected with behavior-driven labels
    (velocity burst, impossible travel, login->transaction gap,
     card-testing burst, high-value anomaly)
"""

from __future__ import annotations

import csv
import json
import shutil
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_generator.config import load_config
from data_generator.geography import haversine_km
from data_generator.pipeline import generate_phase1
from data_generator.utils import parse_timestamp

REQUIRED_KEYS = {
    "transactions": {
        "transaction_id", "customer_id", "event_time", "amount", "currency",
        "merchant_id", "payment_method", "location", "device_id", "status",
    },
    "logins": {
        "login_id", "customer_id", "event_time", "device_id", "ip_address",
        "success", "failure_reason",
    },
    "payments": {
        "payment_id", "customer_id", "event_time", "transaction_id", "amount",
        "currency", "merchant_id", "payment_method", "status", "failure_reason",
    },
    "customer_locations": {
        "location_id", "customer_id", "event_time", "city", "lat", "lon",
        "device_id",
    },
}

ID_FIELD = {
    "transactions": "transaction_id",
    "logins": "login_id",
    "payments": "payment_id",
    "customer_locations": "location_id",
}

EXPECTED_ALERTS = {
    "HIGH_TRANSACTION_VELOCITY",
    "IMPOSSIBLE_TRAVEL",
    "LOGIN_TRANSACTION_CORRELATION",
    "CARD_TESTING_ATTACK",
    "HIGH_VALUE_ANOMALY",
}


def load_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def load_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class Phase1PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = load_config()
        cls.tmp = Path(tempfile.mkdtemp(prefix="phase1_test_"))
        cls.summary = generate_phase1(config=cls.config, out_dir=cls.tmp)
        raw = cls.tmp / "raw"
        labels = cls.tmp / "labels"
        cls.customers = load_csv(raw / "customers.csv")
        cls.merchants = load_csv(raw / "merchants.csv")
        cls.transactions = load_jsonl(raw / "transactions.jsonl")
        cls.logins = load_jsonl(raw / "logins.jsonl")
        cls.payments = load_jsonl(raw / "payments.jsonl")
        cls.locations = load_jsonl(raw / "customer_locations.jsonl")
        cls.label_rows = load_jsonl(labels / "labels.jsonl")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    # --------------------------------------------------------------- volumes
    def test_normal_transaction_volume(self):
        self.assertGreaterEqual(self.summary["counts"]["transactions_normal"], 1000)

    def test_reference_files(self):
        self.assertEqual(len(self.customers), self.config["n_customers"])
        self.assertEqual(len(self.merchants), self.config["n_merchants"])

    # --------------------------------------------------------------- schemas
    def test_customer_fields(self):
        required = {"customer_id", "age", "country", "avg_transaction", "home_lat", "home_lon"}
        for row in self.customers:
            self.assertTrue(required.issubset(row.keys()), row)
            self.assertGreater(int(row["age"]), 0)

    def test_merchant_fields(self):
        required = {"merchant_id", "merchant_name", "category", "country", "city"}
        for row in self.merchants:
            self.assertTrue(required.issubset(row.keys()), row)

    def test_event_schemas(self):
        all_events = {
            "transactions": self.transactions,
            "logins": self.logins,
            "payments": self.payments,
            "customer_locations": self.locations,
        }
        for event_type, events in all_events.items():
            self.assertTrue(events, f"no {event_type} events generated")
            for event in events:
                self.assertTrue(REQUIRED_KEYS[event_type].issubset(event.keys()), event)
                parse_timestamp(event["event_time"])  # must parse
                if event_type in ("transactions", "payments"):
                    self.assertGreater(float(event["amount"]), 0)

    def test_unique_ids(self):
        for event_type, events in (
            ("transactions", self.transactions),
            ("logins", self.logins),
            ("payments", self.payments),
            ("customer_locations", self.locations),
        ):
            field = ID_FIELD[event_type]
            ids = [e[field] for e in events]
            self.assertEqual(len(ids), len(set(ids)), f"duplicate {field}")

    # ---------------------------------------------------------- referential
    def test_references_resolve(self):
        customer_ids = {c["customer_id"] for c in self.customers}
        merchant_ids = {m["merchant_id"] for m in self.merchants}
        currency_by_customer = {c["customer_id"]: c["currency"] for c in self.customers}

        for tx in self.transactions:
            self.assertIn(tx["customer_id"], customer_ids)
            self.assertIn(tx["merchant_id"], merchant_ids)
            self.assertEqual(tx["currency"], currency_by_customer[tx["customer_id"]])

        for login in self.logins:
            self.assertIn(login["customer_id"], customer_ids)

        for pay in self.payments:
            self.assertIn(pay["customer_id"], customer_ids)
            self.assertIn(pay["merchant_id"], merchant_ids)

        for loc in self.locations:
            self.assertIn(loc["customer_id"], customer_ids)

    # ---------------------------------------------------------------- labels
    def test_all_alert_types_present(self):
        present = {row["alert_type"] for row in self.label_rows}
        self.assertEqual(present, EXPECTED_ALERTS)

    def test_label_volumes(self):
        counts = self.summary["labels_by_scenario"]
        expected = {
            f"{alert} ({scenario})": self.config["scenario_counts"][scenario]
            for scenario, alert in (
                ("velocity", "HIGH_TRANSACTION_VELOCITY"),
                ("impossible_travel", "IMPOSSIBLE_TRAVEL"),
                ("login_transaction", "LOGIN_TRANSACTION_CORRELATION"),
                ("payment_attack", "CARD_TESTING_ATTACK"),
                ("high_value", "HIGH_VALUE_ANOMALY"),
            )
        }
        for key, expected_count in expected.items():
            self.assertGreaterEqual(counts.get(key, 0), expected_count, key)

    def test_labels_reference_real_events(self):
        id_sets = {
            event_type: {e[ID_FIELD[event_type]] for e in events}
            for event_type, events in (
                ("transactions", self.transactions),
                ("logins", self.logins),
                ("payments", self.payments),
                ("customer_locations", self.locations),
            )
        }
        self.assertEqual(len(self.label_rows), len({r["event_id"] for r in self.label_rows}))
        for row in self.label_rows:
            self.assertIn(row["event_id"], id_sets[row["event_type"]])
            self.assertEqual(row["label"], 1)

    # ------------------------------------------------------------- scenarios
    def test_velocity_scenario(self):
        groups: dict[str, list[str]] = {}
        for row in self.label_rows:
            if row["alert_type"] == "HIGH_TRANSACTION_VELOCITY":
                groups.setdefault(row["customer_id"], []).append(row["event_id"])
        self.assertTrue(groups)
        by_id = {t["transaction_id"]: t for t in self.transactions}
        for ids in groups.values():
            times = sorted(parse_timestamp(by_id[i]["event_time"]) for i in ids)
            self.assertGreaterEqual(len(times), 6)
            self.assertLessEqual((times[-1] - times[0]).total_seconds(), 120)

    def test_impossible_travel_scenario(self):
        by_customer: dict[str, list[dict]] = {}
        for tx in self.transactions:
            by_customer.setdefault(tx["customer_id"], []).append(tx)
        by_id = {t["transaction_id"]: t for t in self.transactions}

        checked = 0
        for row in self.label_rows:
            if row["alert_type"] != "IMPOSSIBLE_TRAVEL":
                continue
            tx2 = by_id[row["event_id"]]
            t2 = parse_timestamp(tx2["event_time"])
            earlier = [
                tx for tx in by_customer[tx2["customer_id"]]
                if parse_timestamp(tx["event_time"]) < t2
            ]
            match = None
            for tx1 in earlier:
                gap = (t2 - parse_timestamp(tx1["event_time"])).total_seconds()
                if 0 < gap <= 20 * 60:
                    match = tx1
            self.assertIsNotNone(match, f"{row['event_id']}: no prior transaction")
            distance = haversine_km(
                float(tx2["lat"]), float(tx2["lon"]),
                float(match["lat"]), float(match["lon"]),
            )
            gap = (t2 - parse_timestamp(match["event_time"])).total_seconds()
            speed = distance / (gap / 3600.0)
            self.assertGreater(speed, 800.0, f"speed {speed:.0f} km/h")
            checked += 1
        self.assertGreaterEqual(checked, self.config["scenario_counts"]["impossible_travel"])

    def test_login_transaction_scenario(self):
        logins_by_customer: dict[str, list[dict]] = {}
        for login in self.logins:
            logins_by_customer.setdefault(login["customer_id"], []).append(login)
        by_id = {t["transaction_id"]: t for t in self.transactions}

        checked = 0
        for row in self.label_rows:
            if row["alert_type"] != "LOGIN_TRANSACTION_CORRELATION":
                continue
            tx = by_id[row["event_id"]]
            t_tx = parse_timestamp(tx["event_time"])
            login_times = [
                parse_timestamp(login["event_time"])
                for login in logins_by_customer.get(tx["customer_id"], [])
                if login["success"]
            ]
            matched = any(
                lt <= t_tx <= lt + timedelta(minutes=5) for lt in login_times
            )
            self.assertTrue(matched, f"{row['event_id']}: no login within 5 min")
            checked += 1
        self.assertGreaterEqual(checked, self.config["scenario_counts"]["login_transaction"])

    def test_payment_attack_scenario(self):
        groups: dict[str, list[str]] = {}
        for row in self.label_rows:
            if row["alert_type"] == "CARD_TESTING_ATTACK":
                groups.setdefault(row["customer_id"], []).append(row["event_id"])
        self.assertTrue(groups)
        by_id = {p["payment_id"]: p for p in self.payments}
        for ids in groups.values():
            payments = [by_id[i] for i in ids]
            self.assertGreaterEqual(len(payments), 11)
            self.assertTrue(all(p["status"] == "FAILED" for p in payments))
            times = sorted(parse_timestamp(p["event_time"]) for p in payments)
            self.assertLessEqual((times[-1] - times[0]).total_seconds(), 60)

    def test_high_value_scenario(self):
        avg_by_customer = {c["customer_id"]: float(c["avg_transaction"]) for c in self.customers}
        by_id = {t["transaction_id"]: t for t in self.transactions}
        checked = 0
        for row in self.label_rows:
            if row["alert_type"] != "HIGH_VALUE_ANOMALY":
                continue
            tx = by_id[row["event_id"]]
            self.assertGreater(
                float(tx["amount"]), 5.0 * avg_by_customer[tx["customer_id"]]
            )
            checked += 1
        self.assertGreaterEqual(checked, self.config["scenario_counts"]["high_value"])


if __name__ == "__main__":
    unittest.main()
