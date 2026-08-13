"""Fraud scenario injection engine (Phase 1 core).

The generators create *normal* behavior; this engine deliberately injects
the five fraud behaviors from the project spec. Every injected
event gets a label (in ``data/labels/labels.jsonl``) so XGBoost
training has a clean, behavior-driven positive class -- labels are never
assigned by randomly flipping rows.

Scenarios:
    velocity          HIGH_TRANSACTION_VELOCITY     >5 tx / 2 min        (+25)
    impossible_travel IMPOSSIBLE_TRAVEL             travel speed > 800km/h(+30)
    login_transaction LOGIN_TRANSACTION_CORRELATION tx within 5m of login(+20)
    payment_attack    CARD_TESTING_ATTACK           >10 failed pay/60s   (+30)
    high_value        HIGH_VALUE_ANOMALY            amount > 5x average  (+25)
"""

from __future__ import annotations

import random
from datetime import timedelta

from .geography import City, cities_by_country, far_city
from .utils import reference_now


class FraudScenarioEngine:
    def __init__(self, generators: dict, customers: list[dict],
                 merchants: list[dict], rng: random.Random, config: dict):
        self.generators = generators
        self.customers = customers
        self.merchants = merchants
        self.rng = rng
        self.config = config
        self.points = config["risk_points"]

        # Use distinct customers across scenarios so labels never overlap.
        total_scenarios = sum(config["scenario_counts"].values())
        pool_size = min(len(customers), total_scenarios)
        self._customer_pool = list(self.rng.sample(self.customers, pool_size))

        self._start = reference_now() - timedelta(hours=config["time_window_hours"])
        self._window_seconds = config["time_window_hours"] * 3600

    # ------------------------------------------------------------------ helpers
    def _next_customer(self) -> dict:
        if self._customer_pool:
            return self._customer_pool.pop()
        return self.rng.choice(self.customers)

    def _anchor(self):
        """A random event-time within the generation window."""
        return self._start + timedelta(seconds=self.rng.uniform(0, self._window_seconds))

    def _label(self, event_id: str, event_type: str, customer_id: str,
               scenario: str, alert_type: str) -> dict:
        return {
            "event_id": event_id,
            "event_type": event_type,
            "customer_id": customer_id,
            "scenario": scenario,
            "alert_type": alert_type,
            "risk_points": self.points[alert_type],
            "label": 1,
        }

    # ------------------------------------------------------------------ scenarios
    def inject_velocity(self):
        """Burst of 6-8 transactions for one customer inside 2 minutes."""
        tx_gen = self.generators["transactions"]
        events, labels = [], []
        count = self.config["scenario_counts"]["velocity"]
        for _ in range(count):
            customer = self._next_customer()
            anchor = self._anchor()
            burst = self.rng.randint(self.config["velocity_max_transactions"] + 1, 8)
            device = tx_gen.device_for(customer)
            for i in range(burst):
                event_time = anchor + timedelta(seconds=i * self.rng.randint(5, 15))
                tx = tx_gen.build_transaction(
                    customer, event_time,
                    tx_gen.sample_amount(customer, self.rng.uniform(0.5, 3.0)),
                    tx_gen.pick_merchant(customer),
                    tx_gen.location_for(customer),
                    device,
                )
                events.append(tx)
                labels.append(self._label(
                    tx["transaction_id"], "transactions", customer["customer_id"],
                    "velocity", "HIGH_TRANSACTION_VELOCITY",
                ))
        return {"transactions": events}, labels

    def inject_impossible_travel(self):
        """Two transactions from locations so far apart that the implied
        travel speed wildly exceeds any plausible value (Bangalore->London)."""
        tx_gen = self.generators["transactions"]
        events, labels = [], []
        count = self.config["scenario_counts"]["impossible_travel"]
        for _ in range(count):
            customer = self._next_customer()
            home = City(customer["home_city"], customer["country"],
                        customer["home_lat"], customer["home_lon"])
            origin = self.rng.choice(cities_by_country(customer["country"])) \
                if self.rng.random() < 0.5 else home
            destination = far_city(origin, min_distance_km=3000.0, rng=self.rng)

            t1 = self._anchor()
            t2 = t1 + timedelta(seconds=self.rng.randint(180, 900))  # 3-15 min

            tx1 = tx_gen.build_transaction(
                customer, t1,
                tx_gen.sample_amount(customer, self.rng.uniform(0.5, 1.5)),
                tx_gen.pick_merchant(customer),
                {"city": origin.name, "lat": round(origin.lat, 4), "lon": round(origin.lon, 4)},
                tx_gen.device_for(customer),
            )
            tx2 = tx_gen.build_transaction(
                customer, t2,
                tx_gen.sample_amount(customer, self.rng.uniform(2.0, 5.0)),
                tx_gen.pick_merchant(customer),
                {"city": destination.name, "lat": round(destination.lat, 4), "lon": round(destination.lon, 4)},
                tx_gen.device_for(customer),
            )
            events.extend([tx1, tx2])
            labels.append(self._label(
                tx2["transaction_id"], "transactions", customer["customer_id"],
                "impossible_travel", "IMPOSSIBLE_TRAVEL",
            ))
        return {"transactions": events}, labels

    def inject_login_transaction(self):
        """A (successful) login from an unfamiliar device, then a high-value
        transaction from that same device within 5 minutes."""
        tx_gen = self.generators["transactions"]
        login_gen = self.generators["logins"]
        transactions, logins, labels = [], [], []
        max_gap = self.config["login_transaction_max_gap_seconds"]
        count = self.config["scenario_counts"]["login_transaction"]
        for _ in range(count):
            customer = self._next_customer()
            anchor = self._anchor()
            gap = self.rng.randint(60, max_gap - 60)
            device = tx_gen.new_device_id()

            login = login_gen.build_login(customer, anchor, device, True)
            tx = tx_gen.build_transaction(
                customer, anchor + timedelta(seconds=gap),
                tx_gen.sample_amount(customer, self.rng.uniform(2.0, 6.0)),
                tx_gen.pick_merchant(customer),
                tx_gen.location_for(customer),
                device,
            )
            logins.append(login)
            transactions.append(tx)
            labels.append(self._label(
                tx["transaction_id"], "transactions", customer["customer_id"],
                "login_transaction", "LOGIN_TRANSACTION_CORRELATION",
            ))
        return {"logins": logins, "transactions": transactions}, labels

    def inject_payment_attack(self):
        """Card-testing: 11-15 failed payments for one card inside 60 seconds."""
        pay_gen = self.generators["payments"]
        events, labels = [], []
        max_failures = self.config["payment_attack_max_failures"]
        count = self.config["scenario_counts"]["payment_attack"]
        for _ in range(count):
            customer = self._next_customer()
            anchor = self._anchor()

            # a couple of legitimate successful payments as context
            for i in range(2):
                ctx_time = anchor - timedelta(minutes=10 + i)
                events.append(pay_gen.build_payment(
                    customer, ctx_time,
                    pay_gen.sample_amount(customer, 1.0),
                    pay_gen.pick_merchant(customer), "SUCCESS",
                ))

            failures = self.rng.randint(max_failures + 1, 15)
            for i in range(failures):
                event_time = anchor + timedelta(seconds=i * self.rng.randint(1, 4))
                amount = max(1, round(customer["avg_transaction"] * self.rng.uniform(0.01, 0.06)))
                pay = pay_gen.build_payment(
                    customer, event_time, amount,
                    pay_gen.pick_merchant(customer), "FAILED", "CARD_DECLINED",
                )
                events.append(pay)
                labels.append(self._label(
                    pay["payment_id"], "payments", customer["customer_id"],
                    "payment_attack", "CARD_TESTING_ATTACK",
                ))
        return {"payments": events}, labels

    def inject_high_value(self):
        """Single transaction with amount > 5x the customer's historical average."""
        tx_gen = self.generators["transactions"]
        events, labels = [], []
        count = self.config["scenario_counts"]["high_value"]
        for _ in range(count):
            customer = self._next_customer()
            multiplier = self.rng.uniform(
                self.config["high_value_multiplier"] + 0.5, 15.0
            )
            tx = tx_gen.build_transaction(
                customer, self._anchor(),
                tx_gen.sample_amount(customer, multiplier, use_noise=False),
                tx_gen.pick_merchant(customer),
                tx_gen.location_for(customer),
                tx_gen.device_for(customer),
            )
            events.append(tx)
            labels.append(self._label(
                tx["transaction_id"], "transactions", customer["customer_id"],
                "high_value", "HIGH_VALUE_ANOMALY",
            ))
        return {"transactions": events}, labels

    # ------------------------------------------------------------------ runner
    def inject_all(self):
        """Run every scenario; return (events_by_type, labels)."""
        merged: dict[str, list[dict]] = {
            "transactions": [], "logins": [], "payments": [], "locations": [],
        }
        labels: list[dict] = []
        for method in (
            self.inject_velocity,
            self.inject_impossible_travel,
            self.inject_login_transaction,
            self.inject_payment_attack,
            self.inject_high_value,
        ):
            events_by_type, lbls = method()
            for event_type, events in events_by_type.items():
                merged[event_type].extend(events)
            labels.extend(lbls)
        return merged, labels
