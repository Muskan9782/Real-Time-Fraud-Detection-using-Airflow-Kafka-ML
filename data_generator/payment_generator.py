"""Payment attempt / failure event generator.

Most normal payments succeed; a minority fail for realistic reasons. The
card-testing attack scenario relies on a burst of FAILED payments inside 60 seconds.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from .merchant_generator import pick_merchant
from .utils import format_timestamp, reference_now, weighted_choice

PAYMENT_METHODS = [
    ("CARD", 60),
    ("UPI", 25),
    ("WALLET", 15),
]

FAILURE_REASONS = [
    "INSUFFICIENT_FUNDS", "CARD_DECLINED", "EXPIRED_CARD",
    "RISK_BLOCK", "3DS_FAILED", "LIMIT_EXCEEDED",
]


class PaymentGenerator:
    def __init__(self, rng: random.Random, customers: list[dict],
                 merchants: list[dict], config: dict):
        self.rng = rng
        self.customers = customers
        self.merchants = merchants
        self.config = config
        self._payment_counter = 0

    def new_payment_id(self) -> str:
        self._payment_counter += 1
        return f"PAY{self._payment_counter:05d}"

    def pick_merchant(self, customer: dict) -> dict:
        return pick_merchant(self.rng, self.merchants, customer)

    def sample_amount(self, customer: dict, multiplier: float = 1.0) -> int:
        base = customer["avg_transaction"]
        amount = base * multiplier * self.rng.lognormvariate(0.0, 0.4)
        return max(1, round(amount))

    def build_payment(self, customer: dict, event_time: datetime, amount: int,
                      merchant: dict, status: str,
                      failure_reason: str | None = None,
                      transaction_id: str | None = None,
                      payment_method: str | None = None) -> dict:
        return {
            "payment_id": self.new_payment_id(),
            "customer_id": customer["customer_id"],
            "event_time": format_timestamp(event_time),
            "transaction_id": transaction_id,
            "amount": amount,
            "currency": customer["currency"],
            "merchant_id": merchant["merchant_id"],
            "payment_method": payment_method or weighted_choice(self.rng, PAYMENT_METHODS),
            "status": status,
            "failure_reason": failure_reason,
        }

    def generate_normal(self, n: int, transactions: list[dict] | None = None) -> list[dict]:
        start = reference_now() - timedelta(hours=self.config["time_window_hours"])
        window_seconds = self.config["time_window_hours"] * 3600
        tx_by_customer: dict[str, list[str]] = {}
        if transactions:
            for tx in transactions:
                tx_by_customer.setdefault(tx["customer_id"], []).append(tx["transaction_id"])

        events = []
        for _ in range(n):
            customer = self.rng.choice(self.customers)
            event_time = start + timedelta(seconds=self.rng.uniform(0, window_seconds))
            success = self.rng.random() < 0.85
            status = "SUCCESS" if success else "FAILED"
            failure_reason = None if success else self.rng.choice(FAILURE_REASONS)
            transaction_id = None
            if success and transactions and self.rng.random() < 0.3:
                ids = tx_by_customer.get(customer["customer_id"])
                if ids:
                    transaction_id = self.rng.choice(ids)
            events.append(self.build_payment(
                customer, event_time,
                self.sample_amount(customer, self.rng.uniform(0.3, 1.5)),
                self.pick_merchant(customer),
                status, failure_reason, transaction_id,
            ))
        events.sort(key=lambda e: e["event_time"])
        return events
