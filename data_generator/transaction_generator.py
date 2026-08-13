"""Transaction event generator.

Produces normal (non-fraudulent) transactions and exposes low-level builders
used by the fraud-scenario engine, so that normal + injected events share
one ID sequence and one schema.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from .geography import cities_by_country
from .merchant_generator import pick_merchant
from .utils import format_timestamp, reference_now, weighted_choice

PAYMENT_METHODS = [
    ("CARD", 45),
    ("UPI", 25),
    ("WALLET", 15),
    ("NETBANKING", 10),
    ("PAY_LATER", 5),
]


class TransactionGenerator:
    def __init__(self, rng: random.Random, customers: list[dict],
                 merchants: list[dict], config: dict):
        self.rng = rng
        self.customers = customers
        self.merchants = merchants
        self.config = config
        self._tx_counter = 0
        self._device_counter = 0
        self._devices: dict[str, list[str]] = {}

    # -- ID helpers (shared so every transaction ID is unique) -----------
    def new_transaction_id(self) -> str:
        self._tx_counter += 1
        return f"TX{self._tx_counter:05d}"

    def new_device_id(self) -> str:
        """A brand-new device ID (not registered to any customer)."""
        self._device_counter += 1
        return f"D{self._device_counter:04d}"

    def device_for(self, customer: dict) -> str:
        """A known device for this customer (creates one on first use)."""
        cid = customer["customer_id"]
        devices = self._devices.get(cid)
        if not devices:
            devices = [self.new_device_id() for _ in range(self.rng.randint(1, 2))]
            self._devices[cid] = devices
        return self.rng.choice(devices)

    # -- value helpers ---------------------------------------------------
    def pick_merchant(self, customer: dict) -> dict:
        return pick_merchant(self.rng, self.merchants, customer)

    def sample_amount(self, customer: dict, multiplier: float = 1.0,
                      use_noise: bool = True) -> int:
        """Amount around ``customer.avg_transaction`` with log-normal noise.

        Set ``use_noise=False`` when the multiplier itself encodes a hard
        contract (e.g. high-value anomaly must be strictly > 5x average).
        """
        base = customer["avg_transaction"]
        noise = self.rng.lognormvariate(0.0, 0.35) if use_noise else 1.0
        amount = base * multiplier * noise
        return max(1, round(amount))

    def location_for(self, customer: dict) -> dict:
        """Mostly home; occasionally another city in the same country."""
        if self.rng.random() < 0.8:
            return {
                "city": customer["home_city"],
                "lat": customer["home_lat"],
                "lon": customer["home_lon"],
            }
        city = self.rng.choice(cities_by_country(customer["country"]))
        return {"city": city.name, "lat": round(city.lat, 4), "lon": round(city.lon, 4)}

    # -- event builder ---------------------------------------------------
    def build_transaction(self, customer: dict, event_time: datetime,
                          amount: int, merchant: dict, location: dict,
                          device_id: str, status: str = "SUCCESS") -> dict:
        return {
            "transaction_id": self.new_transaction_id(),
            "customer_id": customer["customer_id"],
            "event_time": format_timestamp(event_time),
            "amount": amount,
            "currency": customer["currency"],
            "merchant_id": merchant["merchant_id"],
            "payment_method": weighted_choice(self.rng, PAYMENT_METHODS),
            "location": location["city"],
            "lat": location["lat"],
            "lon": location["lon"],
            "device_id": device_id,
            "status": status,
        }

    # -- bulk generation -------------------------------------------------
    def generate_normal(self, n: int) -> list[dict]:
        start = reference_now() - timedelta(hours=self.config["time_window_hours"])
        window_seconds = self.config["time_window_hours"] * 3600
        events = []
        for _ in range(n):
            customer = self.rng.choice(self.customers)
            amount = self.sample_amount(customer, self.rng.uniform(0.3, 1.5))
            merchant = self.pick_merchant(customer)
            location = self.location_for(customer)
            event_time = start + timedelta(seconds=self.rng.uniform(0, window_seconds))
            events.append(self.build_transaction(
                customer, event_time, amount, merchant, location,
                self.device_for(customer),
            ))
        events.sort(key=lambda e: e["event_time"])
        return events
