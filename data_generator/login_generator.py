"""Login / authentication event generator."""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from .utils import format_timestamp, random_ip, reference_now

FAILURE_REASONS = [
    "WRONG_PASSWORD", "ACCOUNT_LOCKED", "UNKNOWN_DEVICE",
    "TIMEOUT", "BRUTE_FORCE_BLOCK",
]


class LoginGenerator:
    def __init__(self, rng: random.Random, customers: list[dict], config: dict):
        self.rng = rng
        self.customers = customers
        self.config = config
        self._login_counter = 0
        self._device_counter = 0
        self._devices: dict[str, list[str]] = {}

    def new_login_id(self) -> str:
        self._login_counter += 1
        return f"LG{self._login_counter:05d}"

    def new_device_id(self) -> str:
        self._device_counter += 1
        return f"D{self._device_counter:04d}"

    def device_for(self, customer: dict) -> str:
        cid = customer["customer_id"]
        devices = self._devices.get(cid)
        if not devices:
            devices = [self.new_device_id() for _ in range(self.rng.randint(1, 2))]
            self._devices[cid] = devices
        return self.rng.choice(devices)

    def build_login(self, customer: dict, event_time: datetime,
                    device_id: str, success: bool,
                    failure_reason: str | None = None) -> dict:
        return {
            "login_id": self.new_login_id(),
            "customer_id": customer["customer_id"],
            "event_time": format_timestamp(event_time),
            "device_id": device_id,
            "ip_address": random_ip(self.rng),
            "success": success,
            "failure_reason": failure_reason,
        }

    def generate_normal(self, n: int) -> list[dict]:
        start = reference_now() - timedelta(hours=self.config["time_window_hours"])
        window_seconds = self.config["time_window_hours"] * 3600
        events = []
        for _ in range(n):
            customer = self.rng.choice(self.customers)
            event_time = start + timedelta(seconds=self.rng.uniform(0, window_seconds))
            success = self.rng.random() < 0.92
            failure_reason = None if success else self.rng.choice(FAILURE_REASONS)
            events.append(self.build_login(
                customer, event_time, self.device_for(customer), success, failure_reason,
            ))
        events.sort(key=lambda e: e["event_time"])
        return events
