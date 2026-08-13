"""Customer location ping generator (customer_locations).

Normal movement is kept physically plausible: customers mostly stay at home,
occasionally move to another city in the same country, and never travel
faster than ``MAX_TRAVEL_SPEED_KMH`` (so only the injected impossible-travel
scenario breaks the speed rule).
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from .geography import City, cities_by_country, haversine_km
from .utils import format_timestamp, reference_now

MAX_TRAVEL_SPEED_KMH = 800.0


def _city_from(customer: dict) -> City:
    return City(customer["home_city"], customer["country"],
                customer["home_lat"], customer["home_lon"])


class LocationGenerator:
    def __init__(self, rng: random.Random, customers: list[dict], config: dict):
        self.rng = rng
        self.customers = customers
        self.config = config
        self._location_counter = 0
        self._device_counter = 0
        self._devices: dict[str, list[str]] = {}
        self._state: dict[str, dict] = {}

    def new_location_id(self) -> str:
        self._location_counter += 1
        return f"LOC{self._location_counter:05d}"

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

    def build_location(self, customer: dict, event_time: datetime,
                       city: City, device_id: str) -> dict:
        return {
            "location_id": self.new_location_id(),
            "customer_id": customer["customer_id"],
            "event_time": format_timestamp(event_time),
            "city": city.name,
            "lat": round(city.lat, 4),
            "lon": round(city.lon, 4),
            "device_id": device_id,
        }

    def _gap_seconds(self, prev_city: City, new_city: City) -> int:
        """Time needed to move between cities without exceeding max speed."""
        if prev_city.name == new_city.name:
            return self.rng.randint(600, 7200)
        distance = haversine_km(prev_city.lat, prev_city.lon, new_city.lat, new_city.lon)
        min_hours = distance / MAX_TRAVEL_SPEED_KMH
        return max(900, int(min_hours * 3600 * self.rng.uniform(0.9, 1.4)))

    def generate_normal(self, n: int) -> list[dict]:
        start = reference_now() - timedelta(hours=self.config["time_window_hours"])
        end = reference_now()
        window_seconds = self.config["time_window_hours"] * 3600
        events = []
        for _ in range(n):
            customer = self.rng.choice(self.customers)
            cid = customer["customer_id"]
            state = self._state.get(cid)
            if state is None:
                state = {"city": _city_from(customer), "last_time": None}
                self._state[cid] = state

            city = state["city"]
            if state["last_time"] is None:
                event_time = start + timedelta(seconds=self.rng.uniform(0, window_seconds * 0.9))
            else:
                if self.rng.random() < 0.15:
                    candidates = [
                        c for c in cities_by_country(customer["country"])
                        if c.name != city.name
                    ]
                    if candidates and self.rng.random() < 0.5:
                        city = self.rng.choice(candidates)
                event_time = state["last_time"] + timedelta(
                    seconds=self._gap_seconds(state["city"], city)
                )

            if not (start <= event_time < end):
                continue

            events.append(self.build_location(customer, event_time, city, self.device_for(customer)))
            state["city"] = city
            state["last_time"] = event_time

        events.sort(key=lambda e: e["event_time"])
        return events
