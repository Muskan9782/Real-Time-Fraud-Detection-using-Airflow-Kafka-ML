"""Customer reference data (customers.csv).

Columns follow the spec's static data example:
    customer_id, age, country, avg_transaction, home_lat, home_lon
plus currency / home_city for realism.
"""

from __future__ import annotations

import random

from .geography import cities_by_country
from .utils import weighted_choice

CURRENCY_BY_COUNTRY = {
    "India": "INR",
    "United Kingdom": "GBP",
    "United States": "USD",
    "France": "EUR",
    "Germany": "EUR",
    "Singapore": "SGD",
    "United Arab Emirates": "AED",
    "Australia": "AUD",
    "Japan": "JPY",
    "China": "HKD",
    "Canada": "CAD",
}

# (country, weight) -- customer country distribution
COUNTRY_WEIGHTS = [
    ("India", 58),
    ("United Kingdom", 12),
    ("United States", 10),
    ("Singapore", 6),
    ("United Arab Emirates", 5),
    ("Australia", 4),
    ("France", 3),
    ("Germany", 2),
]

# Rough typical transaction size (currency units) per country
AVG_RANGE_BY_COUNTRY = {
    "India": (500, 30000),
    "United Kingdom": (80, 3000),
    "United States": (100, 4000),
    "France": (80, 2500),
    "Germany": (80, 2500),
    "Singapore": (100, 5000),
    "United Arab Emirates": (150, 6000),
    "Australia": (100, 4000),
}


class CustomerGenerator:
    def __init__(self, rng: random.Random, n: int):
        self.rng = rng
        self.n = n

    def generate(self) -> list[dict]:
        customers = []
        for i in range(1, self.n + 1):
            country = weighted_choice(self.rng, COUNTRY_WEIGHTS)
            city = self.rng.choice(cities_by_country(country))
            low, high = AVG_RANGE_BY_COUNTRY[country]
            raw = self.rng.uniform(low, high)
            if self.rng.random() < 0.10:  # occasional "big spender" tail
                raw *= self.rng.uniform(2.0, 5.0)
            avg = max(1, int(round(raw / 50) * 50))
            customers.append({
                "customer_id": f"C{i:03d}",
                "age": self.rng.randint(18, 75),
                "country": country,
                "currency": CURRENCY_BY_COUNTRY[country],
                "avg_transaction": avg,
                "home_city": city.name,
                "home_lat": round(city.lat, 4),
                "home_lon": round(city.lon, 4),
            })
        return customers
