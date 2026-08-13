"""Merchant reference data (merchants.csv).

Also exposes ``pick_merchant`` so every generator uses the same merchant
selection logic (weighted towards the customer's own country).
"""

from __future__ import annotations

import random

from .geography import CITIES

NAME_POOL = [
    "Global", "Prime", "Metro", "Bright", "Swift", "Aurora",
    "Peak", "Horizon", "Vertex", "Summit", "Quantum", "Nova",
]

CATEGORIES = [
    "RETAIL", "GROCERY", "FOOD", "DIGITAL", "TRAVEL", "ELECTRONICS",
    "FASHION", "FUEL", "ENTERTAINMENT", "PHARMACY", "UTILITY", "TRANSFER",
]


def pick_merchant(rng: random.Random, merchants: list[dict], customer: dict,
                  same_country_prob: float = 0.7) -> dict:
    """Prefer merchants in the customer's country; fall back to any merchant."""
    same_country = [m for m in merchants if m["country"] == customer["country"]]
    if same_country and rng.random() < same_country_prob:
        return rng.choice(same_country)
    return rng.choice(merchants)


class MerchantGenerator:
    def __init__(self, rng: random.Random, n: int):
        self.rng = rng
        self.n = n

    def generate(self) -> list[dict]:
        merchants = []
        for i in range(1, self.n + 1):
            city = self.rng.choice(CITIES)
            category = self.rng.choice(CATEGORIES)
            merchants.append({
                "merchant_id": f"M{i:03d}",
                "merchant_name": f"{self.rng.choice(NAME_POOL)} {city.name}",
                "category": category,
                "country": city.country,
                "city": city.name,
                "lat": round(city.lat, 4),
                "lon": round(city.lon, 4),
            })
        return merchants
