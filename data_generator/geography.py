"""Geographic reference data and distance helpers.

Used to make synthetic events geographically realistic and to compute the
impossible-travel fraud scenario (Haversine distance -> implied speed).
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    name: str
    country: str
    lat: float
    lon: float


CITIES: list[City] = [
    City("Mumbai", "India", 19.0760, 72.8777),
    City("Bangalore", "India", 12.9716, 77.5946),
    City("Delhi", "India", 28.6139, 77.2090),
    City("Chennai", "India", 13.0827, 80.2707),
    City("Hyderabad", "India", 17.3850, 78.4867),
    City("Kolkata", "India", 22.5726, 88.3639),
    City("London", "United Kingdom", 51.5072, -0.1276),
    City("Manchester", "United Kingdom", 53.4808, -2.2426),
    City("New York", "United States", 40.7128, -74.0060),
    City("San Francisco", "United States", 37.7749, -122.4194),
    City("Chicago", "United States", 41.8781, -87.6298),
    City("Paris", "France", 48.8566, 2.3522),
    City("Berlin", "Germany", 52.5200, 13.4050),
    City("Singapore", "Singapore", 1.3521, 103.8198),
    City("Dubai", "United Arab Emirates", 25.2048, 55.2708),
    City("Sydney", "Australia", -33.8688, 151.2093),
    City("Tokyo", "Japan", 35.6762, 139.6503),
    City("Hong Kong", "China", 22.3193, 114.1694),
    City("Toronto", "Canada", 43.6532, -79.3832),
]

CITIES_BY_COUNTRY: dict[str, list[City]] = {}
for _city in CITIES:
    CITIES_BY_COUNTRY.setdefault(_city.country, []).append(_city)

DEFAULT_COUNTRY = "India"


def cities_by_country(country: str) -> list[City]:
    return CITIES_BY_COUNTRY.get(country, CITIES_BY_COUNTRY[DEFAULT_COUNTRY])


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points, in kilometres."""
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * radius_km * math.asin(math.sqrt(a))


def travel_speed_kmh(distance_km: float, time_seconds: float) -> float:
    """Implied travel speed (km/h) between two events."""
    if time_seconds <= 0:
        return float("inf")
    return distance_km / (time_seconds / 3600.0)


def far_city(from_city: City, min_distance_km: float, rng) -> City:
    """Pick a city at least ``min_distance_km`` away from ``from_city``."""
    candidates = [
        city
        for city in CITIES
        if city.name != from_city.name
        and haversine_km(from_city.lat, from_city.lon, city.lat, city.lon) >= min_distance_km
    ]
    if not candidates:
        return rng.choice([c for c in CITIES if c.name != from_city.name])
    return rng.choice(candidates)
