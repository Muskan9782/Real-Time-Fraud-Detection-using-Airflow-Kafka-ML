"""Small helpers shared by all generators."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

# ISO-8601-ish, matching the spec's event_time format ("2026-08-13T14:05:21").
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def reference_now() -> datetime:
    """Naive UTC now, without microseconds (keeps output clean)."""
    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


def format_timestamp(dt: datetime) -> str:
    return dt.strftime(TIMESTAMP_FORMAT)


def parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value, TIMESTAMP_FORMAT)


def add_seconds(dt: datetime, seconds: float) -> datetime:
    return dt + timedelta(seconds=seconds)


def weighted_choice(rng: random.Random, choices: list[tuple]) -> object:
    """Pick from ``choices`` where each item is ``(value, weight)``."""
    values, weights = zip(*choices)
    return rng.choices(values, weights=weights, k=1)[0]


def random_ip(rng: random.Random) -> str:
    """Generate a plausible public IPv4 address (first octet never 0/224+)."""
    return ".".join(
        str(rng.randint(1, 223) if i == 0 else rng.randint(0, 255))
        for i in range(4)
    )
