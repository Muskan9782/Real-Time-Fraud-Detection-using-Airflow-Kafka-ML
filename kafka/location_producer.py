"""Produce location events to the ``customer_locations`` topic.

Usage:
    python -m kafka.location_producer
"""

from __future__ import annotations

import argparse

from data_generator.config import RAW_DIR
from .producers import produce_event_type


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce customer locations to Kafka.")
    parser.add_argument("--limit", type=int, default=None, help="send at most N events")
    parser.add_argument("--file", default=str(RAW_DIR / "customer_locations.jsonl"),
                        help="JSONL source file")
    args = parser.parse_args()

    topic, sent = produce_event_type("customer_locations", limit=args.limit, file_path=args.file)
    print(f"sent {sent} events to topic '{topic}'")


if __name__ == "__main__":
    main()
