"""Produce transaction events to the ``transactions`` topic.

Usage:
    python -m kafka.transaction_producer                # full file
    python -m kafka.transaction_producer --limit 100    # smoke test
"""

from __future__ import annotations

import argparse

from data_generator.config import RAW_DIR
from .producers import produce_event_type


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce transactions to Kafka.")
    parser.add_argument("--limit", type=int, default=None, help="send at most N events")
    parser.add_argument("--file", default=str(RAW_DIR / "transactions.jsonl"),
                        help="JSONL source file")
    args = parser.parse_args()

    topic, sent = produce_event_type("transactions", limit=args.limit, file_path=args.file)
    print(f"sent {sent} events to topic '{topic}'")


if __name__ == "__main__":
    main()
