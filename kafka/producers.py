"""Generic producer helpers: stream a JSONL event file into a Kafka topic.

Each event is keyed by a field of your choice (``customer_id`` for this
project), so one customer's events stay ordered within a partition.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka import Producer

from .common import new_producer
from .config import EVENT_ROUTES


def produce_file(producer: Producer, topic: str, file_path: str,
                 key_field: str, limit: int | None = None) -> int:
    """Send every record from a JSONL file to ``topic``.

    Returns the number of events sent. Raises if any message fails delivery.
    """
    sent = 0
    failed = 0

    def _on_delivery(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1
            print(f"  delivery failed: {err}", file=sys.stderr)

    with Path(file_path).open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            event = json.loads(line)
            producer.produce(
                topic,
                key=str(event[key_field]).encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=_on_delivery,
            )
            sent += 1
            if limit is not None and sent >= limit:
                break
            if sent % 1000 == 0:
                producer.poll(0)  # serve delivery reports

    producer.flush()
    if failed:
        raise RuntimeError(f"{failed} of {sent} messages failed delivery to '{topic}'")
    return sent


def produce_event_type(event_type: str, limit: int | None = None,
                       file_path: str | None = None) -> tuple[str, int]:
    """Produce one event type (transactions/logins/payments/customer_locations).

    Returns (topic, number sent).
    """
    topic, key_field = EVENT_ROUTES[event_type]
    path = file_path or f"data/raw/{event_type}.jsonl"
    producer = new_producer()
    try:
        sent = produce_file(producer, topic, path, key_field, limit=limit)
    finally:
        producer.flush()
    return topic, sent
