"""Consume events from a Kafka topic and report counts + samples.

Each call uses a fresh consumer group (unless you pass --group), so it reads
from the beginning of the topic.

Usage:
    python -m kafka.consumer --topic transactions
    python -m kafka.consumer --topic transactions --count 1000 --timeout 20000
"""

from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import KafkaError

from .common import new_consumer


def consume(topic: str, group_id: str, max_records: int | None = None,
            timeout_ms: int = 15000) -> dict:
    consumer = new_consumer(topic, group_id)

    count = 0
    per_partition: dict[int, int] = {}
    samples: list[dict] = []
    deadline = time.time() + timeout_ms / 1000.0
    try:
        while time.time() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"consumer error on '{topic}': {msg.error()}")
            count += 1
            partition = msg.partition()
            per_partition[partition] = per_partition.get(partition, 0) + 1
            if len(samples) < 5:
                samples.append({
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "partition": partition,
                    "offset": msg.offset(),
                    "value": json.loads(msg.value().decode("utf-8")),
                })
            if max_records is not None and count >= max_records:
                break
    finally:
        consumer.close()

    return {
        "topic": topic,
        "group_id": group_id,
        "consumed": count,
        "per_partition": per_partition,
        "samples": samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and report on a Kafka topic.")
    parser.add_argument("--topic", required=True, help="topic to consume from")
    parser.add_argument("--group", default="console-consumer", help="consumer group id")
    parser.add_argument("--count", type=int, default=None, help="stop after N records")
    parser.add_argument("--timeout", type=int, default=15000, help="stop after N ms")
    args = parser.parse_args()

    result = consume(args.topic, args.group, max_records=args.count, timeout_ms=args.timeout)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
