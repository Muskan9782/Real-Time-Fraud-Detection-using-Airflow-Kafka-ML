"""Create all Kafka topics with the partition counts from ``config.py``.

Usage:
    python -m kafka.create_topics
"""

from __future__ import annotations

import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from .config import BOOTSTRAP_SERVERS, REPLICATION_FACTOR, TOPIC_PARTITIONS


def admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def create_topic(name: str, partitions: int,
                 bootstrap_servers: str = BOOTSTRAP_SERVERS) -> str:
    """Idempotently ensure a single topic exists; returns 'created'/'exists'."""
    admin = admin_client()
    if name in admin.list_topics(timeout=10).topics:
        print(f"  topic '{name}' already exists")
        return "exists"
    admin.create_topics([
        NewTopic(name, num_partitions=partitions,
                 replication_factor=REPLICATION_FACTOR)
    ])[name].result(timeout=15)
    print(f"  created topic '{name}' with {partitions} partition(s)")
    return "created"


def delete_topic(name: str, bootstrap_servers: str = BOOTSTRAP_SERVERS,
                 timeout: float = 15.0) -> None:
    """Delete a topic and wait until it has fully disappeared."""
    admin = admin_client()
    admin.delete_topics([name])
    deadline = time.time() + timeout
    while time.time() < deadline:
        if name not in admin.list_topics(timeout=5).topics:
            return
        time.sleep(0.5)
    raise TimeoutError(f"topic '{name}' did not disappear within {timeout}s")


def create_topics(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> list[str]:
    admin = admin_client()
    existing = set(admin.list_topics(timeout=10).topics)

    to_create = [name for name in TOPIC_PARTITIONS if name not in existing]
    created: list[str] = []

    if to_create:
        futures = admin.create_topics([
            NewTopic(
                name,
                num_partitions=TOPIC_PARTITIONS[name],
                replication_factor=REPLICATION_FACTOR,
            )
            for name in to_create
        ])
        for name, future in futures.items():
            future.result(timeout=15)  # raises if topic creation failed
            print(f"  created topic '{name}' with {TOPIC_PARTITIONS[name]} partition(s)")
            created.append(name)

    for name in TOPIC_PARTITIONS:
        if name not in to_create:
            print(f"  topic '{name}' already exists")
    return created


def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS} ...")
    try:
        created = create_topics()
    except Exception as exc:
        print(
            f"ERROR: could not reach Kafka at {BOOTSTRAP_SERVERS}.\n"
            f"  Is it running? Try: docker compose up -d\n"
            f"  ({exc})",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Done. Created: {len(created)}; already present: {len(TOPIC_PARTITIONS) - len(created)}")


if __name__ == "__main__":
    main()
