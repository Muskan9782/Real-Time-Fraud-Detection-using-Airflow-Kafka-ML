"""Kafka connection + topic configuration.

Everything is overridable via environment variables so the same code runs
against local Docker Kafka and, later, managed Kafka on GCP.
"""

from __future__ import annotations

import os

# Point this at your broker. For managed Kafka (Confluent Cloud on GCP later)
# you only change this one variable / env var.
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# topic -> number of partitions. The spec says start transactions with 3.
TOPIC_PARTITIONS: dict[str, int] = {
    "transactions": 3,
    "logins": 3,
    "payments": 3,
    "customer_locations": 3,
    "fraud_alerts": 1,
    "dead_letter": 1,
}

# Single-broker local cluster; managed Kafka will use 3+.
REPLICATION_FACTOR = 1

# event type (output file stem in data/raw) -> (kafka topic, message key field)
# Keying by customer_id keeps one customer's events ordered in one partition.
EVENT_ROUTES: dict[str, tuple[str, str]] = {
    "transactions": ("transactions", "customer_id"),
    "logins": ("logins", "customer_id"),
    "payments": ("payments", "customer_id"),
    "customer_locations": ("customer_locations", "customer_id"),
}
