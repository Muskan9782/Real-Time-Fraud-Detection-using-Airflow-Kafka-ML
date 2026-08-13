"""Shared confluent-kafka client factories (producer + consumer).

We use confluent-kafka (imported as ``confluent_kafka``) rather than
kafka-python because this project's own ``kafka/`` package would shadow the
latter's module name.
"""

from __future__ import annotations

import json

from confluent_kafka import Consumer, Producer

from .config import BOOTSTRAP_SERVERS


def new_producer() -> Producer:
    """Producer with acks=all (no acknowledged message is lost)."""
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "linger.ms": 5,
    })


def new_consumer(topic: str, group_id: str) -> Consumer:
    """Consumer subscribed to ``topic``, reading from the earliest offset.

    Every call creates a fresh group (unless you pass one explicitly), so the
    whole topic is read back -- exactly what the Phase 2 milestone wants.
    """
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    return consumer
