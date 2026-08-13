"""Kafka layer.

local Kafka in Docker, 6 topics, producers + consumer,
1,000+ events round-tripped. Phase 3: partitions, keys, offsets, groups.
Uses confluent-kafka (``confluent_kafka``) so this package's own name never
shadows the client library.
"""

from __future__ import annotations
