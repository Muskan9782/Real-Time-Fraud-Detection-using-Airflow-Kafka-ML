"""Phase 3 concepts: topics, partitions, offsets and consumer groups.

Read-only introspection helpers used by ``run_phase3.py`` and useful for
debugging at any later phase:

    topic_metadata()       all managed topics + their partitions
    topic_partitions(t)    partition ids for a topic
    end_offsets(t)         high watermark (next offset to be written)
    committed_offsets(t,g) last committed offset per partition for a group
    group_report(t,g)      end / committed / lag per partition for a group
"""

from __future__ import annotations

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from .config import BOOTSTRAP_SERVERS, TOPIC_PARTITIONS


def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def list_topics() -> list[str]:
    """Every topic currently on the broker (including internal ones)."""
    return sorted(_admin().list_topics(timeout=10).topics)


def topic_partitions(topic: str) -> list[int]:
    """Partition ids (0..n-1) for a topic."""
    md = _admin().list_topics(topic, timeout=10)
    if topic not in md.topics:
        raise KeyError(f"topic '{topic}' does not exist")
    return sorted(md.topics[topic].partitions)


def topic_metadata() -> dict[str, dict]:
    """Partition layout for the topics this project manages."""
    md = _admin().list_topics(timeout=10).topics
    return {
        name: {
            "partitions": sorted(topic.partitions),
            "configured_partitions": TOPIC_PARTITIONS.get(name),
        }
        for name, topic in md.items()
        if name in TOPIC_PARTITIONS
    }


def _offset_consumer(group_id: str) -> Consumer:
    """Consumer used only for offset queries (no subscription / joining)."""
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def end_offsets(topic: str) -> dict[int, int]:
    """High watermark per partition: the next offset Kafka will write to."""
    consumer = _offset_consumer("offset-inspector")
    try:
        result: dict[int, int] = {}
        for partition in topic_partitions(topic):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10
            )
            result[partition] = high
        return result
    finally:
        consumer.close()


def committed_offsets(topic: str, group_id: str) -> dict[int, int | None]:
    """Last committed offset per partition for a consumer group.

    None means the group has never committed anything on that partition.
    """
    consumer = _offset_consumer(group_id)
    try:
        tps = [TopicPartition(topic, p) for p in topic_partitions(topic)]
        committed = consumer.committed(tps, timeout=10)
        if isinstance(committed, dict):
            entries = committed.values()
        else:
            entries = committed
        by_partition = {entry.partition: entry for entry in entries}
        result: dict[int, int | None] = {}
        for partition in topic_partitions(topic):
            entry = by_partition.get(partition)
            result[partition] = entry.offset if entry is not None and entry.offset >= 0 else None
        return result
    finally:
        consumer.close()


def group_report(topic: str, group_id: str) -> dict:
    """Per-partition end offset, committed offset and lag for one group."""
    end = end_offsets(topic)
    committed = committed_offsets(topic, group_id)
    per_partition: dict[int, dict] = {}
    for partition in sorted(end):
        committed_offset = committed.get(partition)
        per_partition[partition] = {
            "end_offset": end[partition],
            "committed_offset": committed_offset,
            "lag": (end[partition] - committed_offset)
            if committed_offset is not None else None,
        }
    return {
        "topic": topic,
        "group_id": group_id,
        "per_partition": per_partition,
        "total_end": sum(end.values()),
    }
