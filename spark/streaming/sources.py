"""Shared streaming sources: Kafka topic -> parsed event DataFrame.

Every Spark streaming job (Phases 4-15) reads events the same way: subscribe
to a Kafka topic, parse the JSON ``value`` with the explicit schema, and add a
real ``event_ts`` timestamp column.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp

from kafka.config import BOOTSTRAP_SERVERS
from spark.schemas import EVENT_TIME_FORMAT, EVENT_SCHEMAS, parse_event


def kafka_event_stream(spark: SparkSession, topic: str,
                       starting_offsets: str = "earliest",
                       event_type: str | None = None) -> DataFrame:
    """Raw Kafka stream for ``topic`` -> parsed events with ``event_ts``.

    ``event_type`` names the schema to parse with; default is the topic name
    itself. Pass it explicitly when a topic does not match an event name
    (e.g. a test topic that still carries ``transactions`` payloads).
    """
    schema_key = event_type or topic
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    return (
        raw.select(parse_event(schema_key, col("value")).alias("parsed"))
        .select("parsed.*")
        .withColumn("event_ts", to_timestamp(col("event_time"), EVENT_TIME_FORMAT))
    )
