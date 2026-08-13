"""Explicit PySpark ``StructType`` schemas for the event streams.

Mirrors ``data_generator.schemas`` so Spark never has to guess field types.
Guessing is a classic source of bugs: a JSON ``amount`` would come back as a
string, and ``amount > 200`` would then do a *lexical* comparison. Declaring
the schema up front fixes the type at parse time.

Convention: ``event_time`` stays STRING in the schema (that is what the JSON
contains) and each job converts it with ``to_timestamp`` using the exact
format the generators write (see ``data_generator.utils.TIMESTAMP_FORMAT``).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql.functions import from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"


def _s(name: str, *, nullable: bool = False) -> StructField:
    return StructField(name, StringType(), nullable=nullable)


TRANSACTIONS_SCHEMA = StructType([
    _s("transaction_id"),
    _s("customer_id"),
    _s("event_time"),
    StructField("amount", DoubleType()),
    _s("currency"),
    _s("merchant_id"),
    _s("payment_method"),
    _s("location"),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    _s("device_id"),
    _s("status"),
])

LOGINS_SCHEMA = StructType([
    _s("login_id"),
    _s("customer_id"),
    _s("event_time"),
    _s("device_id"),
    _s("ip_address"),
    StructField("success", BooleanType()),
    StructField("failure_reason", StringType(), nullable=True),
])

PAYMENTS_SCHEMA = StructType([
    _s("payment_id"),
    _s("customer_id"),
    _s("event_time"),
    _s("transaction_id"),
    StructField("amount", DoubleType()),
    _s("currency"),
    _s("merchant_id"),
    _s("payment_method"),
    _s("status"),
    StructField("failure_reason", StringType(), nullable=True),
])

LOCATIONS_SCHEMA = StructType([
    _s("location_id"),
    _s("customer_id"),
    _s("event_time"),
    _s("city"),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    _s("device_id"),
])

# Phase 12: unified alert envelope on the `fraud_alerts` topic. Every detector
# (phases 7-11) emits the same shape so the risk engine can combine them.
ALERTS_SCHEMA = StructType([
    _s("alert_id"),
    _s("customer_id"),
    _s("event_time"),
    _s("alert_type"),
    StructField("risk_points", IntegerType()),
    StructField("transaction_id", StringType(), nullable=True),
    StructField("detail", StringType(), nullable=True),
])

# event_type -> StructType, matching data_generator.schemas.EVENT_SCHEMAS
EVENT_SCHEMAS: dict[str, StructType] = {
    "transactions": TRANSACTIONS_SCHEMA,
    "logins": LOGINS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "customer_locations": LOCATIONS_SCHEMA,
    "alerts": ALERTS_SCHEMA,
}


def parse_event(event_type: str, value_col: Column) -> Column:
    """``from_json`` expression: parse a Kafka message ``value`` as this event."""
    return from_json(value_col.cast("string"), EVENT_SCHEMAS[event_type])
