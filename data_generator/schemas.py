"""Explicit schemas / field contracts for every generated dataset.

Keeping these in one place lets later phases reuse them:
- PySpark reads Kafka JSON with an explicit schema.
- Delta Lake defines Bronze/Silver/Gold tables.
- The tests validate every emitted record against these field lists.
"""

from __future__ import annotations

CUSTOMER_FIELDS = [
    "customer_id", "age", "country", "currency", "avg_transaction",
    "home_city", "home_lat", "home_lon",
]

MERCHANT_FIELDS = [
    "merchant_id", "merchant_name", "category", "country", "city", "lat", "lon",
]

TRANSACTION_FIELDS = [
    "transaction_id", "customer_id", "event_time", "amount", "currency",
    "merchant_id", "payment_method", "location", "lat", "lon", "device_id",
    "status",
]

LOGIN_FIELDS = [
    "login_id", "customer_id", "event_time", "device_id", "ip_address",
    "success", "failure_reason",
]

PAYMENT_FIELDS = [
    "payment_id", "customer_id", "event_time", "transaction_id", "amount",
    "currency", "merchant_id", "payment_method", "status", "failure_reason",
]

LOCATION_FIELDS = [
    "location_id", "customer_id", "event_time", "city", "lat", "lon",
    "device_id",
]

# event_type -> output file stem (file is `<stem>.jsonl`)
EVENT_SCHEMAS: dict[str, list[str]] = {
    "transactions": TRANSACTION_FIELDS,
    "logins": LOGIN_FIELDS,
    "payments": PAYMENT_FIELDS,
    "customer_locations": LOCATION_FIELDS,
}

LABEL_FIELDS = [
    "event_id", "event_type", "customer_id", "scenario", "alert_type",
    "risk_points", "label",
]
