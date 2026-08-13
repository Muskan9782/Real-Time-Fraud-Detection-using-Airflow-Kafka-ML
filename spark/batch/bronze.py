"""Bronze layer: raw source data lands untouched in Delta tables.

The medallion (bronze/silver/gold) lakehouse starts here: every raw file from (``data/raw/``) is read back with an explicit schema and appended to a
Delta table with only a ``_ingestion_ts`` processing-time column added. Bronze
is the immutable, auditable copy of what the generators wrote - no cleaning,
no deduplication, no joins. Delta gives it ACID appends + a versioned
transaction log, so every later layer can be rebuilt from these tables.

Tables (``data/lake/bronze/``):
- transactions / logins / payments / customer_locations  (from JSONL)
- customers / merchants                                 (reference CSV)
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark.common import REPO_ROOT
from spark.schemas import (
    LOCATIONS_SCHEMA,
    LOGINS_SCHEMA,
    PAYMENTS_SCHEMA,
    TRANSACTIONS_SCHEMA,
)

LAKE_ROOT = REPO_ROOT / "data" / "lake"
RAW_DIR = REPO_ROOT / "data" / "raw"

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("age", IntegerType()),
    StructField("country", StringType()),
    StructField("currency", StringType()),
    StructField("avg_transaction", DoubleType()),
    StructField("home_city", StringType()),
    StructField("home_lat", DoubleType()),
    StructField("home_lon", DoubleType()),
])

MERCHANTS_SCHEMA = StructType([
    StructField("merchant_id", StringType()),
    StructField("merchant_name", StringType()),
    StructField("category", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
])

# event_type -> (raw file, explicit StructType, natural id column)
EVENT_SOURCES: dict[str, tuple[str, StructType, str]] = {
    "transactions": ("transactions.jsonl", TRANSACTIONS_SCHEMA, "transaction_id"),
    "logins": ("logins.jsonl", LOGINS_SCHEMA, "login_id"),
    "payments": ("payments.jsonl", PAYMENTS_SCHEMA, "payment_id"),
    "customer_locations": (
        "customer_locations.jsonl", LOCATIONS_SCHEMA, "location_id"),
}

# reference table -> (raw file, explicit StructType)
REFERENCE_SOURCES: dict[str, tuple[str, StructType]] = {
    "customers": ("customers.csv", CUSTOMERS_SCHEMA),
    "merchants": ("merchants.csv", MERCHANTS_SCHEMA),
}


def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    """Read a Delta table from its storage path."""
    return spark.read.format("delta").load(str(path))


def delta_count(spark: SparkSession, path: Path) -> int:
    """Row count of a Delta table (for verification)."""
    return int(read_delta(spark, path).count())


def _read_raw(spark: SparkSession, kind: str, file: Path,
              schema: StructType) -> DataFrame:
    if kind == "csv":
        return spark.read.option("header", "true").schema(schema).csv(str(file))
    return spark.read.schema(schema).json(str(file))


def build_bronze(spark: SparkSession, raw_dir: Path = RAW_DIR,
                 lake_root: Path = LAKE_ROOT) -> dict:
    """Append every raw source into its Delta Bronze table; return counts."""
    bronze_root = lake_root / "bronze"
    bronze_root.mkdir(parents=True, exist_ok=True)
    summary: dict = {}
    for name, (file, schema, _id) in EVENT_SOURCES.items():
        df = _read_raw(spark, "json", raw_dir / file, schema)
        df = df.withColumn("_ingestion_ts", current_timestamp())
        out = bronze_root / name
        df.write.format("delta").mode("append").save(str(out))
        summary[name] = {"rows": delta_count(spark, out), "path": str(out)}
    for name, (file, schema) in REFERENCE_SOURCES.items():
        df = _read_raw(spark, "csv", raw_dir / file, schema)
        df = df.withColumn("_ingestion_ts", current_timestamp())
        out = bronze_root / name
        df.write.format("delta").mode("append").save(str(out))
        summary[name] = {"rows": delta_count(spark, out), "path": str(out)}
    return summary
