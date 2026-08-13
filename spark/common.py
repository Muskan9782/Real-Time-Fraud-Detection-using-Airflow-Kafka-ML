"""Shared PySpark bootstrap: environment detection + SparkSession factory.

Spark on Windows needs two things the OS does not provide by default:

- a JDK (``JAVA_HOME``) - we look for an Eclipse Adoptium Temurin install, or
  any ``java`` already on PATH
- the Hadoop Windows native binary ``winutils.exe`` (``HADOOP_HOME``) - we ship
  a copy under ``.tools/winutils`` so local mode starts without manual setup

Every Spark job in this project calls :func:`get_spark` instead of building a
session directly, so all of this is handled in one place.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parent.parent

# Structured Streaming talks to Kafka through the external connector module;
# its version must match the installed PySpark version exactly.
KAFKA_CONNECTOR = (
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
)

# Phase 15 (Delta Lake): the Delta connector jar is pulled from Maven Central
# by Spark's package resolver (no pip install - the delta-spark *wheel* pins
# pyspark<=4.1.1, so installing it would downgrade this box's pyspark 4.2.0).
# Delta Lake has no Spark-4.2-specific artifact yet (latest official builds
# target Spark 4.0/4.1); delta-spark_4.1_2.13:4.3.1 is verified working on
# Spark 4.2.0 here (write / append / merge / delete / time travel / history).
DELTA_CONNECTOR = "io.delta:delta-spark_4.1_2.13:4.3.1"


def _newest_temurin() -> Path | None:
    """Newest installed Temurin JDK under the standard Program Files location."""
    candidates = sorted(
        Path(r"C:\Program Files\Eclipse Adoptium").glob("jdk-*"),
        key=lambda p: p.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


def ensure_java_home() -> str | None:
    """Resolve JAVA_HOME if it is not already set; returns the path or None."""
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    java = shutil.which("java")
    if java:
        home = str(Path(java).resolve().parent.parent)
        os.environ["JAVA_HOME"] = home
        return home
    temurin = _newest_temurin()
    if temurin:
        os.environ["JAVA_HOME"] = str(temurin)
        return str(temurin)
    return None


def ensure_hadoop_home() -> str | None:
    """Resolve HADOOP_HOME if not set; Spark needs winutils.exe on Windows."""
    if os.environ.get("HADOOP_HOME"):
        hadoop_home = os.environ["HADOOP_HOME"]
    else:
        local = REPO_ROOT / ".tools" / "winutils"
        if not (local / "bin" / "winutils.exe").exists():
            return None
        hadoop_home = str(local)
        os.environ["HADOOP_HOME"] = hadoop_home

    # The JVM builds java.library.path from PATH on Windows, so Hadoop can find
    # winutils.exe + hadoop.dll only if their bin dir is on PATH before the
    # JVM starts. Prepend it once so checkpointing (NativeIO) works.
    bin_dir = str(Path(hadoop_home) / "bin")
    if os.name == "nt" and bin_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
    return hadoop_home


def get_spark(app_name: str, master: str | None = None,
              extra_conf: dict[str, str] | None = None,
              use_delta: bool = False) -> SparkSession:
    """Build the SparkSession used by every job in this project.

    Set ``SPARK_MASTER`` (default ``local[*]``) to point at a cluster later.
    Pass ``use_delta=True`` to load the Delta Lake connector (Phase 15).
    """
    java_home = ensure_java_home()
    if java_home is None:
        print("WARN: no JAVA_HOME / java found - install a JDK before running Spark")
    hadoop_home = ensure_hadoop_home()
    if hadoop_home is None:
        print("WARN: no HADOOP_HOME / winutils.exe - Spark may fail on Windows")

    # Spark launches its Python workers through $PYSPARK_PYTHON. On Windows a
    # bare `python` can resolve to the Microsoft Store app-execution alias
    # ("Python was not found; run without arguments to install from the
    # Microsoft Store"), which breaks every Python UDF / stateful processor.
    # Pin it to the interpreter that is actually running this process.
    if not os.environ.get("PYSPARK_PYTHON"):
        os.environ["PYSPARK_PYTHON"] = sys.executable

    builder = SparkSession.builder
    builder.appName(app_name)
    builder.master(master or os.environ.get("SPARK_MASTER", "local[*]"))
    builder.config("spark.jars.packages", KAFKA_CONNECTOR)
    builder.config("spark.sql.shuffle.partitions", "2")
    builder.config("spark.ui.enabled", "false")
    builder.config("spark.sql.streaming.schemaInference", "false")
    builder.config("spark.sql.session.timeZone", "UTC")
    if use_delta:
        # Phase 15: Delta Lake (jars + session extensions + catalog hook). The
        # jar list keeps the Kafka connector so delta and streaming coexist.
        builder.config(
            "spark.jars.packages", f"{KAFKA_CONNECTOR},{DELTA_CONNECTOR}")
        builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder.config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Spark 4 arbitrary stateful operators (transformWithState /
    # applyInPandasWithState) only support the RocksDB state store backend.
    # RocksDB is the recommended provider for windowed aggregations too.
    builder.config(
        "spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    )
    if os.name == "nt":
        # The JVM auto-detects the hostname (e.g. LAPTOP.mshome.net), which it
        # often cannot resolve back to itself. Stateful jobs that use the
        # RocksDB StateStoreCoordinator (transformWithState, applyInPandas*)
        # then fail with "Cannot find endpoint". Pin the loopback address for
        # local mode so driver <-> executor RPC is deterministic.
        builder.config("spark.driver.host", "127.0.0.1")
        builder.config("spark.driver.bindAddress", "127.0.0.1")
        # foreachBatch callbacks run a Spark job from inside the streaming
        # thread; awaiting that job recursively nests frames (DAGScheduler ->
        # awaitReady -> lock acquire) and the default 512K stack overflows on
        # this box ("java.lang.StackOverflowError in stream execution thread").
        # A larger thread stack makes the callback path reliable.
        builder.config("spark.driver.extraJavaOptions", "-Xss32m")
        builder.config("spark.executor.extraJavaOptions", "-Xss32m")
    if hadoop_home:
        builder.config("spark.hadoop.home.dir", hadoop_home)
    for key, value in (extra_conf or {}).items():
        builder.config(key, value)
    return builder.getOrCreate()
