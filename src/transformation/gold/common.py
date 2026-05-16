"""
common.py
=========
Shared utilities for the Gold Layer pipeline.

Gold reads from Silver Iceberg tables (via Spark) and writes aggregated
results to nessie.gold.* with data files stored in gold-vct-data bucket.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col as spark_col

# Reuse config constants + low-level helpers from Silver
from src.transformation.silver.common import (  # noqa: F401
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    NESSIE_URI,
    BRONZE_BUCKET,
    SILVER_BUCKET,
    build_s3_client,
    build_iceberg_catalog,
    ensure_bucket,
    _ensure_java17,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError

log = logging.getLogger("gold_vct_pipeline")

GOLD_NS:     str = "gold"
GOLD_BUCKET: str = "gold-vct-data"


# ---------------------------------------------------------------------------
# SparkSession — gold warehouse points to gold-vct-data
# ---------------------------------------------------------------------------

def build_gold_spark() -> SparkSession:
    """SparkSession identical to Silver's but with gold-vct-data as warehouse."""
    _ensure_java17()

    minio_host = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")

    return (
        SparkSession.builder
        .appName("gold_vct_pipeline")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
                "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ]),
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",           NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref",           "main")
        .config("spark.sql.catalog.nessie.warehouse",    f"s3a://{GOLD_BUCKET}/")
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{minio_host}")
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled",   "true")
        .config("spark.driver.memory",          "4g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------

def ensure_gold_namespace(catalog) -> None:
    s3 = build_s3_client()
    ensure_bucket(s3, GOLD_BUCKET)
    try:
        catalog.create_namespace(GOLD_NS)
        log.info("Namespace '%s' created.", GOLD_NS)
    except NamespaceAlreadyExistsError:
        log.info("Namespace '%s' already exists.", GOLD_NS)


# ---------------------------------------------------------------------------
# Read Silver tables → register as Spark temp views
# ---------------------------------------------------------------------------

def load_silver_views(spark: SparkSession, tables: list[str] | None = None) -> None:
    """
    Read Silver Iceberg tables and register as Spark SQL temp views
    named silver_<table_name>.

    Pass `tables` to load only what a specific Gold task needs,
    avoiding unnecessary reads when tasks run in parallel.
    If None, loads all Silver tables.

    Tables that were skipped in Silver (empty Bronze source) are logged as
    warnings and not registered — Gold queries that reference them will raise
    AnalysisException which callers handle per task.
    """
    all_tables = [
        "fact_player_stats",
        "fact_series",
        "fact_map_scores",
        "fact_round_results",
        "fact_map_picks_bans",
        "fact_player_agent_stats",
        "fact_event_standings",
        "dim_events",
        "dim_teams",
        "dim_players",
    ]
    for table in (tables or all_tables):
        try:
            df = spark.table(f"nessie.silver.{table}")
            df.createOrReplaceTempView(f"silver_{table}")
            log.info("  Loaded view: silver_%s (%d rows)", table, df.count())
        except Exception as exc:
            log.warning("  Skipped view: silver_%s — not found in Silver (%s)", table, exc)


# ---------------------------------------------------------------------------
# Write Gold Iceberg table
# ---------------------------------------------------------------------------

def write_gold_table(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
    partition_cols: tuple[str, ...] = (),
) -> int:
    """
    Write a Spark DataFrame to nessie.gold.<table_name>.

    Always drops the existing Nessie catalog entry via PyIceberg before
    creating fresh — this forces Iceberg to use the current session's
    warehouse (gold-vct-data) instead of inheriting the old table location.
    Returns number of rows written.
    """
    from pyiceberg.exceptions import NoSuchTableError

    full_name = f"nessie.{GOLD_NS}.{table_name}"

    cat = build_iceberg_catalog()
    try:
        cat.drop_table((GOLD_NS, table_name))
    except NoSuchTableError:
        pass

    writer = df.writeTo(full_name)
    if partition_cols:
        writer = writer.partitionedBy(*[spark_col(c) for c in partition_cols])
    writer.create()

    count = df.count()
    log.info("  gold.%-30s written : %d rows", table_name, count)
    return count
