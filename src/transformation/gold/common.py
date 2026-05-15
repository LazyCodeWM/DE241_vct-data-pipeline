"""
common.py
=========
Shared utilities for the Gold Layer pipeline.

Gold reads from Silver Iceberg tables (via Spark) and writes aggregated
results back to Nessie under the 'gold' namespace.

Reuses Spark/Iceberg/config utilities from silver.common to avoid duplication.
TODO: if a separate gold-vct-data bucket is needed, configure a second
      Spark catalog (nessie_gold) pointing to s3a://gold-vct-data/.
      For now, Gold tables live in nessie.gold.* under the same warehouse.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col as spark_col

# Reuse all shared config + helpers from Silver
from src.transformation.silver.common import (  # noqa: F401
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    NESSIE_URI,
    BRONZE_BUCKET,
    SILVER_BUCKET,
    build_s3_client,
    build_spark,
    build_iceberg_catalog,
    ensure_bucket,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError

log = logging.getLogger("gold_vct_pipeline")

GOLD_NS: str = "gold"


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------

def ensure_gold_namespace(catalog) -> None:
    try:
        catalog.create_namespace(GOLD_NS)
        log.info("Namespace '%s' created.", GOLD_NS)
    except NamespaceAlreadyExistsError:
        log.info("Namespace '%s' already exists.", GOLD_NS)


# ---------------------------------------------------------------------------
# Read Silver tables → register as Spark temp views
# ---------------------------------------------------------------------------

def load_silver_views(spark: SparkSession) -> None:
    """
    Read all Silver Iceberg tables needed by Gold and register each as a
    Spark SQL temp view named silver_<table_name>.

    Called once in runner.py before any Gold transform function runs.
    """
    silver_tables = [
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
    for table in silver_tables:
        df = spark.table(f"nessie.silver.{table}")
        df.createOrReplaceTempView(f"silver_{table}")
        log.info("  Loaded view: silver_%s (%d rows)", table, df.count())


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
    createOrReplace() makes every run idempotent.
    Returns number of rows written.
    """
    full_name = f"nessie.{GOLD_NS}.{table_name}"
    writer    = df.writeTo(full_name)
    if partition_cols:
        writer = writer.partitionedBy(*[spark_col(c) for c in partition_cols])
    writer.createOrReplace()
    count = df.count()
    log.info("  gold.%-30s written : %d rows", table_name, count)
    return count
