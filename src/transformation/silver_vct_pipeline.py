"""
silver_vct_pipeline.py
======================
Silver Layer Transformation — Medallion Architecture | VCT Esports

Reads raw JSON records from the bronze MinIO bucket, applies type coercion
and struct flattening via Polars (via Arrow bridge from Spark), enriches
player stats with match/event context, then writes three Iceberg tables to
a Nessie-backed silver bucket.  The pipeline is fully idempotent: re-running
it overwrites the same partitions rather than appending duplicate data.

Run with:
    python -m src.transformation.silver_vct_pipeline
  or:
    python src/transformation/silver_vct_pipeline.py
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col as spark_col

# ---------------------------------------------------------------------------
# Project-root on sys.path so we can import helpers from the bronze script
# without requiring __init__.py files in the tree.
# File is at src/transformation/silver_vct_pipeline.py → parents[2] = repo root.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.ingestion.bronze_vct_backfill import _build_s3_client, _ensure_bucket  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("silver_vct_pipeline")

# ---------------------------------------------------------------------------
# Configuration — all values from .env, never hardcoded
# ---------------------------------------------------------------------------
load_dotenv()

MINIO_ENDPOINT:   str = os.environ["MINIO_ENDPOINT"]       # e.g. http://localhost:9000
MINIO_ACCESS_KEY: str = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY: str = os.environ["MINIO_SECRET_KEY"]
NESSIE_URI:       str = os.environ["NESSIE_URI"]            # e.g. http://localhost:19120/api/v1

BRONZE_BUCKET: str = "bronze-vct-data"
SILVER_BUCKET: str = "silver-vct-data"
SILVER_NS:     str = "silver"


# ---------------------------------------------------------------------------
# Step 0 — SparkSession
# ---------------------------------------------------------------------------

def _build_spark() -> SparkSession:
    """
    Build a SparkSession configured for:
      - Iceberg Spark runtime + Nessie catalog extensions
      - S3A filesystem pointing at local MinIO (path-style access required)
      - Arrow-accelerated pandas bridge for the Spark → Polars conversion
    """
    minio_host = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")

    return (
        SparkSession.builder
        .appName("silver_vct_pipeline")
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
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", f"s3a://{SILVER_BUCKET}/")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_host}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Step 1 — Read bronze JSON files with boto3
# ---------------------------------------------------------------------------

def _read_bronze_prefix(s3: Any, bucket: str, prefix: str) -> pl.DataFrame:
    """
    List all objects under `prefix`, download each JSON body,
    parse into a dict, collect into a list, then build ONE
    Polars DataFrame from the full list in memory.
    Uses paginator to handle >1000 objects correctly.
    """
    paginator = s3.get_paginator("list_objects_v2")
    records: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            records.append(json.loads(body))

    log.info("  Fetched %d files from %s", len(records), prefix)
    return pl.DataFrame(records) if records else pl.DataFrame()


def _read_bronze(s3: Any) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Read all JSON files from the three bronze prefixes into Polars DataFrames
    via boto3 — avoids the small-files penalty of Spark glob scans over MinIO.
    """
    log.info("Step 1: Reading bronze JSON from s3://%s/ …", BRONZE_BUCKET)

    df_events       = _read_bronze_prefix(s3, BRONZE_BUCKET, "events/raw/")
    df_series       = _read_bronze_prefix(s3, BRONZE_BUCKET, "series/raw/")
    df_player_stats = _read_bronze_prefix(s3, BRONZE_BUCKET, "player_stats/raw/")

    log.info("  Events rows      : %d", len(df_events))
    log.info("  Series rows      : %d", len(df_series))
    log.info("  PlayerStats rows : %d", len(df_player_stats))

    return df_events, df_series, df_player_stats


# ---------------------------------------------------------------------------
# Step 2 — Clean & Transform (Polars)
# ---------------------------------------------------------------------------

def _transform_events(df: pl.DataFrame) -> pl.DataFrame:
    """Silver coercion for the events dimension. Drops ingestion metadata."""
    return (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("event_id").cast(pl.Int64),
            pl.col("start_date").cast(pl.Date),
            pl.col("end_date").cast(pl.Date),
        ])
    )


def _transform_series(df: pl.DataFrame) -> pl.DataFrame:
    """
    Silver coercion for series (match) facts.
    Flattens team1/team2 nested structs so every column is a scalar —
    nested structs complicate downstream SQL queries and Iceberg partitioning.
    """
    df = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("match_id").cast(pl.Int64),
            pl.col("match_date").cast(pl.Date),
        ])
    )

    # struct.field() accesses a sub-field; returns null when the parent struct is null
    return (
        df
        .with_columns([
            pl.col("team1").struct.field("name").alias("team1_name"),
            pl.col("team1").struct.field("team_id").cast(pl.Int64).alias("team1_id"),
            pl.col("team1").struct.field("series_score").cast(pl.Int32).alias("team1_score"),
            pl.col("team2").struct.field("name").alias("team2_name"),
            pl.col("team2").struct.field("team_id").cast(pl.Int64).alias("team2_id"),
            pl.col("team2").struct.field("series_score").cast(pl.Int32).alias("team2_score"),
        ])
        .drop(["team1", "team2"])
    )


def _transform_player_stats(df: pl.DataFrame) -> pl.DataFrame:
    """
    Silver coercion for per-map player stats.
    `rating` is renamed to `r_rating` because 'rating' is a reserved word in
    several SQL dialects and can collide with Iceberg internal metadata fields.
    game_id is cast to Int64 with strict=False because the API occasionally
    returns it as a string; non-castable values become null (caught by PK check).
    """
    return (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("match_id").cast(pl.Int64),
            pl.col("map_index").cast(pl.Int32),
            pl.col("game_id").cast(pl.Int64, strict=False),
            pl.col("player_id").cast(pl.Int64, strict=False),
            pl.col("team_id").cast(pl.Int64, strict=False),
            pl.col("acs").cast(pl.Int32, strict=False),
            pl.col("kills").cast(pl.Int32, strict=False),
            pl.col("deaths").cast(pl.Int32, strict=False),
            pl.col("assists").cast(pl.Int32, strict=False),
            pl.col("fk").cast(pl.Int32, strict=False),
            pl.col("fd").cast(pl.Int32, strict=False),
            pl.col("rating").cast(pl.Float32, strict=False),
            pl.col("kast").cast(pl.Float32, strict=False),
            pl.col("adr").cast(pl.Float32, strict=False),
            pl.col("hs_pct").cast(pl.Float32, strict=False),
        ])
        .rename({"rating": "r_rating"})
    )


# ---------------------------------------------------------------------------
# Step 3 — Enrich & Join (Polars)
# ---------------------------------------------------------------------------

def _enrich_player_stats(
    df_player_stats: pl.DataFrame,
    df_series: pl.DataFrame,
    df_events: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Build the enriched player stats table and split out PK violations.

    Join chain:
      player_stats
        LEFT JOIN series       ON match_id
        LEFT JOIN events       ON series.event_name = events.name

    The Bronze series records carry `event_name` (string) rather than `event_id`,
    so we resolve the FK by joining series → events on the name string.  This is
    the only available link between these two datasets in the Bronze layer.

    Primary key: (match_id, game_id, player_id)
    Rows with any null PK column are split into a separate violations frame and
    routed to quarantine by the caller.  Duplicate PKs (possible on backfill
    re-runs) are deduplicated keeping the last occurrence.

    Returns (enriched_df, pk_violations_df, series_with_event_id).
    The third value is df_series enriched with event_id — callers use it for
    Iceberg partitioning without rebuilding the same lookup.
    """
    log.info("Step 3: Joining player_stats ← series ← events …")

    events_lookup = (
        df_events
        .select(["event_id", "name", "region", "start_date"])
        .rename({"name": "_event_name_key"})
    )
    df_series_with_event_id = df_series.join(
        events_lookup,
        left_on="event_name",
        right_on="_event_name_key",
        how="left",
    ).with_columns(pl.col("event_id").cast(pl.Int64))

    # player_stats LEFT JOIN series (carrying event context)
    joined = df_player_stats.join(
        df_series_with_event_id.select([
            "match_id", "match_date",
            "team1_name", "team2_name",
            "event_id", "event_name", "region", "start_date",
        ]),
        on="match_id",
        how="left",
    )

    # Enforce the required output column order
    enriched = joined.select([
        # From events (resolved via series.event_name → events.name)
        pl.col("event_id"),
        pl.col("event_name"),
        pl.col("region"),
        pl.col("start_date"),
        # From series
        pl.col("match_id"),
        pl.col("match_date"),
        pl.col("team1_name"),
        pl.col("team2_name"),
        # Map info
        pl.col("game_id"),
        pl.col("map_index"),
        pl.col("map_name"),
        # Player identity
        pl.col("player_id"),
        pl.col("player_name"),
        pl.col("team_id"),
        pl.col("team_short"),
        # Performance
        pl.col("agents"),
        pl.col("r_rating"),
        pl.col("acs"),
        pl.col("kills"),
        pl.col("deaths"),
        pl.col("assists"),
        # Advanced stats
        pl.col("kast"),
        pl.col("adr"),
        pl.col("hs_pct"),
        pl.col("fk"),
        pl.col("fd"),
    ])

    # Split PK violations (any null in the three-column PK)
    pk_null = (
        pl.col("match_id").is_null()
        | pl.col("game_id").is_null()
        | pl.col("player_id").is_null()
    )
    violations = enriched.filter(pk_null)
    enriched   = enriched.filter(~pk_null)

    # Deduplicate on PK — keeps the last row, consistent with bronze re-ingestion behaviour
    enriched = enriched.unique(
        subset=["match_id", "game_id", "player_id"],
        keep="last",
        maintain_order=True,
    )

    log.info("  Enriched rows : %d", len(enriched))
    log.info("  PK violations : %d", len(violations))
    return enriched, violations, df_series_with_event_id


# ---------------------------------------------------------------------------
# Step 3b — Route PK violations to quarantine
# ---------------------------------------------------------------------------

def _route_pk_violations(s3: Any, violations: pl.DataFrame) -> int:
    """
    Write PK-violating rows as Parquet directly to MinIO via an in-memory buffer.
    No data is written to local disk — io.BytesIO is the only intermediate store.
    Returns the row count written.
    """
    if violations.is_empty():
        log.info("No PK violations — quarantine write skipped.")
        return 0

    ts  = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    key = f"quarantine/silver_pk_violations/{ts}.parquet"

    buf = io.BytesIO()
    violations.write_parquet(buf)
    buf.seek(0)

    s3.upload_fileobj(buf, BRONZE_BUCKET, key)
    log.warning(
        "PK violations quarantined → s3://%s/%s  (%d rows)",
        BRONZE_BUCKET, key, len(violations),
    )
    return len(violations)


# ---------------------------------------------------------------------------
# Step 4 — Write Iceberg Tables (PyIceberg + Nessie)
# ---------------------------------------------------------------------------

def _build_iceberg_catalog() -> Any:
    """
    Build a PyIceberg catalog connected to Project Nessie with MinIO as the
    underlying file store.  PyArrowFileIO handles all S3-compatible I/O so
    no Hadoop dependency is needed on the Python side.

    pyiceberg 0.7.0 dropped CatalogType.NESSIE; Nessie exposes the Iceberg
    REST catalog spec at /iceberg, so we use type=rest pointing there.
    NESSIE_URI is expected as http://host:port/api/v1 — the /api/vX suffix is
    stripped to derive the Nessie base URL before appending /iceberg.
    """
    nessie_base = NESSIE_URI.rsplit("/api/", 1)[0]
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": f"{nessie_base}/iceberg",
            "warehouse": "warehouse",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )


def _ensure_silver_namespace(catalog: Any) -> None:
    """Create the 'silver' namespace in Nessie if it does not already exist."""
    try:
        catalog.create_namespace(SILVER_NS)
        log.info("Namespace '%s' created in Nessie.", SILVER_NS)
    except NamespaceAlreadyExistsError:
        log.info("Namespace '%s' already exists in Nessie.", SILVER_NS)


def _write_table_spark(
    spark: SparkSession,
    table_name: str,
    polars_df: pl.DataFrame,
    partition_cols: tuple[str, ...],
) -> int:
    """
    Write a Polars DataFrame to a Nessie Iceberg table via Spark.

    Spark writes Parquet files to MinIO using the S3A FileSystem (credentials
    from spark.hadoop.fs.s3a.*) and commits Iceberg metadata to Nessie via the
    NessieCatalog Java client (/api/v1).  This bypasses Nessie's Iceberg REST
    endpoint (/iceberg) which requires server-side S3 credentials for location
    validation — credentials that are not straightforward to provide in Nessie's
    Quarkus secrets configuration for a local dev environment.

    createOrReplace() is fully idempotent: creates the table on first run,
    replaces its content on subsequent runs.

    Returns the number of rows written.
    """
    full_name = f"nessie.{SILVER_NS}.{table_name}"
    spark_df  = spark.createDataFrame(polars_df.to_pandas())
    (
        spark_df.writeTo(full_name)
        .partitionedBy(*[spark_col(c) for c in partition_cols])
        .createOrReplace()
    )
    return len(polars_df)


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """
    Orchestrates Bronze → Silver for VCT data.

    Step 1  Read raw JSON from MinIO via boto3 → 3 Polars DataFrames.
    Step 2  Apply type coercion and struct flattening via Polars.
    Step 3  Enrich player stats with match/event context; quarantine PK violations.
    Step 4  Build SparkSession → write three Iceberg tables to Nessie.
    """
    log.info("=" * 60)
    log.info("Silver Layer Pipeline — VCT Esports")
    log.info("MinIO endpoint : %s", MINIO_ENDPOINT)
    log.info("Nessie URI     : %s", NESSIE_URI)
    log.info("Silver bucket  : %s", SILVER_BUCKET)
    log.info("=" * 60)

    stats: dict[str, int] = {
        "events_read":               0,
        "series_read":               0,
        "player_stats_read":         0,
        "dim_events_written":        0,
        "fact_series_written":       0,
        "fact_player_stats_written": 0,
        "pk_violations":             0,
    }

    # ── Infrastructure ───────────────────────────────────────────────────────
    s3 = _build_s3_client()
    _ensure_bucket(s3, SILVER_BUCKET)

    # ── Step 1: Read ─────────────────────────────────────────────────────────
    df_events_raw, df_series_raw, df_player_stats_raw = _read_bronze(s3)

    # ── Step 2: Transform ────────────────────────────────────────────────────
    log.info("Step 2: Applying Silver-layer transformations …")
    df_events       = _transform_events(df_events_raw)
    df_series       = _transform_series(df_series_raw)
    df_player_stats = _transform_player_stats(df_player_stats_raw)

    stats["events_read"]       = len(df_events)
    stats["series_read"]       = len(df_series)
    stats["player_stats_read"] = len(df_player_stats)

    log.info("  Events      : %d rows", stats["events_read"])
    log.info("  Series      : %d rows", stats["series_read"])
    log.info("  PlayerStats : %d rows", stats["player_stats_read"])

    # ── Step 3: Enrich + PK violations ───────────────────────────────────────
    df_enriched, df_violations, df_series = _enrich_player_stats(
        df_player_stats, df_series, df_events
    )
    stats["pk_violations"] = _route_pk_violations(s3, df_violations)

    # ── Step 4: Write Iceberg tables ─────────────────────────────────────────
    spark   = _build_spark()
    catalog = _build_iceberg_catalog()
    _ensure_silver_namespace(catalog)

    log.info("Step 4: Writing Iceberg tables to Nessie (namespace: %s) …", SILVER_NS)

    table_writes = [
        ("dim_events",        df_events,   ("region",),              "dim_events_written"),
        ("fact_series",       df_series,   ("event_id",),            "fact_series_written"),
        ("fact_player_stats", df_enriched, ("event_id", "map_name"), "fact_player_stats_written"),
    ]
    for table_name, df, pcols, stat_key in table_writes:
        try:
            n = _write_table_spark(spark, table_name, df, pcols)
            stats[stat_key] = n
            log.info("  silver.%s written : %d rows", table_name, n)
        except Exception as exc:
            log.error("  FAILED silver.%s: %s", table_name, exc, exc_info=True)

    # ── Final summary (mirrors bronze backfill style) ─────────────────────────
    log.info("=" * 60)
    log.info("Silver pipeline complete.")
    log.info("  Events       read    : %d", stats["events_read"])
    log.info("  Series       read    : %d", stats["series_read"])
    log.info("  PlayerStats  read    : %d", stats["player_stats_read"])
    log.info("  dim_events   written : %d", stats["dim_events_written"])
    log.info("  fact_series  written : %d", stats["fact_series_written"])
    log.info("  fact_player_stats    : %d", stats["fact_player_stats_written"])
    log.info("  PK violations routed : %d", stats["pk_violations"])
    log.info("=" * 60)

    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
