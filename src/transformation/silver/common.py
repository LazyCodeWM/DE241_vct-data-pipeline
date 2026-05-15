"""
common.py
=========
Shared utilities for the Silver Layer pipeline.

Provides:
  - S3 / MinIO client helpers  (build, ensure bucket, read prefix)
  - SparkSession factory        (Iceberg + Nessie + S3A)
  - PyIceberg catalog factory   (Nessie REST)
  - Iceberg table write helper  (via Spark, idempotent)
  - Quarantine writer           (PK violations → Parquet on MinIO)
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime
from typing import Any

import polars as pl
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col as spark_col

load_dotenv()

log = logging.getLogger("silver_vct_pipeline")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MINIO_ENDPOINT:   str = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY: str = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY: str = os.environ["MINIO_SECRET_KEY"]
NESSIE_URI:       str = os.environ["NESSIE_URI"]

BRONZE_BUCKET: str = "bronze-vct-data"
SILVER_BUCKET: str = "silver-vct-data"
SILVER_NS:     str = "silver"


# ---------------------------------------------------------------------------
# S3 / MinIO helpers
# ---------------------------------------------------------------------------

def build_s3_client() -> Any:
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(s3: Any, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if int(exc.response["Error"]["Code"]) == 404:
            s3.create_bucket(Bucket=bucket)
            log.info("Bucket '%s' created.", bucket)
        else:
            raise


def read_bronze_prefix(s3: Any, prefix: str) -> pl.DataFrame:
    """
    Read all JSON objects under `prefix` from the bronze bucket.
    Uses paginator to handle >1000 objects correctly.

    Some Bronze paths store JSON arrays (e.g. team_roster, player_agent_stats)
    rather than a single object per file — those are flattened via extend().

    Returns a Polars DataFrame, or an empty DataFrame if no objects found.
    """
    paginator = s3.get_paginator("list_objects_v2")
    records: list[dict] = []

    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])["Body"].read()
            data = json.loads(body)
            if isinstance(data, list):
                records.extend(data)
            else:
                records.append(data)

    log.info("  [%s] fetched %d records", prefix, len(records))
    return pl.DataFrame(records) if records else pl.DataFrame()


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_spark() -> SparkSession:
    """
    SparkSession configured for:
      - Iceberg Spark runtime + Nessie catalog extensions
      - S3A filesystem pointing at local MinIO (path-style access)
      - Arrow-accelerated pandas bridge for Spark ↔ Polars conversion
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
        .config("spark.sql.catalog.nessie",             "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl","org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",          NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref",          "main")
        .config("spark.sql.catalog.nessie.warehouse",   f"s3a://{SILVER_BUCKET}/")
        .config("spark.hadoop.fs.s3a.endpoint",         f"http://{minio_host}")
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled",  "true")
        .config("spark.driver.memory",          "4g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# PyIceberg catalog (Nessie REST)
# ---------------------------------------------------------------------------

def build_iceberg_catalog() -> Any:
    """
    PyIceberg catalog connected to Nessie's Iceberg REST endpoint (/iceberg).
    PyArrowFileIO handles all S3-compatible I/O — no Hadoop dependency needed.
    """
    nessie_base = NESSIE_URI.rsplit("/api/", 1)[0]
    return load_catalog(
        "nessie",
        **{
            "type":                 "rest",
            "uri":                  f"{nessie_base}/iceberg",
            "warehouse":            "warehouse",
            "py-io-impl":           "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint":          MINIO_ENDPOINT,
            "s3.access-key-id":     MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )


def ensure_namespace(catalog: Any, namespace: str = SILVER_NS) -> None:
    try:
        catalog.create_namespace(namespace)
        log.info("Namespace '%s' created.", namespace)
    except NamespaceAlreadyExistsError:
        log.info("Namespace '%s' already exists.", namespace)


# ---------------------------------------------------------------------------
# Iceberg write
# ---------------------------------------------------------------------------

def write_iceberg_table(
    spark: SparkSession,
    table_name: str,
    df: pl.DataFrame,
    partition_cols: tuple[str, ...] = (),
) -> int:
    """
    Write a Polars DataFrame to nessie.silver.<table_name> via Spark.
    createOrReplace() makes every run fully idempotent.
    Returns number of rows written.
    """
    full_name = f"nessie.{SILVER_NS}.{table_name}"
    spark_df  = spark.createDataFrame(df.to_pandas())
    writer    = spark_df.writeTo(full_name)
    if partition_cols:
        writer = writer.partitionedBy(*[spark_col(c) for c in partition_cols])
    writer.createOrReplace()
    log.info("  silver.%-30s written : %d rows", table_name, len(df))
    return len(df)


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------

def quarantine_violations(s3: Any, table_name: str, df: pl.DataFrame) -> int:
    """
    Write PK-violating rows as Parquet to silver-vct-data/quarantine/<table_name>/.
    Returns number of rows quarantined (0 if df is empty).
    """
    if df.is_empty():
        return 0

    ts  = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    key = f"quarantine/{table_name}/{ts}.parquet"

    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)

    s3.upload_fileobj(buf, SILVER_BUCKET, key)
    log.warning(
        "  quarantine %-25s : %d rows → s3://%s/%s",
        table_name, len(df), SILVER_BUCKET, key,
    )
    return len(df)


# ---------------------------------------------------------------------------
# PK validation helper
# ---------------------------------------------------------------------------

def split_pk_violations(
    df: pl.DataFrame,
    pk_cols: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Split a DataFrame into (clean, violations).

    violations = rows where ANY pk_col is null
    clean      = deduplicated on pk_cols, keeping the last occurrence
                 (consistent with bronze re-ingestion behaviour)
    """
    pk_null = pl.lit(False)
    for col in pk_cols:
        pk_null = pk_null | pl.col(col).is_null()

    violations = df.filter(pk_null)
    clean      = (
        df.filter(~pk_null)
        .unique(subset=pk_cols, keep="last", maintain_order=True)
    )
    return clean, violations
