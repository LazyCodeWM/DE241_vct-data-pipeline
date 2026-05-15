"""
vct_pipeline_dag.py
===================
Airflow DAG — VCT Data Pipeline (Medallion Architecture)

Orchestrates the full Bronze → Silver → Gold pipeline.

Schedule : None  (manual trigger only — designed for on-demand backfill)
Catchup  : False

Task dependency:
    bronze_ingestion >> silver_transform >> gold_transform

Notes:
  - bronze_ingestion  : fully implemented, runs bronze_vct_backfill.run_backfill()
  - silver_transform  : fully implemented, runs silver.runner.run_pipeline()
  - gold_transform    : placeholder — will be wired when feature/gold-layer is merged
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="vct_pipeline",
    description="VCT Medallion Pipeline: Bronze → Silver → Gold",
    schedule=None,           # manual trigger; change to e.g. "@daily" for scheduled runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,       # prevent concurrent runs clobbering MinIO writes
    tags=["vct", "medallion", "bronze", "silver", "gold"],
)
def vct_pipeline():

    # ──────────────────────────────────────────────────────────────────────────
    # Task 1 — Bronze Ingestion
    # Discovers Tier-1 VCT events from vlr.gg and writes raw JSON to MinIO.
    # ──────────────────────────────────────────────────────────────────────────
    @task(
        task_id="bronze_ingestion",
        retries=2,
        retry_delay_seconds=60,
    )
    def bronze_ingestion() -> None:
        from src.ingestion.bronze_vct_backfill import run_backfill
        run_backfill()

    # ──────────────────────────────────────────────────────────────────────────
    # Task 2 — Silver Transformation
    # Reads Bronze JSON, cleans + enriches, writes 11 Iceberg tables to Nessie.
    # ──────────────────────────────────────────────────────────────────────────
    @task(
        task_id="silver_transform",
        retries=1,
        retry_delay_seconds=30,
    )
    def silver_transform() -> None:
        from src.transformation.silver.runner import run_pipeline
        run_pipeline()

    # ──────────────────────────────────────────────────────────────────────────
    # Task 3 — Gold Transformation  (placeholder)
    # Will aggregate Silver tables into 7 analytics-ready Gold tables.
    # TODO: wire up when feature/gold-layer is merged into develop.
    # ──────────────────────────────────────────────────────────────────────────
    @task(
        task_id="gold_transform",
        retries=1,
        retry_delay_seconds=30,
    )
    def gold_transform() -> None:
        log.warning(
            "gold_transform is a placeholder — "
            "feature/gold-layer has not been merged yet. "
            "Skipping Gold layer."
        )

    # ── Dependency chain ──────────────────────────────────────────────────────
    bronze_ingestion() >> silver_transform() >> gold_transform()


# Instantiate the DAG
vct_pipeline()
