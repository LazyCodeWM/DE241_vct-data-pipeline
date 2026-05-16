"""
vct_pipeline_dag.py
===================
Airflow DAG — VCT Data Pipeline (Medallion Architecture)

Task graph:

                        [start]
                           │
                           ▼
                  [bronze_ingestion]
                     │         │
                     ▼         ▼
            [silver_dims]   [silver_facts_meta]
                     │         │
                     ▼         │
           [silver_facts_match]│
                     │         │
                     └────┬────┘
                          ▼
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼               ▼
   [gold_players]   [gold_agents]   [gold_teams]   [gold_matches]
          │               │               │               │
          └───────────────┴───────────────┴───────────────┘
                          │
                          ▼
                        [end]

Schedule : None  (manual trigger)
Catchup  : False
"""

from __future__ import annotations

import os
import sys

# Ensure project root (/opt/airflow) is on sys.path so 'src' is importable.
# DAG lives at /opt/airflow/dags/vct_pipeline_dag.py → parent = /opt/airflow
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="vct_pipeline",
    description="VCT Medallion Pipeline: Bronze → Silver → Gold",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vct", "medallion", "bronze", "silver", "gold"],
)
def vct_pipeline():

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Bronze ────────────────────────────────────────────────────────────────
    @task(task_id="bronze_ingestion", retries=2, retry_delay=timedelta(seconds=60))
    def bronze_ingestion() -> None:
        from src.ingestion.bronze_vct_backfill import run_backfill
        run_backfill()

    # ── Silver ────────────────────────────────────────────────────────────────
    @task(task_id="silver_dims", retries=1, retry_delay=timedelta(seconds=30))
    def silver_dims() -> None:
        from src.transformation.silver.runner import run_dims
        run_dims()

    @task(task_id="silver_facts_meta", retries=1, retry_delay=timedelta(seconds=30))
    def silver_facts_meta() -> None:
        from src.transformation.silver.runner import run_facts_meta
        run_facts_meta()

    @task(task_id="silver_facts_match", retries=1, retry_delay=timedelta(seconds=30))
    def silver_facts_match() -> None:
        from src.transformation.silver.runner import run_facts_match
        run_facts_match()

    # ── Gold ──────────────────────────────────────────────────────────────────
    @task(task_id="gold_players", retries=1, retry_delay=timedelta(seconds=30))
    def gold_players() -> None:
        from src.transformation.gold.runner import run_players
        run_players()

    @task(task_id="gold_agents", retries=1, retry_delay=timedelta(seconds=30))
    def gold_agents() -> None:
        from src.transformation.gold.runner import run_agents
        run_agents()

    @task(task_id="gold_teams", retries=1, retry_delay=timedelta(seconds=30))
    def gold_teams() -> None:
        from src.transformation.gold.runner import run_teams
        run_teams()

    @task(task_id="gold_matches", retries=1, retry_delay=timedelta(seconds=30))
    def gold_matches() -> None:
        from src.transformation.gold.runner import run_matches
        run_matches()

    # ── Dependencies ──────────────────────────────────────────────────────────
    t_bronze       = bronze_ingestion()
    t_dims         = silver_dims()
    t_facts_meta   = silver_facts_meta()
    t_facts_match  = silver_facts_match()
    t_players      = gold_players()
    t_agents       = gold_agents()
    t_teams        = gold_teams()
    t_matches      = gold_matches()

    all_gold = [t_players, t_agents, t_teams, t_matches]

    start >> t_bronze >> [t_dims, t_facts_meta]
    t_dims >> t_facts_match
    [t_dims, t_facts_match, t_facts_meta] >> t_players
    [t_dims, t_facts_match, t_facts_meta] >> t_agents
    [t_dims, t_facts_match, t_facts_meta] >> t_teams
    [t_dims, t_facts_match, t_facts_meta] >> t_matches
    all_gold >> end


vct_pipeline()
