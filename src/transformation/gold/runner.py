"""
runner.py
=========
Gold Layer pipeline entry point — VCT Esports

Reads Silver Iceberg tables, runs aggregation transforms,
writes 7 Gold tables back to Nessie under the 'gold' namespace.

Run with:
    python -m src.transformation.gold.runner
  or:
    python src/transformation/gold/runner.py
"""

from __future__ import annotations

import logging

from .agents import build_agent_meta, build_agent_player_affinity
from .common import (
    GOLD_NS,
    build_gold_spark,
    build_iceberg_catalog,
    ensure_gold_namespace,
    load_silver_views,
    write_gold_table,
)
from .matches import build_match_summary
from .players import build_player_map_performance, build_player_performance
from .teams import build_team_map_performance, build_team_standings

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("gold_vct_pipeline")

# ---------------------------------------------------------------------------
# Airflow-callable sub-functions (one per Gold task group)
# ---------------------------------------------------------------------------

def _try_write(spark, table_name, build_fn, partition_cols, depends_on: str = "") -> None:
    """Write a Gold table, skipping if a required Silver view was not registered."""
    if depends_on and not spark.catalog.tableExists(f"silver_{depends_on}"):
        log.warning(
            "  Skipped gold.%s — silver_%s not available (no data in Bronze)",
            table_name, depends_on,
        )
        return
    write_gold_table(spark, table_name, build_fn(), partition_cols)


def run_players() -> None:
    """gold_players task — player_performance, player_map_performance."""
    log.info("=== gold_players ===")
    spark = build_gold_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_player_stats", "dim_events"])
    write_gold_table(spark, "player_performance",     build_player_performance(spark),     ("event_id",))
    write_gold_table(spark, "player_map_performance", build_player_map_performance(spark), ())
    spark.stop()


def run_agents() -> None:
    """gold_agents task — agent_meta, agent_player_affinity."""
    log.info("=== gold_agents ===")
    spark = build_gold_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_player_stats", "fact_map_scores"])
    _try_write(spark, "agent_meta",            lambda: build_agent_meta(spark),            ("map_name",), "fact_map_scores")
    write_gold_table(spark, "agent_player_affinity", build_agent_player_affinity(spark), ())
    spark.stop()


def run_teams() -> None:
    """gold_teams task — team_standings, team_map_performance."""
    log.info("=== gold_teams ===")
    spark = build_gold_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_series", "fact_map_scores", "dim_teams"])
    write_gold_table(spark, "team_standings",       build_team_standings(spark),       ("event_id",))
    _try_write(spark, "team_map_performance", lambda: build_team_map_performance(spark), (), "fact_map_scores")
    spark.stop()


def run_matches() -> None:
    """gold_matches task — match_summary."""
    log.info("=== gold_matches ===")
    spark = build_gold_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_series", "fact_map_scores"])
    _try_write(spark, "match_summary", lambda: build_match_summary(spark), ("event_id",), "fact_map_scores")
    spark.stop()


# ---------------------------------------------------------------------------
# Full pipeline (CLI / direct run)
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    log.info("=" * 60)
    log.info("Gold Layer Pipeline — VCT Esports (full run)")
    log.info("=" * 60)
    run_players()
    run_agents()
    run_teams()
    run_matches()
    log.info("Gold pipeline complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_pipeline()
