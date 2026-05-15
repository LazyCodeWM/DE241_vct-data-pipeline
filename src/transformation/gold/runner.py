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
    build_iceberg_catalog,
    build_spark,
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

def run_players() -> None:
    """gold_players task — player_performance, player_map_performance."""
    log.info("=== gold_players ===")
    spark = build_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_player_stats"])
    write_gold_table(spark, "player_performance",     build_player_performance(spark),     ("event_id",))
    write_gold_table(spark, "player_map_performance", build_player_map_performance(spark), ())
    spark.stop()


def run_agents() -> None:
    """gold_agents task — agent_meta, agent_player_affinity."""
    log.info("=== gold_agents ===")
    spark = build_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_player_stats", "fact_map_scores"])
    write_gold_table(spark, "agent_meta",            build_agent_meta(spark),            ("map_name",))
    write_gold_table(spark, "agent_player_affinity", build_agent_player_affinity(spark), ())
    spark.stop()


def run_teams() -> None:
    """gold_teams task — team_standings, team_map_performance."""
    log.info("=== gold_teams ===")
    spark = build_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_series", "fact_map_scores"])
    write_gold_table(spark, "team_standings",       build_team_standings(spark),       ("event_id",))
    write_gold_table(spark, "team_map_performance", build_team_map_performance(spark), ())
    spark.stop()


def run_matches() -> None:
    """gold_matches task — match_summary."""
    log.info("=== gold_matches ===")
    spark = build_spark()
    ensure_gold_namespace(build_iceberg_catalog())
    load_silver_views(spark, ["fact_series", "fact_map_scores"])
    write_gold_table(spark, "match_summary", build_match_summary(spark), ("event_id",))
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
