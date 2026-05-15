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
# Table registry
# Format: (table_name, build_fn, partition_cols)
# ---------------------------------------------------------------------------

# Registered after Silver views are loaded — build_fn receives spark session
_GOLD_TABLES = [
    # Players
    ("player_performance",     build_player_performance,    ("event_id",)),
    ("player_map_performance", build_player_map_performance, ()),
    # Agents
    ("agent_meta",             build_agent_meta,             ("map_name",)),
    ("agent_player_affinity",  build_agent_player_affinity,  ()),
    # Teams
    ("team_standings",         build_team_standings,         ("event_id",)),
    ("team_map_performance",   build_team_map_performance,   ()),
    # Matches
    ("match_summary",          build_match_summary,          ("event_id",)),
]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    log.info("=" * 60)
    log.info("Gold Layer Pipeline — VCT Esports")
    log.info("Namespace : %s", GOLD_NS)
    log.info("=" * 60)

    stats: dict[str, int] = {name: 0 for name, _, _ in _GOLD_TABLES}

    # ── Step 1: Build Spark + catalog ────────────────────────────────────────
    spark   = build_spark()
    catalog = build_iceberg_catalog()
    ensure_gold_namespace(catalog)

    # ── Step 2: Load Silver tables as temp views ──────────────────────────────
    log.info("Step 1: Loading Silver tables as Spark temp views …")
    load_silver_views(spark)

    # ── Step 3: Build + write each Gold table ─────────────────────────────────
    log.info("Step 2: Building and writing Gold tables …")

    for table_name, build_fn, partition_cols in _GOLD_TABLES:
        try:
            df = build_fn(spark)
            stats[table_name] = write_gold_table(spark, table_name, df, partition_cols)
        except Exception as exc:
            log.error("  FAILED gold.%s: %s", table_name, exc, exc_info=True)

    spark.stop()

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Gold pipeline complete.")
    log.info("")
    log.info("  Players")
    log.info("    player_performance     : %d rows", stats["player_performance"])
    log.info("    player_map_performance : %d rows", stats["player_map_performance"])
    log.info("")
    log.info("  Agents")
    log.info("    agent_meta             : %d rows", stats["agent_meta"])
    log.info("    agent_player_affinity  : %d rows", stats["agent_player_affinity"])
    log.info("")
    log.info("  Teams")
    log.info("    team_standings         : %d rows", stats["team_standings"])
    log.info("    team_map_performance   : %d rows", stats["team_map_performance"])
    log.info("")
    log.info("  Matches")
    log.info("    match_summary          : %d rows", stats["match_summary"])
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_pipeline()
