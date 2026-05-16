"""
runner.py
=========
Silver Layer pipeline entry point — VCT Esports

Exposes three Airflow-callable sub-functions that map to the task graph:

  run_dims()         → dim_events, dim_teams, dim_team_roster, dim_players
  run_facts_match()  → fact_series, fact_player_stats, fact_map_scores,
                        fact_round_results, fact_map_picks_bans
  run_facts_meta()   → fact_player_agent_stats, fact_event_standings,
                        fact_team_placements, fact_team_transactions

  run_pipeline()     → runs all three in sequence (for direct CLI use)

Airflow task dependency:
  bronze_ingestion >> [silver_dims, silver_facts_meta]
  silver_dims      >> silver_facts_match
  [silver_dims, silver_facts_match, silver_facts_meta] >> gold_*

Note: run_facts_match re-reads Bronze events independently to build the
series_event_lookup — keeps each task self-contained across process boundaries.
  Supplemental fact_player_agent_stats, fact_event_standings,
               fact_team_placements, fact_team_transactions

Run with:
    python -m src.transformation.silver.runner
  or:
    python src/transformation/silver/runner.py
"""

from __future__ import annotations

import logging

from .common import (
    SILVER_NS,
    build_iceberg_catalog,
    build_s3_client,
    build_spark,
    ensure_bucket,
    ensure_namespace,
    quarantine_violations,
    read_bronze_prefix,
    write_iceberg_table,
)
from .dims import (
    transform_dim_events,
    transform_dim_players,
    transform_dim_team_roster,
    transform_dim_teams,
)
from .facts_match import (
    transform_fact_map_picks_bans,
    transform_fact_map_scores,
    transform_fact_player_stats,
    transform_fact_round_results,
    transform_fact_series,
)
from .facts_meta import (
    transform_fact_event_standings,
    transform_fact_player_agent_stats,
    transform_fact_team_placements,
    transform_fact_team_transactions,
)

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
# Table registry
# Format: (table_name, partition_cols)
# ---------------------------------------------------------------------------

_DIM_TABLES = [
    ("dim_events",      ("region",)),
    ("dim_teams",       ()),
    ("dim_team_roster", ()),
    ("dim_players",     ()),
]

_FACT_MATCH_TABLES = [
    ("fact_series",         ("event_id",)),
    ("fact_player_stats",   ("event_id", "map_name")),
    ("fact_map_scores",     ("event_id",)),
    ("fact_round_results",  ("event_id",)),
    ("fact_map_picks_bans", ("event_id",)),
]

_FACT_META_TABLES = [
    ("fact_player_agent_stats", ()),
    ("fact_event_standings",    ("event_id",)),
    ("fact_team_placements",    ()),
    ("fact_team_transactions",  ()),
]


# ---------------------------------------------------------------------------
# Airflow-callable sub-functions
# ---------------------------------------------------------------------------

def run_dims() -> None:
    """silver_dims task — reads Bronze master data, writes 4 dimension tables."""
    log.info("=== silver_dims ===")
    s3 = build_s3_client()
    ensure_bucket(s3, "silver-vct-data")

    dim_events,      viol_dim_events      = transform_dim_events(read_bronze_prefix(s3, "events/raw/"))
    dim_teams,       viol_dim_teams       = transform_dim_teams(read_bronze_prefix(s3, "team_info/raw/"))
    dim_team_roster, viol_dim_team_roster = transform_dim_team_roster(read_bronze_prefix(s3, "team_roster/raw/"))
    dim_players,     viol_dim_players     = transform_dim_players(read_bronze_prefix(s3, "player_profiles/raw/"))

    for name, vdf in [
        ("dim_events", viol_dim_events), ("dim_teams", viol_dim_teams),
        ("dim_team_roster", viol_dim_team_roster), ("dim_players", viol_dim_players),
    ]:
        quarantine_violations(s3, name, vdf)

    spark   = build_spark()
    catalog = build_iceberg_catalog()
    ensure_namespace(catalog)

    write_iceberg_table(spark, "dim_events",      dim_events,      ("region",))
    write_iceberg_table(spark, "dim_teams",       dim_teams,       ())
    write_iceberg_table(spark, "dim_team_roster", dim_team_roster, ())
    write_iceberg_table(spark, "dim_players",     dim_players,     ())
    spark.stop()


def run_facts_match() -> None:
    """
    silver_facts_match task — reads Bronze match data + Bronze events (for
    series_event_lookup), writes 5 match-activity fact tables.
    Depends on silver_dims completing first (DAG enforces this).
    """
    log.info("=== silver_facts_match ===")
    s3 = build_s3_client()

    # Re-read Bronze events here to build the series_event_lookup independently
    raw_events = read_bronze_prefix(s3, "events/raw/")
    dim_events, _ = transform_dim_events(raw_events)

    fact_series, viol_series, series_event_lookup = transform_fact_series(
        read_bronze_prefix(s3, "series/raw/"), dim_events
    )
    fact_player_stats,   viol_ps  = transform_fact_player_stats(read_bronze_prefix(s3, "player_stats/raw/"),   series_event_lookup)
    fact_map_scores,     viol_ms  = transform_fact_map_scores(read_bronze_prefix(s3, "map_scores/raw/"),       series_event_lookup)
    fact_round_results,  viol_rr  = transform_fact_round_results(read_bronze_prefix(s3, "round_results/raw/"), series_event_lookup)
    fact_map_picks_bans, viol_mpb = transform_fact_map_picks_bans(read_bronze_prefix(s3, "map_picks_bans/raw/"), series_event_lookup)

    for name, vdf in [
        ("fact_series", viol_series), ("fact_player_stats", viol_ps),
        ("fact_map_scores", viol_ms), ("fact_round_results", viol_rr),
        ("fact_map_picks_bans", viol_mpb),
    ]:
        quarantine_violations(s3, name, vdf)

    spark   = build_spark()
    catalog = build_iceberg_catalog()
    ensure_namespace(catalog)

    write_iceberg_table(spark, "fact_series",         fact_series,         ("event_id",))
    write_iceberg_table(spark, "fact_player_stats",   fact_player_stats,   ("event_id", "map_name"))
    write_iceberg_table(spark, "fact_map_scores",     fact_map_scores,     ("event_id",))
    write_iceberg_table(spark, "fact_round_results",  fact_round_results,  ("event_id",))
    write_iceberg_table(spark, "fact_map_picks_bans", fact_map_picks_bans, ("event_id",))
    spark.stop()


def run_facts_meta() -> None:
    """silver_facts_meta task — reads Bronze supplemental data, writes 4 fact tables."""
    log.info("=== silver_facts_meta ===")
    s3 = build_s3_client()

    fact_player_agent_stats, viol_pas = transform_fact_player_agent_stats(read_bronze_prefix(s3, "player_agent_stats/raw/"))
    fact_event_standings,    viol_es  = transform_fact_event_standings(read_bronze_prefix(s3, "event_standings/raw/"))
    fact_team_placements,    viol_tp  = transform_fact_team_placements(read_bronze_prefix(s3, "team_placements/raw/"))
    fact_team_transactions,  viol_tt  = transform_fact_team_transactions(read_bronze_prefix(s3, "team_transactions/raw/"))

    for name, vdf in [
        ("fact_player_agent_stats", viol_pas), ("fact_event_standings", viol_es),
        ("fact_team_placements", viol_tp), ("fact_team_transactions", viol_tt),
    ]:
        quarantine_violations(s3, name, vdf)

    spark   = build_spark()
    catalog = build_iceberg_catalog()
    ensure_namespace(catalog)

    write_iceberg_table(spark, "fact_player_agent_stats", fact_player_agent_stats, ())
    write_iceberg_table(spark, "fact_event_standings",    fact_event_standings,    ("event_id",))
    write_iceberg_table(spark, "fact_team_placements",    fact_team_placements,    ())
    write_iceberg_table(spark, "fact_team_transactions",  fact_team_transactions,  ())
    spark.stop()


# ---------------------------------------------------------------------------
# Full pipeline (CLI / direct run)
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    log.info("=" * 60)
    log.info("Silver Layer Pipeline — VCT Esports (full run)")
    log.info("=" * 60)
    run_dims()
    run_facts_meta()
    run_facts_match()
    log.info("Silver pipeline complete.")


def _run_pipeline_original() -> None:
    """Original monolithic pipeline — kept for reference."""
    stats: dict[str, int] = {t: 0 for t, _ in _DIM_TABLES + _FACT_MATCH_TABLES + _FACT_META_TABLES}
    stats["quarantine_total"] = 0

    s3 = build_s3_client()
    ensure_bucket(s3, "silver-vct-data")

    # ── Step 1: Read all Bronze prefixes ─────────────────────────────────────
    log.info("Step 1: Reading Bronze data from MinIO …")

    raw = {
        "events":             read_bronze_prefix(s3, "events/raw/"),
        "series":             read_bronze_prefix(s3, "series/raw/"),
        "player_stats":       read_bronze_prefix(s3, "player_stats/raw/"),
        "map_scores":         read_bronze_prefix(s3, "map_scores/raw/"),
        "round_results":      read_bronze_prefix(s3, "round_results/raw/"),
        "map_picks_bans":     read_bronze_prefix(s3, "map_picks_bans/raw/"),
        "team_info":          read_bronze_prefix(s3, "team_info/raw/"),
        "team_roster":        read_bronze_prefix(s3, "team_roster/raw/"),
        "player_profiles":    read_bronze_prefix(s3, "player_profiles/raw/"),
        "player_agent_stats": read_bronze_prefix(s3, "player_agent_stats/raw/"),
        "event_standings":    read_bronze_prefix(s3, "event_standings/raw/"),
        "team_placements":    read_bronze_prefix(s3, "team_placements/raw/"),
        "team_transactions":  read_bronze_prefix(s3, "team_transactions/raw/"),
    }

    # ── Step 2: Transform ────────────────────────────────────────────────────
    log.info("Step 2: Transforming …")

    # --- Dimensions ---
    dim_events,      viol_dim_events      = transform_dim_events(raw["events"])
    dim_teams,       viol_dim_teams       = transform_dim_teams(raw["team_info"])
    dim_team_roster, viol_dim_team_roster = transform_dim_team_roster(raw["team_roster"])
    dim_players,     viol_dim_players     = transform_dim_players(raw["player_profiles"])

    # --- fact_series (also builds series_event_lookup for downstream enrichment) ---
    fact_series, viol_fact_series, series_event_lookup = transform_fact_series(
        raw["series"], dim_events
    )

    # --- Facts (match-level, enriched with event context) ---
    fact_player_stats,   viol_fact_player_stats   = transform_fact_player_stats(raw["player_stats"],   series_event_lookup)
    fact_map_scores,     viol_fact_map_scores     = transform_fact_map_scores(raw["map_scores"],       series_event_lookup)
    fact_round_results,  viol_fact_round_results  = transform_fact_round_results(raw["round_results"], series_event_lookup)
    fact_map_picks_bans, viol_fact_map_picks_bans = transform_fact_map_picks_bans(raw["map_picks_bans"], series_event_lookup)

    # --- Supplemental facts ---
    fact_player_agent_stats, viol_fact_player_agent_stats = transform_fact_player_agent_stats(raw["player_agent_stats"])
    fact_event_standings,    viol_fact_event_standings    = transform_fact_event_standings(raw["event_standings"])
    fact_team_placements,    viol_fact_team_placements    = transform_fact_team_placements(raw["team_placements"])
    fact_team_transactions,  viol_fact_team_transactions  = transform_fact_team_transactions(raw["team_transactions"])

    # ── Step 3: Quarantine violations ────────────────────────────────────────
    log.info("Step 3: Routing PK violations to quarantine …")

    violation_pairs = [
        ("dim_events",             viol_dim_events),
        ("dim_teams",              viol_dim_teams),
        ("dim_team_roster",        viol_dim_team_roster),
        ("dim_players",            viol_dim_players),
        ("fact_series",            viol_fact_series),
        ("fact_player_stats",      viol_fact_player_stats),
        ("fact_map_scores",        viol_fact_map_scores),
        ("fact_round_results",     viol_fact_round_results),
        ("fact_map_picks_bans",    viol_fact_map_picks_bans),
        ("fact_player_agent_stats",viol_fact_player_agent_stats),
        ("fact_event_standings",   viol_fact_event_standings),
        ("fact_team_placements",   viol_fact_team_placements),
        ("fact_team_transactions", viol_fact_team_transactions),
    ]
    for table_name, viol_df in violation_pairs:
        stats["quarantine_total"] += quarantine_violations(s3, table_name, viol_df)

    # ── Step 4: Write Iceberg tables ─────────────────────────────────────────
    log.info("Step 4: Writing Iceberg tables …")

    spark   = build_spark()
    catalog = build_iceberg_catalog()
    ensure_namespace(catalog)

    write_jobs = [
        ("dim_events",             dim_events,             ("region",)),
        ("dim_teams",              dim_teams,              ()),
        ("dim_team_roster",        dim_team_roster,        ()),
        ("dim_players",            dim_players,            ()),
        ("fact_series",            fact_series,            ("event_id",)),
        ("fact_player_stats",      fact_player_stats,      ("event_id", "map_name")),
        ("fact_map_scores",        fact_map_scores,        ("event_id",)),
        ("fact_round_results",     fact_round_results,     ("event_id",)),
        ("fact_map_picks_bans",    fact_map_picks_bans,    ("event_id",)),
        ("fact_player_agent_stats",fact_player_agent_stats,()),
        ("fact_event_standings",   fact_event_standings,   ("event_id",)),
        ("fact_team_placements",   fact_team_placements,   ()),
        ("fact_team_transactions", fact_team_transactions, ()),
    ]

    for table_name, df, partition_cols in write_jobs:
        try:
            stats[table_name] = write_iceberg_table(spark, table_name, df, partition_cols)
        except Exception as exc:
            log.error("  FAILED silver.%s: %s", table_name, exc, exc_info=True)

    spark.stop()

    # ── Summary ──────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Silver pipeline complete.")
    log.info("")
    log.info("  Dimensions")
    log.info("    dim_events             : %d rows", stats["dim_events"])
    log.info("    dim_teams              : %d rows", stats["dim_teams"])
    log.info("    dim_team_roster        : %d rows", stats["dim_team_roster"])
    log.info("    dim_players            : %d rows", stats["dim_players"])
    log.info("")
    log.info("  Facts (match-level)")
    log.info("    fact_series            : %d rows", stats["fact_series"])
    log.info("    fact_player_stats      : %d rows", stats["fact_player_stats"])
    log.info("    fact_map_scores        : %d rows", stats["fact_map_scores"])
    log.info("    fact_round_results     : %d rows", stats["fact_round_results"])
    log.info("    fact_map_picks_bans    : %d rows", stats["fact_map_picks_bans"])
    log.info("")
    log.info("  Facts (supplemental)")
    log.info("    fact_player_agent_stats: %d rows", stats["fact_player_agent_stats"])
    log.info("    fact_event_standings   : %d rows", stats["fact_event_standings"])
    log.info("    fact_team_placements   : %d rows", stats["fact_team_placements"])
    log.info("    fact_team_transactions : %d rows", stats["fact_team_transactions"])
    log.info("")
    log.info("  PK violations quarantined: %d rows total", stats["quarantine_total"])
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_pipeline()
