"""
facts_meta.py
=============
Silver supplemental / career / historical fact table transformations.

Tables in this module are not tied to individual matches — they capture
career-level or historical records. No match-level enrichment needed.

Tables:
  fact_player_agent_stats  ← player_agent_stats/raw/  partition: -
  fact_event_standings     ← event_standings/raw/     partition: event_id
  fact_team_placements     ← team_placements/raw/     partition: -
  fact_team_transactions   ← team_transactions/raw/   partition: -
"""

from __future__ import annotations

import polars as pl

from .common import split_pk_violations


# ---------------------------------------------------------------------------
# fact_player_agent_stats
# ---------------------------------------------------------------------------

def transform_fact_player_agent_stats(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (player_id, agent)
    PK       : (player_id, agent)
    partition: -

    Career-level stats — no event/match context.
    Fully overwritten on each pipeline run (no snapshotting).

    Bronze stores these as JSON arrays per player — read_bronze_prefix()
    flattens them into individual rows before this function is called.
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("player_id").cast(pl.Int64),
            pl.col("usage_count").cast(pl.Int32, strict=False),
            pl.col("usage_percent").cast(pl.Float32, strict=False),
            pl.col("rounds_played").cast(pl.Int32, strict=False),
            pl.col("rating").cast(pl.Float32, strict=False),
            pl.col("acs").cast(pl.Float32, strict=False),
            pl.col("kd").cast(pl.Float32, strict=False),
            pl.col("adr").cast(pl.Float32, strict=False),
            pl.col("kast").cast(pl.Float32, strict=False),
            pl.col("kills").cast(pl.Int32, strict=False),
            pl.col("deaths").cast(pl.Int32, strict=False),
            pl.col("assists").cast(pl.Int32, strict=False),
            pl.col("first_kills").cast(pl.Int32, strict=False),
            pl.col("first_deaths").cast(pl.Int32, strict=False),
        ])
        .rename({"rating": "r_rating"})
    )

    return split_pk_violations(clean, ["player_id", "agent"])


# ---------------------------------------------------------------------------
# fact_event_standings
# ---------------------------------------------------------------------------

def transform_fact_event_standings(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (event_id, stage_path, place)
    PK       : (event_id, stage_path, place)
    partition: event_id
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("event_id").cast(pl.Int64),
            pl.col("team_id").cast(pl.Int64, strict=False),
        ])
    )

    return split_pk_violations(clean, ["event_id", "stage_path", "place"])


# ---------------------------------------------------------------------------
# fact_team_placements
# ---------------------------------------------------------------------------

def transform_fact_team_placements(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (team_id, event_id)
    PK       : (team_id, event_id)
    partition: -
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("team_id").cast(pl.Int64),
            pl.col("event_id").cast(pl.Int64, strict=False),
        ])
    )

    return split_pk_violations(clean, ["team_id", "event_id"])


# ---------------------------------------------------------------------------
# fact_team_transactions
# ---------------------------------------------------------------------------

def transform_fact_team_transactions(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (team_id, player_id, transaction_date)
    PK       : (team_id, player_id, transaction_date)
    partition: -
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("team_id").cast(pl.Int64),
            pl.col("player_id").cast(pl.Int64, strict=False),
            pl.col("transaction_date").cast(pl.Date, strict=False),
        ])
    )

    return split_pk_violations(clean, ["team_id", "player_id", "transaction_date"])
