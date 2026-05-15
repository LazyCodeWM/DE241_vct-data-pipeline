"""
dims.py
=======
Silver dimension table transformations.

Each function takes a raw Bronze DataFrame and returns (clean_df, violations_df).
All heavy lifting (type coercion, dedup, PK validation) happens here in Polars
before the data is handed off to the Iceberg writer in runner.py.

Tables:
  dim_events        ← events/raw/          partition: region
  dim_teams         ← team_info/raw/       partition: -
  dim_team_roster   ← team_roster/raw/     partition: -
  dim_players       ← player_profiles/raw/ partition: -
"""

from __future__ import annotations

import polars as pl

from .common import split_pk_violations


# ---------------------------------------------------------------------------
# dim_events
# ---------------------------------------------------------------------------

def transform_dim_events(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain   : 1 row per event
    PK      : event_id
    partition: region
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("event_id").cast(pl.Int64),
            pl.col("start_date").cast(pl.Date),
            pl.col("end_date").cast(pl.Date),
        ])
    )

    return split_pk_violations(clean, ["event_id"])


# ---------------------------------------------------------------------------
# dim_teams
# ---------------------------------------------------------------------------

def transform_dim_teams(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain   : 1 row per team
    PK      : team_id
    partition: -
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("team_id").cast(pl.Int64),
            pl.col("is_active").cast(pl.Boolean),
        ])
    )

    return split_pk_violations(clean, ["team_id"])


# ---------------------------------------------------------------------------
# dim_team_roster
# ---------------------------------------------------------------------------

def transform_dim_team_roster(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain   : 1 row per (team_id, player_id)
    PK      : (team_id, player_id)
    partition: -

    Bronze stores roster as a JSON array per team — read_bronze_prefix()
    flattens these into individual rows before this function is called.
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("team_id").cast(pl.Int64),
            pl.col("player_id").cast(pl.Int64, strict=False),
            pl.col("is_captain").cast(pl.Boolean),
        ])
    )

    return split_pk_violations(clean, ["team_id", "player_id"])


# ---------------------------------------------------------------------------
# dim_players
# ---------------------------------------------------------------------------

def transform_dim_players(
    df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain   : 1 row per player
    PK      : player_id
    partition: -
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("player_id").cast(pl.Int64),
        ])
    )

    return split_pk_violations(clean, ["player_id"])
