"""
facts_match.py
==============
Silver match-activity fact table transformations.

Each function takes raw Bronze DataFrame(s) and returns (clean_df, violations_df).
Tables that need event context (event_id, region) receive a prebuilt
series_event_lookup from runner.py — no duplicate join logic across modules.

Tables:
  fact_series          ← series/raw/          partition: event_id
  fact_player_stats    ← player_stats/raw/    partition: event_id, map_name
  fact_map_scores      ← map_scores/raw/      partition: event_id
  fact_round_results   ← round_results/raw/   partition: event_id
  fact_map_picks_bans  ← map_picks_bans/raw/  partition: event_id
"""

from __future__ import annotations

import polars as pl

from .common import split_pk_violations


# ---------------------------------------------------------------------------
# Shared enrichment helper
# ---------------------------------------------------------------------------

def _enrich_with_event(
    df: pl.DataFrame,
    series_event_lookup: pl.DataFrame,
) -> pl.DataFrame:
    """
    Left-join df (which has match_id) with series_event_lookup
    to attach event_id and region.
    """
    return df.join(series_event_lookup, on="match_id", how="left")


# ---------------------------------------------------------------------------
# fact_series
# ---------------------------------------------------------------------------

def transform_fact_series(
    df: pl.DataFrame,
    df_events: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per match
    PK       : match_id
    partition: event_id

    Also returns series_event_lookup (match_id → event_id, region) for use
    by the other fact transforms — built here to avoid re-reading events.

    Returns (clean_df, violations_df, series_event_lookup)
    """
    if df.is_empty():
        empty_lookup = pl.DataFrame(schema={"match_id": pl.Int64, "event_id": pl.Int64, "region": pl.Utf8})
        return df, df, empty_lookup

    # Flatten nested team structs
    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("match_id").cast(pl.Int64),
            pl.col("match_date").cast(pl.Date),
        ])
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

    # Build series → event_id lookup via event_name → events.name
    events_lookup = (
        df_events
        .select(["event_id", "name", "region"])
        .rename({"name": "_event_name_key"})
    )
    clean = clean.join(
        events_lookup,
        left_on="event_name",
        right_on="_event_name_key",
        how="left",
    ).with_columns(pl.col("event_id").cast(pl.Int64))

    # Build the lookup for downstream fact tables
    series_event_lookup = clean.select(["match_id", "event_id", "region"])

    clean_df, violations = split_pk_violations(clean, ["match_id"])
    return clean_df, violations, series_event_lookup


# ---------------------------------------------------------------------------
# fact_player_stats
# ---------------------------------------------------------------------------

def transform_fact_player_stats(
    df: pl.DataFrame,
    series_event_lookup: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (match_id, game_id, player_id)
    PK       : (match_id, game_id, player_id)
    partition: event_id, map_name

    `rating` renamed to r_rating — reserved word in several SQL dialects.
    game_id cast with strict=False — API occasionally returns it as string.
    """
    if df.is_empty():
        return df, df

    clean = (
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

    clean = _enrich_with_event(clean, series_event_lookup)

    return split_pk_violations(clean, ["match_id", "game_id", "player_id"])


# ---------------------------------------------------------------------------
# fact_map_scores
# ---------------------------------------------------------------------------

def transform_fact_map_scores(
    df: pl.DataFrame,
    series_event_lookup: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (match_id, game_id)
    PK       : (match_id, game_id)
    partition: event_id
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("match_id").cast(pl.Int64),
            pl.col("map_index").cast(pl.Int32),
            pl.col("game_id").cast(pl.Int64, strict=False),
            pl.col("team1_id").cast(pl.Int64, strict=False),
            pl.col("team2_id").cast(pl.Int64, strict=False),
            pl.col("team1_score").cast(pl.Int32, strict=False),
            pl.col("team2_score").cast(pl.Int32, strict=False),
            pl.col("team1_attacker_rounds").cast(pl.Int32, strict=False),
            pl.col("team1_defender_rounds").cast(pl.Int32, strict=False),
            pl.col("team2_attacker_rounds").cast(pl.Int32, strict=False),
            pl.col("team2_defender_rounds").cast(pl.Int32, strict=False),
            pl.col("team1_is_winner").cast(pl.Boolean, strict=False),
            pl.col("team2_is_winner").cast(pl.Boolean, strict=False),
        ])
    )

    clean = _enrich_with_event(clean, series_event_lookup)

    return split_pk_violations(clean, ["match_id", "game_id"])


# ---------------------------------------------------------------------------
# fact_round_results
# ---------------------------------------------------------------------------

def transform_fact_round_results(
    df: pl.DataFrame,
    series_event_lookup: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (match_id, map_index, round_number)
    PK       : (match_id, map_index, round_number)
    partition: event_id
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("match_id").cast(pl.Int64),
            pl.col("map_index").cast(pl.Int32),
            pl.col("round_number").cast(pl.Int32),
            pl.col("score_team1").cast(pl.Int32, strict=False),
            pl.col("score_team2").cast(pl.Int32, strict=False),
            pl.col("winner_team_id").cast(pl.Int64, strict=False),
        ])
    )

    clean = _enrich_with_event(clean, series_event_lookup)

    return split_pk_violations(clean, ["match_id", "map_index", "round_number"])


# ---------------------------------------------------------------------------
# fact_map_picks_bans
# ---------------------------------------------------------------------------

def transform_fact_map_picks_bans(
    df: pl.DataFrame,
    series_event_lookup: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    grain    : 1 row per (match_id, action_type, map, team)
    PK       : (match_id, action_type, map, team)
    partition: event_id

    Natural key — each map can only be picked/banned once per match per team.
    """
    if df.is_empty():
        return df, df

    clean = (
        df
        .drop("ingested_at")
        .with_columns([
            pl.col("match_id").cast(pl.Int64),
        ])
    )

    clean = _enrich_with_event(clean, series_event_lookup)

    return split_pk_violations(clean, ["match_id", "action_type", "map", "team"])
