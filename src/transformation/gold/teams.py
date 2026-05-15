"""
teams.py
========
Gold layer — team aggregation tables.

Tables:
  team_standings        ← match/map record ต่อทีม ต่อ tournament
                          grain : (event_id, team_id)
                          source: silver_fact_series

  team_map_performance  ← win rate ทีมต่อ map + attacker/defender side stats
                          grain : (team_id, map_name)
                          source: silver_fact_map_scores

Requires silver temp views to be registered via load_silver_views() first.

Both tables use a UNION ALL pattern to unpivot team1/team2 columns
into per-team rows before aggregating.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame


def build_team_standings(spark: SparkSession) -> DataFrame:
    """
    Win/loss record per team per tournament.
    UNION ALL flattens team1/team2 columns into one row per team per match,
    then aggregates up to tournament level.
    """
    return spark.sql("""
        WITH team_match AS (
            -- team1 perspective
            SELECT
                match_id,
                event_id,
                event_name,
                region,
                team1_id                                        AS team_id,
                team1_name                                      AS team_name,
                team1_score                                     AS maps_won,
                team2_score                                     AS maps_lost,
                CASE WHEN team1_score > team2_score THEN 1
                     ELSE 0 END                                 AS match_won
            FROM silver_fact_series
            WHERE team1_id IS NOT NULL

            UNION ALL

            -- team2 perspective
            SELECT
                match_id,
                event_id,
                event_name,
                region,
                team2_id                                        AS team_id,
                team2_name                                      AS team_name,
                team2_score                                     AS maps_won,
                team1_score                                     AS maps_lost,
                CASE WHEN team2_score > team1_score THEN 1
                     ELSE 0 END                                 AS match_won
            FROM silver_fact_series
            WHERE team2_id IS NOT NULL
        )
        SELECT
            event_id,
            event_name,
            region,
            team_id,
            team_name,

            COUNT(match_id)                                     AS matches_played,
            SUM(match_won)                                      AS matches_won,
            SUM(1 - match_won)                                  AS matches_lost,
            ROUND(
                CAST(SUM(match_won) AS DOUBLE) /
                NULLIF(COUNT(match_id), 0)
            , 3)                                                AS match_win_rate,

            SUM(maps_won)                                       AS maps_won,
            SUM(maps_lost)                                      AS maps_lost,
            ROUND(
                CAST(SUM(maps_won) AS DOUBLE) /
                NULLIF(SUM(maps_won) + SUM(maps_lost), 0)
            , 3)                                                AS map_win_rate

        FROM team_match
        WHERE event_id IS NOT NULL
        GROUP BY event_id, event_name, region, team_id, team_name
        ORDER BY event_id, matches_won DESC
    """)


def build_team_map_performance(spark: SparkSession) -> DataFrame:
    """
    Per-team per-map win rate and side performance (attacker vs defender).
    UNION ALL flattens team1/team2 columns before aggregating.
    """
    return spark.sql("""
        WITH team_map AS (
            -- team1 perspective
            SELECT
                team1_id                                        AS team_id,
                team1_name                                      AS team_name,
                map_name,
                team1_is_winner                                 AS is_winner,
                COALESCE(team1_attacker_rounds, 0)              AS att_rounds_won,
                COALESCE(team1_defender_rounds, 0)              AS def_rounds_won,
                COALESCE(team2_attacker_rounds, 0)              AS opp_att_rounds,
                COALESCE(team2_defender_rounds, 0)              AS opp_def_rounds
            FROM silver_fact_map_scores
            WHERE team1_id IS NOT NULL

            UNION ALL

            -- team2 perspective
            SELECT
                team2_id                                        AS team_id,
                team2_name                                      AS team_name,
                map_name,
                team2_is_winner                                 AS is_winner,
                COALESCE(team2_attacker_rounds, 0)              AS att_rounds_won,
                COALESCE(team2_defender_rounds, 0)              AS def_rounds_won,
                COALESCE(team1_attacker_rounds, 0)              AS opp_att_rounds,
                COALESCE(team1_defender_rounds, 0)              AS opp_def_rounds
            FROM silver_fact_map_scores
            WHERE team2_id IS NOT NULL
        )
        SELECT
            team_id,
            team_name,
            map_name,

            COUNT(*)                                            AS maps_played,
            SUM(CASE WHEN is_winner = true  THEN 1 ELSE 0 END) AS maps_won,
            SUM(CASE WHEN is_winner = false THEN 1 ELSE 0 END) AS maps_lost,
            ROUND(
                CAST(SUM(CASE WHEN is_winner = true THEN 1 ELSE 0 END) AS DOUBLE) /
                NULLIF(COUNT(*), 0)
            , 3)                                                AS map_win_rate,

            SUM(att_rounds_won)                                 AS total_att_rounds_won,
            SUM(def_rounds_won)                                 AS total_def_rounds_won,
            ROUND(
                CAST(SUM(att_rounds_won) AS DOUBLE) /
                NULLIF(SUM(att_rounds_won) + SUM(opp_att_rounds), 0)
            , 3)                                                AS att_round_win_rate,
            ROUND(
                CAST(SUM(def_rounds_won) AS DOUBLE) /
                NULLIF(SUM(def_rounds_won) + SUM(opp_def_rounds), 0)
            , 3)                                                AS def_round_win_rate

        FROM team_map
        WHERE team_id IS NOT NULL
          AND map_name IS NOT NULL
        GROUP BY team_id, team_name, map_name
        ORDER BY team_id, maps_played DESC
    """)
