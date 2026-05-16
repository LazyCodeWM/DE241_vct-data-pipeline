"""
players.py
==========
Gold layer — player aggregation tables.

Tables:
  player_performance      ← stats รวมต่อผู้เล่น ต่อ tournament
                            grain : (event_id, player_id)
                            source: silver_fact_player_stats

  player_map_performance  ← stats ต่อผู้เล่น ต่อ map (career across all events)
                            grain : (player_id, map_name)
                            source: silver_fact_player_stats

Requires silver temp views to be registered via load_silver_views() first.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame


def build_player_performance(spark: SparkSession) -> DataFrame:
    """
    Aggregate per-map player stats up to tournament level.
    Includes match count, map count, and all key performance metrics.
    event_name is denormalized via join with silver_dim_events.
    """
    return spark.sql("""
        SELECT
            fps.event_id,
            de.name                                         AS event_name,
            fps.region,
            fps.player_id,
            fps.player_name,
            fps.team_id,
            fps.team_short,

            COUNT(DISTINCT fps.match_id)                    AS matches_played,
            COUNT(DISTINCT fps.game_id)                     AS maps_played,

            ROUND(AVG(fps.r_rating), 3)                     AS avg_rating,
            ROUND(AVG(fps.acs), 1)                          AS avg_acs,

            SUM(fps.kills)                                  AS total_kills,
            SUM(fps.deaths)                                 AS total_deaths,
            SUM(fps.assists)                                AS total_assists,
            ROUND(
                CAST(SUM(fps.kills) AS DOUBLE) /
                NULLIF(SUM(fps.deaths), 0)
            , 3)                                            AS kd_ratio,

            ROUND(AVG(fps.kast), 3)                         AS avg_kast,
            ROUND(AVG(fps.adr), 1)                          AS avg_adr,
            ROUND(AVG(fps.hs_pct), 3)                       AS avg_hs_pct,

            SUM(fps.fk)                                     AS total_first_kills,
            SUM(fps.fd)                                     AS total_first_deaths,
            ROUND(
                CAST(SUM(fps.fk) AS DOUBLE) /
                NULLIF(SUM(fps.fk) + SUM(fps.fd), 0)
            , 3)                                            AS fk_rate

        FROM silver_fact_player_stats fps
        LEFT JOIN silver_dim_events de ON fps.event_id = de.event_id
        WHERE fps.event_id IS NOT NULL
          AND fps.player_id IS NOT NULL
        GROUP BY
            fps.event_id, de.name, fps.region,
            fps.player_id, fps.player_name, fps.team_id, fps.team_short
    """)


def build_player_map_performance(spark: SparkSession) -> DataFrame:
    """
    Aggregate per-map player stats by map name (across all events).
    Useful for identifying player strengths on specific maps.
    """
    return spark.sql("""
        SELECT
            player_id,
            player_name,
            team_id,
            team_short,
            map_name,

            COUNT(DISTINCT game_id)                         AS maps_played,

            ROUND(AVG(r_rating), 3)                         AS avg_rating,
            ROUND(AVG(acs), 1)                              AS avg_acs,

            SUM(kills)                                      AS total_kills,
            SUM(deaths)                                     AS total_deaths,
            ROUND(
                CAST(SUM(kills) AS DOUBLE) /
                NULLIF(SUM(deaths), 0)
            , 3)                                            AS kd_ratio,

            ROUND(AVG(kast), 3)                             AS avg_kast,
            ROUND(AVG(adr), 1)                              AS avg_adr,
            ROUND(AVG(hs_pct), 3)                           AS avg_hs_pct

        FROM silver_fact_player_stats
        WHERE player_id IS NOT NULL
          AND map_name IS NOT NULL
        GROUP BY
            player_id, player_name, team_id, team_short, map_name
    """)
