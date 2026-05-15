"""
agents.py
=========
Gold layer — agent meta tables.

Tables:
  agent_meta             ← pick rate / win rate ต่อ agent ต่อ map
                           grain : (map_name, agent)
                           source: silver_fact_player_stats (explode agents)
                                   + silver_fact_map_scores (for win rate)

  agent_player_affinity  ← ผู้เล่นถนัด agent ไหน (career level)
                           grain : (player_id, agent)
                           source: silver_fact_player_stats (explode agents)

Requires silver temp views to be registered via load_silver_views() first.

Note on agents column: stored as array<string> in Silver.
LATERAL VIEW EXPLODE unpacks each element into its own row.
In practice each player plays one agent per map, but the array
type is preserved from Bronze for correctness.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame


def build_agent_meta(spark: SparkSession) -> DataFrame:
    """
    Pick rate and performance stats per agent per map.
    Win rate is derived by joining with fact_map_scores to determine
    whether the player's team won that map.
    """
    return spark.sql("""
        WITH exploded AS (
            SELECT
                fps.map_name,
                agent,
                fps.r_rating,
                fps.acs,
                fps.team_id,
                CASE
                    WHEN fps.team_id = fms.team1_id THEN fms.team1_is_winner
                    WHEN fps.team_id = fms.team2_id THEN fms.team2_is_winner
                    ELSE NULL
                END AS is_winner
            FROM silver_fact_player_stats fps
            LATERAL VIEW EXPLODE(fps.agents) AS agent
            LEFT JOIN silver_fact_map_scores fms
                ON fps.match_id = fms.match_id
               AND fps.game_id  = fms.game_id
        )
        SELECT
            map_name,
            agent,
            COUNT(*)                                            AS pick_count,
            ROUND(AVG(r_rating), 3)                            AS avg_rating,
            ROUND(AVG(acs), 1)                                 AS avg_acs,
            ROUND(
                AVG(CASE WHEN is_winner = true THEN 1.0 ELSE 0.0 END)
            , 3)                                               AS win_rate
        FROM exploded
        WHERE map_name IS NOT NULL
          AND agent    IS NOT NULL
        GROUP BY map_name, agent
        ORDER BY map_name, pick_count DESC
    """)


def build_agent_player_affinity(spark: SparkSession) -> DataFrame:
    """
    Per-player per-agent career performance.
    Shows which agents each player favours and performs best on.
    """
    return spark.sql("""
        WITH exploded AS (
            SELECT
                player_id,
                player_name,
                team_id,
                team_short,
                agent,
                r_rating,
                acs,
                kills,
                deaths,
                assists
            FROM silver_fact_player_stats
            LATERAL VIEW EXPLODE(agents) AS agent
        )
        SELECT
            player_id,
            player_name,
            team_id,
            team_short,
            agent,

            COUNT(*)                                            AS maps_played,
            ROUND(AVG(r_rating), 3)                            AS avg_rating,
            ROUND(AVG(acs), 1)                                 AS avg_acs,

            SUM(kills)                                         AS total_kills,
            SUM(deaths)                                        AS total_deaths,
            ROUND(
                CAST(SUM(kills) AS DOUBLE) /
                NULLIF(SUM(deaths), 0)
            , 3)                                               AS kd_ratio

        FROM exploded
        WHERE player_id IS NOT NULL
          AND agent     IS NOT NULL
        GROUP BY player_id, player_name, team_id, team_short, agent
        ORDER BY player_id, maps_played DESC
    """)
