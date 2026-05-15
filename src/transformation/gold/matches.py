"""
matches.py
==========
Gold layer — match summary table.

Tables:
  match_summary  ← ภาพรวมต่อ match พร้อม map breakdown
                   grain : match_id
                   source: silver_fact_series + silver_fact_map_scores

Requires silver temp views to be registered via load_silver_views() first.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame


def build_match_summary(spark: SparkSession) -> DataFrame:
    """
    One row per match with series result and map breakdown.
    winner_id / winner_name derived from series scores.
    total_maps_played counted from fact_map_scores.
    """
    return spark.sql("""
        SELECT
            fs.match_id,
            fs.event_id,
            fs.event_name,
            fs.region,
            fs.match_date,
            fs.best_of,
            fs.patch,

            fs.team1_id,
            fs.team1_name,
            fs.team1_score                                      AS team1_maps_won,

            fs.team2_id,
            fs.team2_name,
            fs.team2_score                                      AS team2_maps_won,

            CASE
                WHEN fs.team1_score > fs.team2_score THEN fs.team1_id
                WHEN fs.team2_score > fs.team1_score THEN fs.team2_id
                ELSE NULL
            END                                                 AS winner_id,

            CASE
                WHEN fs.team1_score > fs.team2_score THEN fs.team1_name
                WHEN fs.team2_score > fs.team1_score THEN fs.team2_name
                ELSE 'Draw'
            END                                                 AS winner_name,

            COUNT(DISTINCT fms.game_id)                         AS total_maps_played

        FROM silver_fact_series fs
        LEFT JOIN silver_fact_map_scores fms
            ON fs.match_id = fms.match_id
        WHERE fs.match_id IS NOT NULL
        GROUP BY
            fs.match_id, fs.event_id, fs.event_name, fs.region,
            fs.match_date, fs.best_of, fs.patch,
            fs.team1_id, fs.team1_name, fs.team1_score,
            fs.team2_id, fs.team2_name, fs.team2_score
        ORDER BY fs.match_date DESC
    """)
