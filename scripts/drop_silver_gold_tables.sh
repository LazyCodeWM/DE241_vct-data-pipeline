#!/usr/bin/env bash
# drop_silver_gold_tables.sh
# Drops all Silver and Gold Iceberg tables from Nessie catalog (main branch)
# Run: bash scripts/drop_silver_gold_tables.sh

set -euo pipefail

NESSIE="http://localhost:19120/api/v1"
BRANCH="main"

drop() {
    local table="$1"
    echo -n "Dropping $table ... "
    status=$(curl -s -o /dev/null -w "%{http_code}" \
        -X DELETE "${NESSIE}/trees/${BRANCH}/contents/${table}")
    if [[ "$status" == "204" || "$status" == "200" ]]; then
        echo "OK"
    elif [[ "$status" == "404" ]]; then
        echo "not found (skip)"
    else
        echo "HTTP $status"
    fi
}

echo "=== Dropping Silver tables ==="
drop "silver.dim_events"
drop "silver.dim_teams"
drop "silver.dim_team_roster"
drop "silver.dim_players"
drop "silver.fact_series"
drop "silver.fact_player_stats"
drop "silver.fact_map_scores"
drop "silver.fact_round_results"
drop "silver.fact_map_picks_bans"
drop "silver.fact_player_agent_stats"
drop "silver.fact_event_standings"
drop "silver.fact_team_placements"
drop "silver.fact_team_transactions"

echo ""
echo "=== Dropping Gold tables ==="
drop "gold.player_performance"
drop "gold.player_map_performance"
drop "gold.agent_meta"
drop "gold.agent_player_affinity"
drop "gold.team_standings"
drop "gold.team_map_performance"
drop "gold.match_summary"

echo ""
echo "Done. You can now trigger the Airflow DAG for a clean run."
