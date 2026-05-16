# VCT Data Pipeline

End-to-end data pipeline for **VALORANT Champions Tour (VCT)** esports data built on the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Architecture

```
                    ┌─────────────────┐
                    │  VLR.gg / API   │
                    └────────┬────────┘
                             │ raw JSON
                             ▼
              ┌──────────────────────────┐
              │     Bronze Layer         │  MinIO: bronze-vct-data/
              │  Raw JSON, as-ingested   │
              └──────────────┬───────────┘
                             │ Polars transforms
                  ┌──────────┴──────────┐
                  ▼                     ▼
         [silver_dims]       [silver_facts_meta]
                  │                     │
                  ▼                     │
        [silver_facts_match]            │
                  │                     │
                  └──────────┬──────────┘
                             │ Iceberg tables
              ┌──────────────────────────┐
              │     Silver Layer         │  MinIO: silver-vct-data/
              │  Typed, deduplicated,    │  Nessie: nessie.silver.*
              │  PK-validated facts/dims │
              └──────────────┬───────────┘
                             │ Spark SQL aggregations
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
   [gold_players]     [gold_agents]      [gold_teams / gold_matches]
              ┌──────────────────────────┐
              │      Gold Layer          │  MinIO: gold-vct-data/
              │  Aggregated analytics    │  Nessie: nessie.gold.*
              │  ready for dashboards    │
              └──────────────────────────┘
```

### Data Flow

| Layer | Tool | Storage | Catalog |
|-------|------|---------|---------|
| Bronze | Python + boto3 | `bronze-vct-data` (MinIO) | — |
| Silver | Polars + PySpark + Iceberg | `silver-vct-data` (MinIO) | Nessie `silver.*` |
| Gold | PySpark SQL + Iceberg | `gold-vct-data` (MinIO) | Nessie `gold.*` |

---

## Gold Tables

| Table | Grain | Source |
|-------|-------|--------|
| `player_performance` | (event_id, player_id) | fact_player_stats |
| `player_map_performance` | (player_id, map_name) | fact_player_stats |
| `agent_meta` | (map_name, agent) | fact_player_stats + fact_map_scores |
| `agent_player_affinity` | (player_id, agent) | fact_player_stats |
| `team_standings` | (event_id, team_id) | fact_series |
| `team_map_performance` | (team_id, map_name) | fact_map_scores |
| `match_summary` | match_id | fact_series + fact_map_scores |

> Tables that depend on `fact_map_scores` are skipped gracefully when Bronze has no map data — they populate automatically on the next run once data is available.

---

## Quick Start (Docker)

### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
NESSIE_URI=http://localhost:19120/api/v1
```

### 2. Start infrastructure + Airflow

```bash
docker compose up -d
```

Services:
| Service | URL |
|---------|-----|
| MinIO Console | http://localhost:9001 |
| Nessie API | http://localhost:19120 |
| Airflow UI | http://localhost:8080 (admin / admin) |

### 3. Trigger the pipeline

Open http://localhost:8080, find `vct_pipeline` DAG, and click **Trigger DAG**.

---

## Local Development (without Docker)

Requires: Python 3.11, Java 17.

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run each layer manually
python -m src.ingestion.bronze_vct_backfill        # Bronze
source .venv/bin/activate && python -m src.transformation.silver.runner   # Silver
source .venv/bin/activate && python -m src.transformation.gold.runner     # Gold
```

> **Note:** PySpark 3.5.x requires Java 17. If your system default is Java 21+, the pipeline auto-switches to Java 17 at runtime (see `silver/common.py: _ensure_java17`).

---

## Airflow DAG

**DAG ID:** `vct_pipeline`  
**Schedule:** Manual trigger (no cron)

```
[start]
   │
   ▼
[bronze_ingestion]
   │           │
   ▼           ▼
[silver_dims] [silver_facts_meta]
   │
   ▼
[silver_facts_match]
   │
   └──────────────────────────────────┐
   ▼          ▼           ▼           ▼
[gold_players][gold_agents][gold_teams][gold_matches]
   │
   ▼
[end]
```

---

## Project Structure

```
DE241_vct-data-pipeline/
├── dags/
│   └── vct_pipeline_dag.py       # Airflow DAG definition
├── src/
│   ├── ingestion/
│   │   └── bronze_vct_backfill.py
│   └── transformation/
│       ├── silver/
│       │   ├── common.py         # Spark, S3, Iceberg utilities
│       │   ├── dims.py           # Dimension transforms
│       │   ├── facts_match.py    # Match fact transforms
│       │   ├── facts_meta.py     # Supplemental fact transforms
│       │   └── runner.py         # Airflow-callable entry points
│       └── gold/
│           ├── common.py         # Gold Spark session + write helpers
│           ├── players.py        # Player aggregations
│           ├── agents.py         # Agent pick/win rate
│           ├── teams.py          # Team standings + map stats
│           ├── matches.py        # Match summary
│           └── runner.py         # Airflow-callable entry points
├── Dockerfile                    # Airflow image with Java 17 + deps
├── docker-compose.yml            # MinIO, Nessie, Postgres, Airflow
├── requirements.txt
├── requirements-dev.txt
├── TECHSTACK.md
└── .env.example
```

---

## Tech Stack

See [TECHSTACK.md](TECHSTACK.md) for full version details.

| Category | Tools |
|----------|-------|
| Orchestration | Apache Airflow 2.9.1 |
| Storage | MinIO (S3-compatible) |
| Table format | Apache Iceberg + Project Nessie |
| Transform | Polars (Silver), PySpark 3.5.1 (Iceberg writes + Gold) |
| Infrastructure | Docker Compose |
