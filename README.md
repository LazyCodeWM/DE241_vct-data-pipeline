# VCT Data Pipeline

An end-to-end **batch data pipeline** for VALORANT Champions Tour (VCT) esports data built on **Medallion Architecture** (Bronze → Silver → Gold) with Apache Airflow orchestration.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Tech Stack & Rationale](#3-tech-stack--rationale)
4. [Data Model — Silver Layer](#4-data-model--silver-layer)
5. [Data Model — Gold Layer](#5-data-model--gold-layer)
6. [Airflow DAG](#6-airflow-dag)
7. [Design Decisions](#7-design-decisions)
8. [Quick Start](#8-quick-start)
9. [Project Structure](#9-project-structure)

---

## 1. Overview

The pipeline ingests Tier-1 VCT 2025 match data from **vlr.gg** via an API wrapper, processes it through three medallion layers, and stores the results as **Apache Iceberg tables** ready for Data Analysts.

```
vlr.gg API
    │
    ▼  HTTP scraping (vlrdevapi)
┌─────────────────────────────────────────┐
│              Bronze Layer               │
│   Raw JSON — as-is from the API         │
│   13 prefixes, ~10,000+ files           │
└────────────────────┬────────────────────┘
                     │  Polars transforms + PK validation
                     ▼
┌─────────────────────────────────────────┐
│              Silver Layer               │
│   Typed, deduplicated Iceberg tables    │
│   4 dimension tables + 9 fact tables    │
└────────────────────┬────────────────────┘
                     │  PySpark SQL aggregations
                     ▼
┌─────────────────────────────────────────┐
│               Gold Layer                │
│   Aggregated analytics tables           │
│   7 tables ready for DA/BI consumption  │
└─────────────────────────────────────────┘
```

**Data Scope:**

| Item | Count |
|------|-------|
| Tournaments (Tier-1, 2025) | 19 |
| Matches | ~578 |
| Player stats records | ~2,500 |
| Unique players | ~320 |
| Unique teams | ~61 |

---

## 2. Architecture

### Full Pipeline

```
┌──────────────────────────────────────────────────────────────────────┐
│                           vlr.gg API                                 │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ HTTP (vlrdevapi wrapper)
                                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                                 │
│                    MinIO: bronze-vct-data/                           │
│                                                                      │
│  events/raw/          series/raw/         player_stats/raw/          │
│  team_info/raw/       team_roster/raw/    player_profiles/raw/       │
│  player_agent_stats/  event_standings/    team_placements/raw/       │
│  team_transactions/   map_scores/raw/     round_results/raw/         │
│  map_picks_bans/raw/                                                 │
│                                                                      │
│  Format: JSON (1 file per entity)                                    │
│  Validation: Pydantic data contracts                                 │
│  Bad records → quarantine/                                           │
└───────────────────────────────┬──────────────────────────────────────┘
                                │ boto3 paginator → Polars DataFrame
          ┌─────────────────────┴───────────────────────┐
          ▼                                             ▼
┌─────────────────────┐                   ┌────────────────────────┐
│    silver_dims      │                   │   silver_facts_meta    │
│  (runs in parallel) │                   │   (runs in parallel)   │
│                     │                   │                        │
│  dim_events         │                   │  fact_player_agent_stats│
│  dim_teams          │                   │  fact_event_standings  │
│  dim_team_roster    │                   │  fact_team_placements  │
│  dim_players        │                   │  fact_team_transactions│
└──────────┬──────────┘                   └───────────┬────────────┘
           │                                          │
           ▼                                          │
┌─────────────────────┐                              │
│  silver_facts_match │◄─────────────────────────────┘
│  (waits for dims)   │         (all three must finish)
│                     │
│  fact_series        │
│  fact_player_stats  │
│  fact_map_scores    │
│  fact_round_results │
│  fact_map_picks_bans│
└──────────┬──────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                                 │
│       MinIO: silver-vct-data/   │   Nessie catalog: silver.*         │
│       Format: Apache Iceberg (Parquet files + metadata)              │
│       PK violations → silver-vct-data/quarantine/                   │
└──────────┬───────────────────────────────────────────────────────────┘
           │ Spark reads Silver Iceberg → registers as temp views
  ┌────────┴────────┬─────────────────┬──────────────────┐
  ▼                 ▼                 ▼                  ▼
gold_players   gold_agents       gold_teams         gold_matches
(parallel)     (parallel)        (parallel)         (parallel)
  │                 │                 │                  │
  └────────┬────────┴─────────────────┴──────────────────┘
           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER                                  │
│       MinIO: gold-vct-data/       │   Nessie catalog: gold.*         │
│       Format: Apache Iceberg — ready for DA/BI tools                 │
└──────────────────────────────────────────────────────────────────────┘
```

### Infrastructure

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Compose                        │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌─────────┐  ┌────────┐  │
│  │  MinIO   │  │  Nessie  │  │Postgres │  │Airflow │  │
│  │:9000/9001│  │  :19120  │  │ :5432   │  │ :8080  │  │
│  └────┬─────┘  └────┬─────┘  └────┬────┘  └───┬────┘  │
│       │             │             │            │       │
│  Object Storage  Iceberg       Airflow      Scheduler  │
│  (3 buckets)     Catalog       Metadata    + Webserver │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Tech Stack & Rationale

### Apache Airflow — Orchestration

**Why Airflow:**
- Industry-standard orchestration tool widely used in data engineering
- **TaskFlow API** (decorator-based) makes DAG code concise and readable
- Built-in retry, centralized logging, and task dependency management
- Visual DAG graph for real-time pipeline state monitoring

**Why `schedule=None`:**
VCT match data does not update continuously — batch on-demand is more appropriate. The pipeline is triggered manually when a season ends or a data refresh is needed.

---

### MinIO — Object Storage

**Why MinIO:**
- Fully S3-compatible API — uses `boto3` identical to AWS S3
- Runs locally via Docker at zero cloud cost
- Switching to production AWS S3 requires only an endpoint URL change; no code changes needed

**Why Object Storage instead of a database:**
Raw Bronze JSON files have no fixed schema. Object storage scales cheaply and is well-suited for semi-structured data.

---

### Apache Iceberg — Table Format

**Why Iceberg:**

| Feature | Benefit |
|---------|---------|
| Schema evolution | Add new columns without breaking existing queries |
| ACID transactions | Multiple tasks can write concurrently without corruption |
| Time travel | Query previous snapshots for debugging |
| Partition pruning | Queries scan only relevant partitions, not the entire table |

Iceberg makes Parquet files scattered across MinIO behave like proper database tables queryable with SQL.

---

### Project Nessie — Iceberg Catalog

**What Nessie does:** Stores Iceberg table metadata — schema definitions, file locations, and snapshot history — acting as the "index" that lets Spark locate any table.

**Why Nessie over Hive Metastore:**
Nessie supports **git-like branching for data** — transformations can be tested on an isolated branch and merged into `main` without affecting production data.

---

### Polars — In-memory Transformation (Silver)

**Why Polars over Pandas:**
- Written in Rust — 5–10× faster than Pandas on medium-sized data
- **Lazy evaluation** — builds a query plan before executing, enabling automatic optimization
- Expressive API for complex nested data: `struct.field()`, `explode()`, `cast()`

**Why not use Spark end-to-end:**
At ~10,000 rows, distributed compute is unnecessary. Spark carries significant overhead (JVM startup, task scheduling) that makes it slower than single-machine Polars at this scale. Spark is used only for the final Iceberg write step because the Spark–Iceberg connector is the most mature and reliable.

---

### PySpark — Iceberg Writer & Gold Aggregation

PySpark serves two roles:
1. **Writing Silver/Gold tables** via the Iceberg Spark connector (idempotent, partitioned writes)
2. **Gold aggregations** via Spark SQL — queries across Silver tables registered as temp views

---

## 4. Data Model — Silver Layer

### Galaxy Schema (Fact Constellation)

This project uses a **Galaxy Schema** rather than a Star Schema because VCT data has multiple grain levels that cannot be collapsed into a single fact table without introducing excessive nulls and query complexity.

```
                         ┌─────────────────┐
                         │   dim_events    │
                         │─────────────────│
                         │ PK: event_id    │
                         │ name            │
                         │ region          │
                         │ start_date      │
                         │ end_date        │
                         │ status          │
                         └────────┬────────┘
                                  │
              ┌───────────────────┼────────────────────┐
              │                   │                    │
              ▼                   ▼                    ▼
   ┌──────────────────┐ ┌─────────────────┐ ┌──────────────────────┐
   │   fact_series    │ │fact_player_stats│ │  fact_map_scores     │
   │──────────────────│ │─────────────────│ │──────────────────────│
   │PK: match_id      │ │PK: match_id +   │ │PK: match_id +        │
   │event_id (FK)     │ │    game_id +     │ │    game_id           │
   │event_name        │ │    player_id     │ │event_id (FK)         │
   │match_date        │ │event_id (FK)     │ │map_name              │
   │best_of           │ │map_name          │ │team1_id, team1_score │
   │patch             │ │player_id         │ │team2_id, team2_score │
   │team1_id          │ │player_name       │ │team1_attacker_rounds │
   │team1_name        │ │team_id           │ │team1_defender_rounds │
   │team1_score       │ │agents (array)    │ │team1_is_winner       │
   │team2_id          │ │r_rating          │ │team2_attacker_rounds │
   │team2_name        │ │acs               │ │team2_defender_rounds │
   │team2_score       │ │kills/deaths/     │ │team2_is_winner       │
   └──────────────────┘ │assists           │ └──────────────────────┘
                        │kast, adr, hs_pct │
                        │fk, fd            │ ┌──────────────────────┐
                        └──────────────────┘ │ fact_round_results   │
                                             │──────────────────────│
   ┌──────────────────┐ ┌─────────────────┐  │PK: match_id +        │
   │    dim_teams     │ │  dim_players    │  │    map_index +        │
   │──────────────────│ │─────────────────│  │    round_number       │
   │PK: team_id       │ │PK: player_id    │  │winner_side           │
   │name              │ │handle           │  │method                │
   │tag               │ │real_name        │  │score_team1/2         │
   │country           │ │country          │  │winner_team_id        │
   │logo_url          │ │avatar_url       │  └──────────────────────┘
   └──────────────────┘ │aliases (array)  │
                        └─────────────────┘ ┌──────────────────────┐
   ┌──────────────────┐                     │ fact_map_picks_bans  │
   │ dim_team_roster  │                     │──────────────────────│
   │──────────────────│                     │PK: match_id +        │
   │PK: team_id +     │                     │    action_type +     │
   │    player_id     │                     │    map + team        │
   │ign               │                     │action (pick/ban)     │
   │real_name         │                     │map                   │
   │role              │                     │team                  │
   │is_captain        │                     └──────────────────────┘
   └──────────────────┘

   ─ ─ ─ ─  Supplemental Facts (independent of fact_series)  ─ ─ ─ ─

   ┌───────────────────────┐  ┌──────────────────────┐
   │ fact_player_agent_stats│  │ fact_event_standings │
   │───────────────────────│  │──────────────────────│
   │PK: player_id + agent  │  │PK: event_id +        │
   │usage_count            │  │    stage_path + place│
   │rating, acs, kd        │  │team_id, team_name    │
   │kills, deaths, assists │  │prize, note           │
   └───────────────────────┘  └──────────────────────┘

   ┌───────────────────────┐  ┌──────────────────────┐
   │ fact_team_placements  │  │fact_team_transactions│
   │───────────────────────│  │──────────────────────│
   │PK: team_id + event_id │  │PK: team_id +         │
   │event_name, year       │  │    player_id + date  │
   │series, place          │  │ign, action           │
   │prize_money            │  │position              │
   └───────────────────────┘  └──────────────────────┘
```

### Silver Tables Summary

| Table | Grain | Rows (~) | Partition |
|-------|-------|----------|-----------|
| `dim_events` | 1 per tournament | 19 | region |
| `dim_teams` | 1 per team | 61 | — |
| `dim_team_roster` | 1 per team × player | 443 | — |
| `dim_players` | 1 per player | 320 | — |
| `fact_series` | 1 per match | 578 | event_id |
| `fact_player_stats` | 1 per match × map × player | 2,500+ | event_id, map_name |
| `fact_map_scores` | 1 per match × map | 78+ | event_id |
| `fact_round_results` | 1 per match × map × round | varies | event_id |
| `fact_map_picks_bans` | 1 per match × action | varies | event_id |
| `fact_player_agent_stats` | 1 per player × agent | 5,268 | — |
| `fact_event_standings` | 1 per tournament × place | varies | event_id |
| `fact_team_placements` | 1 per team × event | 1,467 | — |
| `fact_team_transactions` | 1 per team × player × date | 3,234 | — |

---

## 5. Data Model — Gold Layer

Gold tables are pre-aggregated analytics that Data Analysts can query directly without writing complex JOINs.

```
Silver Tables                      Gold Tables
───────────────────────────────────────────────────────────────
fact_player_stats ────────────────► player_performance
                                    grain: (event_id, player_id)
                                    avg_rating, avg_acs, kd_ratio
                                    total_kills, fk_rate

fact_player_stats ────────────────► player_map_performance
                                    grain: (player_id, map_name)
                                    avg_rating, avg_acs, kd_ratio

fact_player_stats ──┐
                    ├─────────────► agent_meta
fact_map_scores ────┘               grain: (map_name, agent)
                                    pick_count, avg_rating, win_rate

fact_player_stats ────────────────► agent_player_affinity
                                    grain: (player_id, agent)
                                    maps_played, avg_rating, kd_ratio

fact_series ──────────────────────► team_standings
                                    grain: (event_id, team_id)
                                    matches_won/lost, match_win_rate
                                    maps_won/lost, map_win_rate

fact_map_scores ──────────────────► team_map_performance
                                    grain: (team_id, map_name)
                                    map_win_rate
                                    att_round_win_rate
                                    def_round_win_rate

fact_series ────┐
                ├─────────────────► match_summary
fact_map_scores ┘                   grain: match_id
                                    winner_id, winner_name
                                    team1/2_maps_won
                                    total_maps_played
```

### Gold Tables Detail

#### `player_performance` — Per Tournament
```
event_id | event_name | region | player_id | player_name | team_id
matches_played | maps_played | avg_rating | avg_acs
total_kills | total_deaths | total_assists | kd_ratio
avg_kast | avg_adr | avg_hs_pct | total_first_kills | fk_rate
```

#### `team_standings` — Win/Loss Record
```
event_id | event_name | region | team_id | team_name
matches_played | matches_won | matches_lost | match_win_rate
maps_won | maps_lost | map_win_rate
```

#### `agent_meta` — Pick & Win Rate
```
map_name | agent | pick_count | avg_rating | avg_acs | win_rate
```

#### `match_summary` — Match Overview
```
match_id | event_id | match_date | best_of | patch
team1_id | team1_name | team1_maps_won
team2_id | team2_name | team2_maps_won
winner_id | winner_name | total_maps_played
```

---

## 6. Airflow DAG

**DAG ID:** `vct_pipeline` | **Schedule:** Manual trigger only | **Max active runs:** 1

```
[start]
   │
   ▼
[bronze_ingestion]  ←  Fetch from vlr.gg, store JSON to MinIO
   │                   Idempotent: skips entirely if Bronze already populated
   ├──────────────────────────────────────────┐
   ▼                                          ▼
[silver_dims]                        [silver_facts_meta]
dim_events, dim_teams,               fact_player_agent_stats
dim_team_roster, dim_players         fact_event_standings
   │                                 fact_team_placements
   │                                 fact_team_transactions
   ▼                                          │
[silver_facts_match] ◄────────────────────────┘
fact_series, fact_player_stats       (waits for both dims + facts_meta)
fact_map_scores, fact_round_results
fact_map_picks_bans
   │
   ├──────────┬──────────┬──────────┐
   ▼          ▼          ▼          ▼
[gold_players][gold_agents][gold_teams][gold_matches]
   │          │          │          │    (all run in parallel)
   └──────────┴──────────┴──────────┘
                    │
                    ▼
                  [end]
```

### Why the Task Graph is Designed This Way

| Task | Waits For | Reason |
|------|-----------|--------|
| `silver_dims` | bronze complete | Needs raw data from Bronze |
| `silver_facts_meta` | bronze complete | Independent of dims — runs in parallel |
| `silver_facts_match` | dims **and** facts_meta | Must JOIN with `dim_events` to resolve `event_id` from event_name |
| all `gold_*` | all silver complete | Reads from Silver Iceberg tables that must be fully written first |
| `gold_*` run in parallel | no inter-dependencies | Four independent aggregations — saves time |

### Retry Policy

| Task | Retries | Retry Delay |
|------|---------|-------------|
| bronze_ingestion | 2 | 60 seconds |
| silver_* | 1 | 30 seconds |
| gold_* | 1 | 30 seconds |

---

## 7. Design Decisions

### Idempotency — Same Result Every Run

The pipeline can be triggered multiple times without duplicating data.

```
Bronze:  _key_exists(s3, "series/raw/{match_id}.json")
         → exists:  skip API call + upload entirely
         → missing: fetch from vlr.gg + upload

         Early-exit guard in run_backfill():
         → checks if series/raw/ prefix has any objects
         → if yes: return immediately (no vlr.gg scanning at all)

Silver:  write_iceberg_table() uses createOrReplace
         → overwrites the entire table on each run

Gold:    drop_table() then create()
         → rebuilds the table from scratch every run
```

### Data Quality — Quarantine Pattern

Rows with duplicate Primary Keys are routed to a quarantine path instead of polluting the clean table:

```
Bronze JSON
    │
    ▼  transform + PK deduplication
    ├── clean rows ──────────────────► Silver Iceberg table
    └── duplicate PK rows ──────────► silver-vct-data/quarantine/{table}/{ts}.parquet
```

The pipeline never halts due to dirty data, and all violations are auditable after the run.

### Polars → Spark Hand-off

```
Bronze JSON (MinIO)
    │ boto3 paginator
    ▼
Polars DataFrame          ←  all business logic (fast, low overhead)
    │ .to_pandas()
    ▼
Pandas DataFrame
    │ spark.createDataFrame()
    ▼
Spark DataFrame           ←  only for Iceberg write
    │ .writeTo(nessie.silver.X).createOrReplace()
    ▼
Iceberg Table (MinIO + Nessie)
```

Polars handles transformation because it outperforms Spark at this data scale. Spark handles the Iceberg write because its connector is the most stable and feature-complete.

### Galaxy Schema vs Star Schema

VCT data has five distinct grain levels — collapsing them into a single fact table would produce excessive NULLs and make queries harder:

| Fact Table | Grain |
|------------|-------|
| `fact_series` | 1 row per match |
| `fact_map_scores` | 1 row per match × map |
| `fact_player_stats` | 1 row per match × map × player |
| `fact_round_results` | 1 row per match × map × round |
| `fact_player_agent_stats` | 1 row per player × agent (career) |

All fact tables share the same dimension tables (`dim_events`, `dim_teams`, `dim_players`), forming a Galaxy Schema (Fact Constellation).

---

## 8. Quick Start

### Prerequisites

- Docker + Docker Compose
- `.env` file (copy from `.env.example`)

### 1. Configure Environment

```bash
cp .env.example .env
# Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY as needed
```

### 2. Start All Services

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | as set in .env |
| Nessie API | http://localhost:19120 | — |

### 3. Trigger the Pipeline

Open http://localhost:8080 → DAG `vct_pipeline` → **Trigger DAG ▶**

### 4. Run Locally (without Docker)

Requires Python 3.11 + Java 17.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m src.ingestion.bronze_vct_backfill       # Bronze
python -m src.transformation.silver.runner        # Silver
python -m src.transformation.gold.runner          # Gold
```

### 5. Reset Silver/Gold Before a Clean Run

```bash
bash scripts/drop_silver_gold_tables.sh
```

---

## 9. Project Structure

```
DE241_vct-data-pipeline/
│
├── dags/
│   └── vct_pipeline_dag.py          # Airflow DAG — task graph + dependencies
│
├── src/
│   ├── ingestion/
│   │   └── bronze_vct_backfill.py   # Fetch vlr.gg → validate → JSON → MinIO
│   │                                # Pydantic contracts, retry logic,
│   │                                # _key_exists idempotency guard
│   │
│   └── transformation/
│       ├── silver/
│       │   ├── common.py            # build_s3_client, read_bronze_prefix,
│       │   │                        # build_spark, write_iceberg_table,
│       │   │                        # quarantine_violations
│       │   ├── dims.py              # transform_dim_events/teams/roster/players
│       │   ├── facts_match.py       # transform_fact_series (+ series_event_lookup)
│       │   │                        # transform_fact_player_stats/map_scores/
│       │   │                        # round_results/map_picks_bans
│       │   ├── facts_meta.py        # transform_fact_player_agent_stats/
│       │   │                        # event_standings/team_placements/transactions
│       │   └── runner.py            # run_dims(), run_facts_match(),
│       │                            # run_facts_meta(), run_pipeline()
│       │
│       └── gold/
│           ├── common.py            # build_gold_spark, load_silver_views,
│           │                        # write_gold_table, ensure_gold_namespace
│           ├── players.py           # build_player_performance/map_performance
│           ├── agents.py            # build_agent_meta/player_affinity
│           ├── teams.py             # build_team_standings/map_performance
│           ├── matches.py           # build_match_summary
│           └── runner.py            # run_players/agents/teams/matches/pipeline()
│
├── scripts/
│   └── drop_silver_gold_tables.sh   # Reset Nessie catalog entries
│
├── Dockerfile                       # Airflow + Java 17 + Python deps
├── docker-compose.yml               # MinIO, Nessie, Postgres, Airflow
├── requirements.txt
├── PRESENTATION_NOTES.md            # Design decisions Q&A for presentation
└── .env.example
```

---

## Tech Stack Summary

| Category | Tool | Version |
|----------|------|---------|
| Orchestration | Apache Airflow | 2.9.1 |
| Object Storage | MinIO | latest |
| Table Format | Apache Iceberg | 1.5.2 |
| Catalog | Project Nessie | latest |
| In-memory Transform | Polars | 0.20.0 |
| Distributed Compute | PySpark | 3.5.1 |
| Data Validation | Pydantic | 2.7.0 |
| API Client | vlrdevapi + httpx | — |
| Infrastructure | Docker Compose | — |
| Language | Python | 3.11 |
