"""
bronze_vct_backfill.py
======================
Historical Backfill Ingestion — Bronze Layer
Medallion Architecture | VCT Tier-1 Global/Regional Events (2025)

Responsibilities:
  - Discovers VCT-tier events from vlrdevapi, keeping ONLY Tier-1
    Global/Regional tournaments (Masters, Champions, Lock//In, and the
    four regional leagues: Americas, EMEA, Pacific, China).
  - For every completed match in each in-scope event, fetches:
      • Series-level metadata  (series.info)
      • Per-map player stats   (series.matches)
      • Map Picks/Bans, Map Scores, and Round Results
  - Fetches supplemental event metadata:
      • Event standings
      • Team info & rosters
      • Player profiles & agent stats
      • Team placements & transactions
  - Validates every record against a Pydantic Data Contract.
  - Streams valid records as JSON directly to MinIO (bronze-vct-data bucket).
  - Routes invalid records to a Quarantine/DLQ path in MinIO.
  - Never writes anything to local disk.
  - Self-healing: individual match / stat failures are logged and
    skipped without crashing the pipeline.

Event filtering (applied before any nested API calls are made)
  Blacklist — skip immediately if the name contains (case-insensitive):
    CHALLENGERS | ASCENSION | GAME CHANGERS | GC | PROMO
  Whitelist — keep only if the name contains at least one of:
    PACIFIC | EMEA | AMERICAS | CHINA | MASTERS | CHAMPIONS | LOCK//IN

MinIO path conventions
  bronze-vct-data/
    events/               raw/{event_id}.json
    series/               raw/{match_id}.json
    player_stats/         raw/{match_id}_{map_index}_{player_name}.json
    map_picks_bans/       raw/{match_id}_{action_type}_{index}.json
    map_scores/           raw/{match_id}_{map_index}.json
    round_results/        raw/{match_id}_{map_index}_{round_number}.json
    event_standings/      raw/{event_id}_{place}.json
    team_info/            raw/{team_id}.json
    team_roster/          raw/{team_id}.json
    player_profiles/      raw/{player_id}.json
    player_agent_stats/   raw/{player_id}_{agent_name}.json
    team_placements/      raw/{team_id}_{event_id}.json
    team_transactions/    raw/{team_id}_{player_id}_{date}.json
    quarantine/           ...
"""

from __future__ import annotations

import io
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator

import httpx
import vlrdevapi as vlr
from vlrdevapi.events import EventStatus, EventTier
from vlrdevapi.exceptions import (
    DataNotFoundError,
    NetworkError,
    RateLimitError,
    ScrapingError,
    VlrdevapiError,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bronze_vct_backfill")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()  # Reads .env in the current working directory

MINIO_ENDPOINT:   str = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY: str = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY: str = os.environ["MINIO_SECRET_KEY"]
BUCKET_NAME:      str = "bronze-vct-data"

# VCT Partnership League scope
BACKFILL_START_YEAR: int = 2025
BACKFILL_END_YEAR:   int = 2025

# Rate-limiting — be conservative to avoid IP bans from vlr.gg
REQUEST_DELAY_SECONDS:    float = 1.5   # Between every API call
RETRY_DELAY_SECONDS:      float = 30.0  # On RateLimitError / transient errors
MAX_RETRIES:              int   = 3

# ---------------------------------------------------------------------------
# Tier-1 Event Filter Sets
# ---------------------------------------------------------------------------
_BLACKLIST_TOKENS: frozenset[str] = frozenset({
    "CHALLENGERS", "ASCENSION", "GAME CHANGERS", "GC", "PROMO"
})

_WHITELIST_TOKENS: frozenset[str] = frozenset({
    "PACIFIC", "EMEA", "AMERICAS", "CHINA",
    "MASTERS", "CHAMPIONS", "LOCK//IN"
})

# ---------------------------------------------------------------------------
# Pydantic Data Contracts (Bronze Layer)
# ---------------------------------------------------------------------------

class BronzeEventRecord(BaseModel):
    event_id:   int
    name:       str
    status:     str
    region:     str | None = None
    start_date: date | None = None
    end_date:   date | None = None
    prize:      str | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("name")
    @classmethod
    def name_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Event name must not be blank.")
        return v

class BronzeTeamRecord(BaseModel):
    name:         str
    team_id:      int | None = None
    short:        str | None = None
    country:      str | None = None
    series_score: int | None = None

class BronzeSeriesRecord(BaseModel):
    match_id:    int
    event_name:  str
    event_phase: str
    status_note: str
    best_of:     str | None = None
    match_date:  date | None = None
    patch:       str | None = None
    team1:       BronzeTeamRecord
    team2:       BronzeTeamRecord
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzePlayerStatRecord(BaseModel):
    match_id:   int
    map_index:  int
    game_id:    int | str | None = None
    map_name:   str | None = None
    player_name: str
    player_id:  int | None = None
    team_short:  str | None = None
    team_id:    int | None = None
    agents:     list[str] = Field(default_factory=list)
    rating:   float | None = None
    acs:      int | None = None
    kills:    int | None = None
    deaths:   int | None = None
    assists:  int | None = None
    kast:     float | None = None
    adr:      float | None = None
    hs_pct:   float | None = None
    fk:       int | None = None
    fd:       int | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("player_name")
    @classmethod
    def player_name_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Player name must not be blank.")
        return v

    @field_validator("acs", "kills", "deaths", "assists", mode="before")
    @classmethod
    def non_negative_int(cls, v: Any) -> Any:
        if v is not None and int(v) < 0:
            raise ValueError(f"Stat value {v} must be non-negative.")
        return v

# --- New Bronze Models ---

class BronzeMapActionRecord(BaseModel):
    match_id:    int
    action_type: str          # "pick" | "ban" | "remaining" | "map_action"
    action:      str
    team:        str
    map:         str
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzeMapScoreRecord(BaseModel):
    match_id:             int
    map_index:            int
    map_name:             str | None = None
    game_id:              int | str | None = None
    team1_id:             int | None = None
    team1_name:           str | None = None
    team1_short:          str | None = None
    team1_score:          int | None = None
    team1_attacker_rounds: int | None = None
    team1_defender_rounds: int | None = None
    team1_is_winner:      bool | None = None
    team2_id:             int | None = None
    team2_name:           str | None = None
    team2_short:          str | None = None
    team2_score:          int | None = None
    team2_attacker_rounds: int | None = None
    team2_defender_rounds: int | None = None
    team2_is_winner:      bool | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzeRoundResultRecord(BaseModel):
    match_id:          int
    map_index:         int
    round_number:      int
    winner_side:       str | None = None
    method:            str | None = None
    score_team1:       int | None = None
    score_team2:       int | None = None
    winner_team_id:    int | None = None
    winner_team_short: str | None = None
    winner_team_name:  str | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzeStandingEntryRecord(BaseModel):
    event_id:     int
    stage_path:   str
    place:        str
    team_id:      int | None = None
    team_name:    str | None = None
    team_country: str | None = None
    prize:        str | None = None
    note:         str | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzeTeamInfoRecord(BaseModel):
    team_id:    int
    name:       str | None = None
    tag:        str | None = None
    logo_url:   str | None = None
    country:    str | None = None
    is_active:  bool = True
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzeRosterMemberRecord(BaseModel):
    team_id:    int
    player_id:  int | None = None
    ign:        str | None = None
    real_name:  str | None = None
    country:    str | None = None
    role:       str
    is_captain: bool = False
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzePlayerProfileRecord(BaseModel):
    player_id:   int
    handle:      str | None = None
    real_name:   str | None = None
    country:     str | None = None
    avatar_url:  str | None = None
    aliases:     list[str] = Field(default_factory=list)
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzePlayerAgentStatsRecord(BaseModel):
    player_id:      int
    agent:          str | None = None
    usage_count:    int | None = None
    usage_percent:  float | None = None
    rounds_played:  int | None = None
    rating:         float | None = None
    acs:            float | None = None
    kd:             float | None = None
    adr:            float | None = None
    kast:           float | None = None
    kills:          int | None = None
    deaths:         int | None = None
    assists:        int | None = None
    first_kills:    int | None = None
    first_deaths:   int | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

class BronzeTeamPlacementRecord(BaseModel):
    team_id:      int
    event_id:     int | None = None
    event_name:   str | None = None
    year:         str | None = None
    series:       str | None = None
    place:        str | None = None
    prize_money:  str | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

# แก้จากแบบเดิม
class BronzePlayerTransactionRecord(BaseModel):
    team_id:      int
    player_id:    int | None = None
    ign:          str | None = None
    real_name:    str | None = None
    country:      str | None = None
    action:       str | None = None
    position:     str | None = None
    transaction_date: date | None = None 
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

# ---------------------------------------------------------------------------
# MinIO client helpers
# ---------------------------------------------------------------------------

def _build_s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def _ensure_bucket(s3: Any, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        error_code = int(exc.response["Error"]["Code"])
        if error_code == 404:
            s3.create_bucket(Bucket=bucket)
            log.info("Bucket '%s' created.", bucket)
        else:
            raise

def _upload_json(s3: Any, bucket: str, key: str, payload: dict | list) -> None:
    body = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")
    buffer = io.BytesIO(body)
    s3.upload_fileobj(buffer, bucket, key, ExtraArgs={"ContentType": "application/json"})
    log.debug("Uploaded s3://%s/%s  (%d bytes)", bucket, key, len(body))

def _upload_valid(s3: Any, record_type: str, key_stem: str, payload: dict | list) -> None:
    key = f"{record_type}/raw/{key_stem}.json"
    _upload_json(s3, BUCKET_NAME, key, payload)

def _upload_quarantine(s3: Any, record_type: str, key_stem: str, payload: dict | list, errors: str) -> None:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    key = f"quarantine/{record_type}/{key_stem}_{ts}.json"
    quarantine_payload = {"original_payload": payload, "validation_errors": errors}
    _upload_json(s3, BUCKET_NAME, key, quarantine_payload)
    log.warning("Quarantined record → s3://%s/%s", BUCKET_NAME, key)

def _key_exists(s3: Any, bucket: str, key: str) -> bool:
    """Return True if the S3 key already exists (idempotency check)."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------

def _call_with_retry(fn, *args, **kwargs) -> Any:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = fn(*args, **kwargs)
            time.sleep(REQUEST_DELAY_SECONDS)
            return result
        except RateLimitError:
            log.warning("Rate limit hit attempt %d/%d — sleeping %ss …", attempt, MAX_RETRIES, RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)
        except (NetworkError, httpx.HTTPError) as exc:
            log.warning("Network error attempt %d/%d: %s — sleeping %ss …", attempt, MAX_RETRIES, exc, RETRY_DELAY_SECONDS)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                raise
        except ScrapingError as exc:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                return None
        except DataNotFoundError:
            return None
        except VlrdevapiError as exc:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                return None
    return None

# ---------------------------------------------------------------------------
# Core ingestion pipeline
# ---------------------------------------------------------------------------

def _is_in_scope(event: vlr.events.ListEvent) -> bool:
    """
    Tier-1 filter with correct evaluation order:
      1. Year window check
      2. Blacklist check
      3. Whitelist check
    """
    # 1. Year window
    if event.start_date:
        if not (BACKFILL_START_YEAR <= event.start_date.year <= BACKFILL_END_YEAR):
            return False
    else:
        if not any(str(year) in event.name for year in range(BACKFILL_START_YEAR, BACKFILL_END_YEAR + 1)):
            return False

    name_upper = event.name.upper()

    # 2. Blacklist check (Immediate reject)
    for token in _BLACKLIST_TOKENS:
        if token in name_upper:
            return False

    # 3. Whitelist check (Must match at least one)
    for token in _WHITELIST_TOKENS:
        if token in name_upper:
            return True

    # 4. If nothing matches
    log.debug("Skipping unrecognised event %d '%s' (no whitelist token matched).", event.id, event.name)
    return False


def _process_event_matches(s3: Any, event_id: int, stats: dict) -> None:
    log.info("  → Fetching matches for event %d …", event_id)
    matches: list[vlr.events.Match] | None = _call_with_retry(vlr.events.matches, event_id)
    if not matches:
        return

    # Track unique IDs to pass to _process_event_meta
    if "unique_teams" not in stats: stats["unique_teams"] = set()
    if "unique_players" not in stats: stats["unique_players"] = set()

    completed_matches = [m for m in matches if m.status.lower() == "completed"]
    for match in completed_matches:
        match_id = match.match_id

        # ── Idempotency: skip if already ingested ────────────────────────────
        if _key_exists(s3, BUCKET_NAME, f"series/raw/{match_id}.json"):
            log.debug("Skipping already-ingested match %d", match_id)
            stats["series_skipped"] = stats.get("series_skipped", 0) + 1
            continue

        # ── Series metadata ──────────────────────────────────────────────────
        try:
            series_info: vlr.series.Info | None = _call_with_retry(vlr.series.info, match_id)
            if not series_info:
                stats["series_missing"] += 1
                continue

            team1_data, team2_data = series_info.teams[0], series_info.teams[1]
            if team1_data.id: stats["unique_teams"].add(team1_data.id)
            if team2_data.id: stats["unique_teams"].add(team2_data.id)

            series_raw = {
                "match_id":    series_info.match_id,
                "event_name":  series_info.event,
                "event_phase": series_info.event_phase,
                "status_note": series_info.status_note,
                "best_of":     series_info.best_of,
                "match_date":  series_info.date,
                "patch":       series_info.patch,
                "team1": {"name": team1_data.name, "team_id": team1_data.id, "short": team1_data.short, "country": team1_data.country, "series_score": team1_data.score},
                "team2": {"name": team2_data.name, "team_id": team2_data.id, "short": team2_data.short, "country": team2_data.country, "series_score": team2_data.score},
            }
            try:
                validated_series = BronzeSeriesRecord(**series_raw)
                _upload_valid(s3, "series", str(match_id), validated_series.model_dump())
                stats["series_valid"] += 1
            except ValidationError as exc:
                _upload_quarantine(s3, "series", str(match_id), series_raw, exc.json())
                stats["series_invalid"] += 1

            # ── 2a: Map Picks/Bans ──────────────────────────────────────────
            action_idx = 0
            for attr, act_type in [("picks", "pick"), ("bans", "ban"), ("remaining", "remaining"), ("map_actions", "map_action")]:
                items = getattr(series_info, attr, [])
                for item in items:
                    try:
                        team_val = getattr(item, 'team', item.get('team', 'unknown') if isinstance(item, dict) else 'unknown')
                        map_val = getattr(item, 'map', item.get('map', str(item)) if isinstance(item, dict) else str(item))
                        action_raw = {
                            "match_id": match_id,
                            "action_type": act_type,
                            "action": act_type,
                            "team": team_val,
                            "map": map_val
                        }
                        validated_action = BronzeMapActionRecord(**action_raw)
                        _upload_valid(s3, "map_picks_bans", f"{match_id}_{act_type}_{action_idx}", validated_action.model_dump())
                        stats["map_picks_bans_valid"] += 1
                    except ValidationError as exc:
                        _upload_quarantine(s3, "map_picks_bans", f"{match_id}_{act_type}_{action_idx}", action_raw, exc.json())
                        stats["map_picks_bans_invalid"] += 1
                    except Exception:
                        pass
                    action_idx += 1

        except Exception:
            stats["series_missing"] += 1
            continue

        # ── Map Stats & Rounds ───────────────────────────────────────────────
        try:
            maps: list[vlr.series.MapPlayers] | None = _call_with_retry(vlr.series.matches, match_id)
            if not maps:
                continue

            for map_idx, map_data in enumerate(maps):
                # Player Stats
                for player in getattr(map_data, "players", []):
                    if player.player_id: stats["unique_players"].add(player.player_id)
                    try:
                        stat_raw = {
                            "match_id": match_id, "map_index": map_idx, "game_id": map_data.game_id, "map_name": map_data.map_name,
                            "player_name": player.name, "player_id": player.player_id, "team_short": player.team_short, "team_id": player.team_id,
                            "agents": player.agents, "rating": player.r, "acs": player.acs, "kills": player.k, "deaths": player.d, "assists": player.a,
                            "kast": player.kast, "adr": player.adr, "hs_pct": player.hs_pct, "fk": player.fk, "fd": player.fd,
                        }
                        key_stem = f"{match_id}_{map_idx}_{player.name}"
                        validated_stat = BronzePlayerStatRecord(**stat_raw)
                        _upload_valid(s3, "player_stats", key_stem, validated_stat.model_dump())
                        stats["player_stats_valid"] += 1
                    except ValidationError as exc:
                        _upload_quarantine(s3, "player_stats", f"{match_id}_{map_idx}_unknown", stat_raw, exc.json())
                        stats["player_stats_invalid"] += 1
                    except Exception:
                        pass

                # ── 2b: Map Scores ──────────────────────────────────────────
                if hasattr(map_data, "teams") and map_data.teams and len(map_data.teams) >= 2:
                    try:
                        t1, t2 = map_data.teams[0], map_data.teams[1]
                        score_raw = {
                            "match_id": match_id, "map_index": map_idx, "map_name": getattr(map_data, "map_name", None), "game_id": getattr(map_data, "game_id", None),
                            "team1_id": getattr(t1, "id", None), "team1_name": getattr(t1, "name", None), "team1_short": getattr(t1, "short", None),
                            "team1_score": getattr(t1, "score", None), "team1_attacker_rounds": getattr(t1, "attacker_rounds", None), "team1_defender_rounds": getattr(t1, "defender_rounds", None), "team1_is_winner": getattr(t1, "is_winner", None),
                            "team2_id": getattr(t2, "id", None), "team2_name": getattr(t2, "name", None), "team2_short": getattr(t2, "short", None),
                            "team2_score": getattr(t2, "score", None), "team2_attacker_rounds": getattr(t2, "attacker_rounds", None), "team2_defender_rounds": getattr(t2, "defender_rounds", None), "team2_is_winner": getattr(t2, "is_winner", None),
                        }
                        val_score = BronzeMapScoreRecord(**score_raw)
                        _upload_valid(s3, "map_scores", f"{match_id}_{map_idx}", val_score.model_dump())
                        stats["map_scores_valid"] += 1
                    except ValidationError as exc:
                        _upload_quarantine(s3, "map_scores", f"{match_id}_{map_idx}", score_raw, exc.json())
                        stats["map_scores_invalid"] += 1
                    except Exception: pass

                # ── 2c: Round Results ───────────────────────────────────────
                if hasattr(map_data, "rounds") and map_data.rounds:
                    for rnd in map_data.rounds:
                        try:
                            r_num = getattr(rnd, "round_number", 0)
                            rr_raw = {
                                "match_id": match_id, "map_index": map_idx, "round_number": r_num,
                                "winner_side": getattr(rnd, "winner_side", None), "method": getattr(rnd, "method", None),
                                "score_team1": getattr(rnd, "score_team1", None), "score_team2": getattr(rnd, "score_team2", None),
                                "winner_team_id": getattr(rnd, "winner_team_id", None), "winner_team_short": getattr(rnd, "winner_team_short", None), "winner_team_name": getattr(rnd, "winner_team_name", None),
                            }
                            val_rr = BronzeRoundResultRecord(**rr_raw)
                            _upload_valid(s3, "round_results", f"{match_id}_{map_idx}_{r_num}", val_rr.model_dump())
                            stats["round_results_valid"] += 1
                        except ValidationError as exc:
                            _upload_quarantine(s3, "round_results", f"{match_id}_{map_idx}_unknown", rr_raw, exc.json())
                            stats["round_results_invalid"] += 1
                        except Exception: pass

        except Exception:
            continue


def _process_event_meta(s3: Any, event_id: int, stats: dict) -> None:
    log.info("  → Fetching event metadata for event %d …", event_id)

    # ── 3a: Event Standings ──────────────────────────────────────────────────
    try:
        standings = _call_with_retry(vlr.events.standings, event_id)
        if standings:
            for st in standings:
                try:
                    place_raw = str(getattr(st, "place", "unknown"))
                    place_clean = place_raw.replace(" ", "_").replace("/", "_")
                    st_raw = {
                        "event_id": event_id, "stage_path": getattr(st, "stage_path", ""), "place": place_raw,
                        "team_id": getattr(st, "team_id", None), "team_name": getattr(st, "team_name", None), "team_country": getattr(st, "team_country", None),
                        "prize": getattr(st, "prize", None), "note": getattr(st, "note", None)
                    }
                    val_st = BronzeStandingEntryRecord(**st_raw)
                    _upload_valid(s3, "event_standings", f"{event_id}_{place_clean}", val_st.model_dump())
                    stats["event_standings_valid"] += 1
                except Exception: pass
    except Exception: pass

    # ── Iterating Collected Teams ────────────────────────────────────────────
    for team_id in stats.get("unique_teams", set()):
        # Skip entire team block if team_info already ingested
        if _key_exists(s3, BUCKET_NAME, f"team_info/raw/{team_id}.json"):
            log.debug("Skipping already-ingested team %d", team_id)
            continue

        # 3b: Team Info
        try:
            t_info = _call_with_retry(vlr.teams.info, team_id)
            if t_info:
                info_raw = {
                    "team_id": team_id, "name": getattr(t_info, "name", None), "tag": getattr(t_info, "tag", None),
                    "logo_url": getattr(t_info, "logo_url", None), "country": getattr(t_info, "country", None), "is_active": getattr(t_info, "is_active", True)
                }
                val_info = BronzeTeamInfoRecord(**info_raw)
                _upload_valid(s3, "team_info", str(team_id), val_info.model_dump())
                stats["team_info_valid"] += 1
        except Exception: pass

        # 3b: Team Roster
        try:
            roster = _call_with_retry(vlr.teams.roster, team_id)
            if roster:
                valid_roster = []
                for rm in roster:
                    try:
                        rm_raw = {
                            "team_id": team_id, "player_id": getattr(rm, "player_id", None), "ign": getattr(rm, "ign", None),
                            "real_name": getattr(rm, "real_name", None), "country": getattr(rm, "country", None),
                            "role": getattr(rm, "role", ""), "is_captain": getattr(rm, "is_captain", False)
                        }
                        valid_roster.append(BronzeRosterMemberRecord(**rm_raw).model_dump())
                    except Exception: pass
                if valid_roster:
                    _upload_valid(s3, "team_roster", str(team_id), valid_roster)
                    stats["team_roster_valid"] += 1
        except Exception: pass

        # 3e: Team Placements
        try:
            placements = _call_with_retry(vlr.teams.placements, team_id)
            if placements:
                for p in placements:
                    try:
                        ev_id = getattr(p, "event_id", "unknown")
                        p_raw = {
                            "team_id": team_id, "event_id": getattr(p, "event_id", None), "event_name": getattr(p, "event_name", None),
                            "year": getattr(p, "year", None), "series": getattr(p, "series", None), "place": getattr(p, "place", None), "prize_money": getattr(p, "prize_money", None)
                        }
                        val_pl = BronzeTeamPlacementRecord(**p_raw)
                        _upload_valid(s3, "team_placements", f"{team_id}_{ev_id}", val_pl.model_dump())
                        stats["team_placements_valid"] += 1
                    except Exception: pass
        except Exception: pass

        # 3f: Team Transactions
        try:
            trans = _call_with_retry(vlr.teams.transactions, team_id)
            if trans:
                for t in trans:
                    try:
                        p_id = getattr(t, "player_id", "unknown")
                        d_val = getattr(t, "date", "unknown")
                        t_raw = {
                            "team_id": team_id, "player_id": getattr(t, "player_id", None), "ign": getattr(t, "ign", None),
                            "real_name": getattr(t, "real_name", None), "country": getattr(t, "country", None),
                            "action": getattr(t, "action", None), "position": getattr(t, "position", None), 
                            "transaction_date": getattr(t, "date", None) # <--- แก้บรรทัดนี้
                        }
                        val_tr = BronzePlayerTransactionRecord(**t_raw)
                        _upload_valid(s3, "team_transactions", f"{team_id}_{p_id}_{d_val}", val_tr.model_dump())
                        stats["team_transactions_valid"] += 1
                    except Exception: pass
        except Exception: pass

    # ── Iterating Collected Players ──────────────────────────────────────────
    for p_id in stats.get("unique_players", set()):
        # Skip entire player block if profile already ingested
        if _key_exists(s3, BUCKET_NAME, f"player_profiles/raw/{p_id}.json"):
            log.debug("Skipping already-ingested player %d", p_id)
            continue

        # 3c: Player Profiles
        try:
            prof = _call_with_retry(vlr.players.profile, p_id)
            if prof:
                prof_raw = {
                    "player_id": p_id, "handle": getattr(prof, "handle", None), "real_name": getattr(prof, "real_name", None),
                    "country": getattr(prof, "country", None), "avatar_url": getattr(prof, "avatar_url", None), "aliases": getattr(prof, "aliases", [])
                }
                val_prof = BronzePlayerProfileRecord(**prof_raw)
                _upload_valid(s3, "player_profiles", str(p_id), val_prof.model_dump())
                stats["player_profiles_valid"] += 1
        except Exception: pass

        # 3d: Player Agent Stats
        try:
            astats = _call_with_retry(vlr.players.agent_stats, p_id)
            if astats:
                for a in astats:
                    try:
                        agent_name = getattr(a, "agent", "unknown")
                        a_raw = {
                            "player_id": p_id, "agent": getattr(a, "agent", None), "usage_count": getattr(a, "usage_count", None), "usage_percent": getattr(a, "usage_percent", None),
                            "rounds_played": getattr(a, "rounds_played", None), "rating": getattr(a, "rating", None), "acs": getattr(a, "acs", None),
                            "kd": getattr(a, "kd", None), "adr": getattr(a, "adr", None), "kast": getattr(a, "kast", None), "kills": getattr(a, "kills", None),
                            "deaths": getattr(a, "deaths", None), "assists": getattr(a, "assists", None), "first_kills": getattr(a, "first_kills", None), "first_deaths": getattr(a, "first_deaths", None)
                        }
                        val_a = BronzePlayerAgentStatsRecord(**a_raw)
                        _upload_valid(s3, "player_agent_stats", f"{p_id}_{agent_name}", val_a.model_dump())
                        stats["player_agent_stats_valid"] += 1
                    except Exception: pass
        except Exception: pass


def run_backfill() -> None:
    log.info("=" * 60)
    log.info("Bronze Layer Backfill — VCT Esports (%d-%d)", BACKFILL_START_YEAR, BACKFILL_END_YEAR)
    log.info("=" * 60)

    s3 = _build_s3_client()
    _ensure_bucket(s3, BUCKET_NAME)

    # ── Idempotency guard: skip entire backfill if data already exists ────────
    # Once Bronze is ingested, the DAG should proceed straight to Silver/Gold
    # without re-scanning vlr.gg. Check for any existing series file as proxy.
    probe = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="series/raw/", MaxKeys=1)
    if probe.get("KeyCount", 0) > 0:
        log.info("Bronze data already present in MinIO (series/raw/ non-empty) — skipping backfill.")
        log.info("=" * 60)
        return

    stats: dict[str, int] = {
        "events_total":             0,
        "events_skipped":           0,
        "events_in_scope":          0,
        "events_valid":             0,
        "events_invalid":           0,
        "series_valid":             0,
        "series_invalid":           0,
        "series_missing":           0,
        "player_stats_valid":       0,
        "player_stats_invalid":     0,
        "map_scores_valid":         0,
        "map_scores_invalid":       0,
        "round_results_valid":      0,
        "round_results_invalid":    0,
        "map_picks_bans_valid":     0,
        "map_picks_bans_invalid":   0,
        "event_standings_valid":    0,
        "team_info_valid":          0,
        "team_roster_valid":        0,
        "player_profiles_valid":    0,
        "player_agent_stats_valid": 0,
        "team_placements_valid":    0,
        "team_transactions_valid":  0,
        "unique_teams":             set(),
        "unique_players":           set(),
    }

    page = 1
    while True:
        log.info("Fetching events page %d …", page)
        page_events: list[vlr.events.ListEvent] = _call_with_retry(
            vlr.events.list_events, tier=EventTier.VCT, status=EventStatus.COMPLETED, page=page
        )
        if not page_events: break

        stats["events_total"] += len(page_events)
        in_scope_this_page = [e for e in page_events if _is_in_scope(e)]
        skipped_this_page  = len(page_events) - len(in_scope_this_page)
        
        stats["events_skipped"]  += skipped_this_page
        stats["events_in_scope"] += len(in_scope_this_page)

        events_with_dates = [e for e in page_events if e.start_date]
        all_too_old = (bool(events_with_dates) and len(events_with_dates) == len(page_events) and all(e.start_date.year < BACKFILL_START_YEAR for e in events_with_dates))
        
        if not in_scope_this_page and all_too_old:
            log.info("All remaining events pre-date %d — stopping.", BACKFILL_START_YEAR)
            break

        for event in in_scope_this_page:
            log.info("Processing event %d: %s", event.id, event.name)
            event_raw = {
                "event_id": event.id, "name": event.name, "status": event.status,
                "region": event.region, "start_date": event.start_date, "end_date": event.end_date, "prize": event.prize,
            }
            try:
                validated = BronzeEventRecord(**event_raw)
                _upload_valid(s3, "events", str(validated.event_id), validated.model_dump())
                stats["events_valid"] += 1
            except ValidationError as exc:
                _upload_quarantine(s3, "events", str(event_raw.get("event_id", "unknown")), event_raw, exc.json())
                stats["events_invalid"] += 1

            _process_event_matches(s3, event.id, stats)
            _process_event_meta(s3, event.id, stats)

        page += 1

    # ── Final summary ────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Backfill complete.")
    log.info("  Events             : %d valid / %d scope / %d total", stats["events_valid"], stats["events_in_scope"], stats["events_total"])
    log.info("  Series             : %d valid / %d missing", stats["series_valid"], stats["series_missing"])
    log.info("  Player Stats       : %d valid", stats["player_stats_valid"])
    log.info("  Map Picks/Bans     : %d valid", stats["map_picks_bans_valid"])
    log.info("  Map Scores         : %d valid", stats["map_scores_valid"])
    log.info("  Round Results      : %d valid", stats["round_results_valid"])
    log.info("  Event Standings    : %d valid", stats["event_standings_valid"])
    log.info("  Team Info          : %d valid", stats["team_info_valid"])
    log.info("  Team Roster        : %d valid", stats["team_roster_valid"])
    log.info("  Player Profiles    : %d valid", stats["player_profiles_valid"])
    log.info("  Player Agent Stats : %d valid", stats["player_agent_stats_valid"])
    log.info("  Team Placements    : %d valid", stats["team_placements_valid"])
    log.info("  Team Transactions  : %d valid", stats["team_transactions_valid"])
    log.info("=" * 60)

if __name__ == "__main__":
    run_backfill()