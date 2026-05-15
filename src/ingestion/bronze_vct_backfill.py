"""
bronze_vct_backfill.py
======================
Historical Backfill Ingestion — Bronze Layer
Medallion Architecture | VCT Partnership Leagues (2023–2026)

Responsibilities:
  - Discovers all VCT-tier events from vlrdevapi.
  - For every completed match in each event, fetches:
      • Series-level metadata  (series.info)
      • Per-map player stats   (series.matches)
  - Validates every record against a Pydantic Data Contract.
  - Streams valid records as JSON directly to MinIO (bronze-vct-data bucket).
  - Routes invalid records to a Quarantine/DLQ path in MinIO.
  - Never writes anything to local disk.

MinIO path conventions
  bronze-vct-data/
    events/           raw/{event_id}.json
    series/           raw/{match_id}.json
    player_stats/     raw/{match_id}_{map_index}.json
    quarantine/       events/{event_id}_{ts}.json
                      series/{match_id}_{ts}.json
                      player_stats/{match_id}_{map_index}_{ts}.json
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

import vlrdevapi as vlr
from vlrdevapi.events import EventStatus, EventTier
from vlrdevapi.exceptions import DataNotFoundError, NetworkError, RateLimitError

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

MINIO_ENDPOINT:   str = os.environ["MINIO_ENDPOINT"]       # e.g. "http://localhost:9000"
MINIO_ACCESS_KEY: str = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY: str = os.environ["MINIO_SECRET_KEY"]
BUCKET_NAME:      str = "bronze-vct-data"

# VCT Partnership League scope
BACKFILL_START_YEAR: int = 2023
BACKFILL_END_YEAR:   int = 2026

# Rate-limiting — be conservative to avoid IP bans from vlr.gg
REQUEST_DELAY_SECONDS:    float = 1.5   # Between every API call
RETRY_DELAY_SECONDS:      float = 30.0  # On RateLimitError / transient errors
MAX_RETRIES:              int   = 3


# ---------------------------------------------------------------------------
# Pydantic Data Contracts (Bronze Layer — "raw but typed")
#
# The Bronze contract is intentionally permissive: we only enforce that
# required identifiers exist and that numeric columns are actually numeric.
# Strict business-rule validation belongs in the Silver layer.
# ---------------------------------------------------------------------------

class BronzeEventRecord(BaseModel):
    """Data contract for a raw VCT event record."""
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
    """Minimal team snapshot embedded inside a series record."""
    name:         str
    team_id:      int | None = None
    short:        str | None = None
    country:      str | None = None
    series_score: int | None = None


class BronzeSeriesRecord(BaseModel):
    """Data contract for a raw series (match) record."""
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
    """Data contract for a single player's stats on a single map."""
    match_id:   int
    map_index:  int                         # 0-based index of the map in the series
    game_id:    int | str | None = None
    map_name:   str | None = None
    player_name: str
    player_id:  int | None = None
    team_short:  str | None = None
    team_id:    int | None = None
    agents:     list[str] = Field(default_factory=list)
    # Core performance metrics
    rating:   float | None = None
    acs:      int | None = None
    kills:    int | None = None
    deaths:   int | None = None
    assists:  int | None = None
    kast:     float | None = None    # % rounds with kill/assist/survive/trade
    adr:      float | None = None    # average damage per round
    hs_pct:   float | None = None    # headshot %
    fk:       int | None = None      # first kills
    fd:       int | None = None      # first deaths
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


# ---------------------------------------------------------------------------
# MinIO client helpers
# ---------------------------------------------------------------------------

def _build_s3_client() -> Any:
    """Return a boto3 S3 client pointed at MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _ensure_bucket(s3: Any, bucket: str) -> None:
    """Create the MinIO bucket if it does not already exist."""
    try:
        s3.head_bucket(Bucket=bucket)
        log.info("Bucket '%s' already exists.", bucket)
    except ClientError as exc:
        error_code = int(exc.response["Error"]["Code"])
        if error_code == 404:
            s3.create_bucket(Bucket=bucket)
            log.info("Bucket '%s' created.", bucket)
        else:
            raise


def _upload_json(s3: Any, bucket: str, key: str, payload: dict) -> None:
    """
    Serialise *payload* to JSON and stream it directly to MinIO.
    No data is written to local disk — we use io.BytesIO as an in-memory buffer.
    """
    body = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")
    buffer = io.BytesIO(body)
    s3.upload_fileobj(buffer, bucket, key, ExtraArgs={"ContentType": "application/json"})
    log.debug("Uploaded s3://%s/%s  (%d bytes)", bucket, key, len(body))


def _upload_valid(s3: Any, record_type: str, key_stem: str, payload: dict) -> None:
    key = f"{record_type}/raw/{key_stem}.json"
    _upload_json(s3, BUCKET_NAME, key, payload)


def _upload_quarantine(
    s3: Any, record_type: str, key_stem: str, payload: dict, errors: str
) -> None:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    key = f"quarantine/{record_type}/{key_stem}_{ts}.json"
    quarantine_payload = {"original_payload": payload, "validation_errors": errors}
    _upload_json(s3, BUCKET_NAME, key, quarantine_payload)
    log.warning("Quarantined record → s3://%s/%s", BUCKET_NAME, key)


# ---------------------------------------------------------------------------
# Retry wrapper for vlrdevapi calls
# ---------------------------------------------------------------------------

def _call_with_retry(fn, *args, **kwargs) -> Any:
    """
    Call a vlrdevapi function with automatic retry on transient errors.
    Respects rate-limit signals from the library.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = fn(*args, **kwargs)
            time.sleep(REQUEST_DELAY_SECONDS)
            return result
        except RateLimitError:
            log.warning(
                "Rate limit hit on attempt %d/%d — sleeping %ss …",
                attempt, MAX_RETRIES, RETRY_DELAY_SECONDS,
            )
            time.sleep(RETRY_DELAY_SECONDS)
        except NetworkError as exc:
            log.warning("Network error on attempt %d/%d: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                raise
        except DataNotFoundError as exc:
            # Not a transient error — data simply doesn't exist; caller handles it.
            log.debug("DataNotFoundError (non-retried): %s", exc)
            return None
    return None


# ---------------------------------------------------------------------------
# Ingestion helpers: validate → upload
# ---------------------------------------------------------------------------

def _ingest_event(s3: Any, event_raw: dict) -> bool:
    """Validate and upload a single event record. Returns True if valid."""
    try:
        validated = BronzeEventRecord(**event_raw)
        _upload_valid(s3, "events", str(validated.event_id), validated.model_dump())
        return True
    except ValidationError as exc:
        _upload_quarantine(s3, "events", str(event_raw.get("event_id", "unknown")),
                           event_raw, exc.json())
        return False


def _ingest_series(s3: Any, series_raw: dict, match_id: int) -> bool:
    """Validate and upload a single series record. Returns True if valid."""
    try:
        validated = BronzeSeriesRecord(**series_raw)
        _upload_valid(s3, "series", str(match_id), validated.model_dump())
        return True
    except ValidationError as exc:
        _upload_quarantine(s3, "series", str(match_id), series_raw, exc.json())
        return False


def _ingest_player_stat(
    s3: Any, stat_raw: dict, match_id: int, map_index: int
) -> bool:
    """Validate and upload a single player-stat record. Returns True if valid."""
    key_stem = f"{match_id}_{map_index}_{stat_raw.get('player_name', 'unknown')}"
    try:
        validated = BronzePlayerStatRecord(**stat_raw)
        _upload_valid(s3, "player_stats", key_stem, validated.model_dump())
        return True
    except ValidationError as exc:
        _upload_quarantine(s3, "player_stats", key_stem, stat_raw, exc.json())
        return False


# ---------------------------------------------------------------------------
# Core ingestion pipeline
# ---------------------------------------------------------------------------

def _is_in_scope(event: vlr.events.ListEvent) -> bool:
    """
    Return True if the event falls within the backfill year window.
    We use start_date as the anchor; if it's absent we fall back to the name.
    """
    if event.start_date:
        return BACKFILL_START_YEAR <= event.start_date.year <= BACKFILL_END_YEAR
    # Heuristic fallback: check for year digits in the event name
    for year in range(BACKFILL_START_YEAR, BACKFILL_END_YEAR + 1):
        if str(year) in event.name:
            return True
    return False


def _process_event_matches(s3: Any, event_id: int, stats: dict) -> None:
    """
    Fetch all completed matches for *event_id*, then for each match
    fetch the series-level info and per-map player stats.
    """
    log.info("  → Fetching match list for event %d …", event_id)
    matches: list[vlr.events.Match] | None = _call_with_retry(
        vlr.events.matches, event_id
    )
    if not matches:
        log.info("    No matches found for event %d.", event_id)
        return

    completed_matches = [m for m in matches if m.status.lower() == "completed"]
    log.info("    %d completed matches found.", len(completed_matches))

    for match in completed_matches:
        match_id = match.match_id
        log.info("    Processing match %d …", match_id)

        # ── Series metadata ─────────────────────────────────────────────────
        series_info: vlr.series.Info | None = _call_with_retry(
            vlr.series.info, match_id
        )
        if series_info is None:
            log.warning("    series.info returned None for match %d — skipping.", match_id)
            stats["series_missing"] += 1
            continue

        team1_data = series_info.teams[0]
        team2_data = series_info.teams[1]
        series_raw = {
            "match_id":    series_info.match_id,
            "event_name":  series_info.event,
            "event_phase": series_info.event_phase,
            "status_note": series_info.status_note,
            "best_of":     series_info.best_of,
            "match_date":  series_info.date,
            "patch":       series_info.patch,
            "team1": {
                "name":         team1_data.name,
                "team_id":      team1_data.id,
                "short":        team1_data.short,
                "country":      team1_data.country,
                "series_score": team1_data.score,
            },
            "team2": {
                "name":         team2_data.name,
                "team_id":      team2_data.id,
                "short":        team2_data.short,
                "country":      team2_data.country,
                "series_score": team2_data.score,
            },
        }
        if _ingest_series(s3, series_raw, match_id):
            stats["series_valid"] += 1
        else:
            stats["series_invalid"] += 1

        # ── Per-map player stats ─────────────────────────────────────────────
        maps: list[vlr.series.MapPlayers] | None = _call_with_retry(
            vlr.series.matches, match_id
        )
        if not maps:
            log.debug("    No map data for match %d.", match_id)
            continue

        for map_idx, map_data in enumerate(maps):
            for player in map_data.players:
                stat_raw = {
                    "match_id":    match_id,
                    "map_index":   map_idx,
                    "game_id":     map_data.game_id,
                    "map_name":    map_data.map_name,
                    "player_name": player.name,
                    "player_id":   player.player_id,
                    "team_short":  player.team_short,
                    "team_id":     player.team_id,
                    "agents":      player.agents,
                    "rating":      player.r,
                    "acs":         player.acs,
                    "kills":       player.k,
                    "deaths":      player.d,
                    "assists":     player.a,
                    "kast":        player.kast,
                    "adr":         player.adr,
                    "hs_pct":      player.hs_pct,
                    "fk":          player.fk,
                    "fd":          player.fd,
                }
                if _ingest_player_stat(s3, stat_raw, match_id, map_idx):
                    stats["player_stats_valid"] += 1
                else:
                    stats["player_stats_invalid"] += 1


def run_backfill() -> None:
    """
    Entry point for the historical Bronze-layer backfill.

    Flow:
      1. Build MinIO client & ensure bucket exists.
      2. Paginate through EventTier.VCT completed events.
      3. Filter to BACKFILL_START_YEAR – BACKFILL_END_YEAR window.
      4. For each in-scope event: validate + upload event record,
         then drill into its matches.
    """
    log.info("=" * 60)
    log.info("Bronze Layer Backfill — VCT Esports (2023-2026)")
    log.info("MinIO endpoint : %s", MINIO_ENDPOINT)
    log.info("Bucket         : %s", BUCKET_NAME)
    log.info("=" * 60)

    s3 = _build_s3_client()
    _ensure_bucket(s3, BUCKET_NAME)

    # Running counters for the final summary
    stats: dict[str, int] = {
        "events_total":        0,
        "events_in_scope":     0,
        "events_valid":        0,
        "events_invalid":      0,
        "series_valid":        0,
        "series_invalid":      0,
        "series_missing":      0,
        "player_stats_valid":  0,
        "player_stats_invalid": 0,
    }

    # ── Paginate completed VCT events ────────────────────────────────────────
    page = 1
    while True:
        log.info("Fetching events page %d …", page)
        page_events: list[vlr.events.ListEvent] = _call_with_retry(
            vlr.events.list_events,
            tier=EventTier.VCT,
            status=EventStatus.COMPLETED,
            page=page,
        )

        if not page_events:
            log.info("No more events on page %d — pagination complete.", page)
            break

        stats["events_total"] += len(page_events)
        log.info("  Page %d: %d events returned.", page, len(page_events))

        in_scope_this_page = [e for e in page_events if _is_in_scope(e)]

        # If none of the events on this page are in scope, we may have gone
        # past our window — stop early only if all events pre-date 2023.
        all_too_old = all(
            e.start_date is not None and e.start_date.year < BACKFILL_START_YEAR
            for e in page_events
            if e.start_date
        ) and len(page_events) == len([e for e in page_events if e.start_date])

        if not in_scope_this_page and all_too_old:
            log.info("All remaining events pre-date %d — stopping.", BACKFILL_START_YEAR)
            break

        stats["events_in_scope"] += len(in_scope_this_page)

        for event in in_scope_this_page:
            log.info("Processing event %d: %s (%s)", event.id, event.name, event.status)

            event_raw = {
                "event_id":   event.id,
                "name":       event.name,
                "status":     event.status,
                "region":     event.region,
                "start_date": event.start_date,
                "end_date":   event.end_date,
                "prize":      event.prize,
            }

            if _ingest_event(s3, event_raw):
                stats["events_valid"] += 1
            else:
                stats["events_invalid"] += 1
                # Still attempt match ingestion even if the event envelope fails
                # validation — the underlying match data may still be good.

            _process_event_matches(s3, event.id, stats)

        page += 1

    # ── Final summary ────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Backfill complete.")
    log.info("  Events     total      : %d", stats["events_total"])
    log.info("  Events     in scope   : %d", stats["events_in_scope"])
    log.info("  Events     valid      : %d", stats["events_valid"])
    log.info("  Events     quarantined: %d", stats["events_invalid"])
    log.info("  Series     valid      : %d", stats["series_valid"])
    log.info("  Series     quarantined: %d", stats["series_invalid"])
    log.info("  Series     missing    : %d", stats["series_missing"])
    log.info("  PlayerStats valid      : %d", stats["player_stats_valid"])
    log.info("  PlayerStats quarantined: %d", stats["player_stats_invalid"])
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_backfill()
