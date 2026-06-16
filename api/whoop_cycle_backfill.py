"""
Backfill Whoop cycles (daily physiological cycles) into MinIO bronze layer.

A "cycle" in Whoop is a 24-hour physiological day — it contains daily strain,
average/max HR, and kilojoules. Recovery and Sleep records link back to a cycle_id,
making this the join key across all Whoop data.

Writes to: s3://whoop-lakehouse/bronze/whoop/cycle/dt=YYYY-MM-DD/cycle_<ts>_pN.json

Usage:
    cd api
    python whoop_cycle_backfill.py

    # Or override date range via env vars:
    WHOOP_BACKFILL_START=2025-12-01 WHOOP_BACKFILL_END=2026-01-01 python whoop_cycle_backfill.py
"""

import os
import time
from datetime import datetime, timezone, timedelta, date

import requests
from dotenv import load_dotenv

from whoop_token import get_access_token
from s3_minio import put_json, ensure_bucket

load_dotenv()

WHOOP_V2_BASE = "https://api.prod.whoop.com/developer/v2"
CYCLE_LIST_PATH = "/cycle"


def iso_z(dt: datetime) -> str:
    """Format a datetime as an ISO 8601 string with Z suffix (UTC)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def daterange(start_d: date, end_d: date):
    """Yield each date from start_d to end_d inclusive."""
    d = start_d
    while d <= end_d:
        yield d
        d += timedelta(days=1)


def whoop_get_cycles(
    token: str,
    start: datetime,
    end: datetime,
    limit: int = 25,
    next_token: str | None = None,
) -> dict:
    """
    Call the Whoop v2 /cycle endpoint for a given time window.
    Returns the raw API response as a dict.
    Handles pagination via nextToken.
    """
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "start": iso_z(start),
        "end": iso_z(end),
        "limit": limit,
    }
    if next_token:
        params["nextToken"] = next_token

    r = requests.get(
        f"{WHOOP_V2_BASE}{CYCLE_LIST_PATH}",
        headers=headers,
        params=params,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def backfill_cycles_to_minio(start_date: str, end_date: str, sleep_s: float = 0.25):
    """
    Backfill cycles day-by-day into MinIO bronze layer.

    For each day:
      - Queries cycles in the window [00:00Z, 24:00Z)
      - Paginates through all pages using nextToken
      - Wraps each page in a metadata envelope and writes to MinIO:
          bronze/whoop/cycle/dt=YYYY-MM-DD/cycle_<ingested_at>_pN.json

    Note: A Whoop cycle doesn't always align perfectly to a calendar day
    (it starts when you wake up), so some days may return 0 records and
    adjacent days may return 2. This is expected.
    """
    bucket = os.getenv("MINIO_BUCKET", "whoop-lakehouse")
    prefix = os.getenv("WHOOP_BRONZE_PREFIX", "bronze/whoop")

    ensure_bucket(bucket)
    token = get_access_token()

    start_d = parse_date(start_date)
    end_d = parse_date(end_date)

    for d in daterange(start_d, end_d):
        window_start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        window_end = window_start + timedelta(days=1)

        next_token = None
        page = 0

        while True:
            page += 1
            data = whoop_get_cycles(token, window_start, window_end, limit=25, next_token=next_token)

            ingested_at = iso_z(datetime.now(timezone.utc))

            # Wrap the raw API response with metadata so we know when/how it was fetched
            payload = {
                "meta": {
                    "endpoint": "developer/v2/cycle",
                    "start": iso_z(window_start),
                    "end": iso_z(window_end),
                    "limit": 25,
                    "nextToken": next_token,
                    "page": page,
                    "ingested_at": ingested_at,
                },
                "data": data,
            }

            safe_ts = ingested_at.replace(":", "-")
            key = f"{prefix}/cycle/dt={d.isoformat()}/cycle_{safe_ts}_p{page}.json"

            put_json(bucket, key, payload)
            print(f"Wrote s3://{bucket}/{key}")

            next_token = data.get("nextToken") or data.get("next_token")
            if not next_token:
                break

            time.sleep(sleep_s)

        time.sleep(sleep_s)


if __name__ == "__main__":
    start = os.getenv("WHOOP_BACKFILL_START", "2026-01-01")
    end = os.getenv("WHOOP_BACKFILL_END", datetime.now(timezone.utc).date().isoformat())

    backfill_cycles_to_minio(start, end)
