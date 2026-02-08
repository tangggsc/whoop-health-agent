import os
import time
from datetime import datetime, timezone, timedelta, date
import requests

from dotenv import load_dotenv
from whoop_token import get_access_token
from s3_minio import put_json, ensure_bucket

load_dotenv()

WHOOP_V2_BASE = "https://api.prod.whoop.com/developer/v2"
WORKOUT_LIST_PATH = "/activity/workout"


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def daterange(start_d: date, end_d: date):
    d = start_d
    while d <= end_d:
        yield d
        d += timedelta(days=1)


def whoop_get_workouts(token: str, start: datetime, end: datetime, limit: int = 25, next_token: str | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "start": iso_z(start),
        "end": iso_z(end),
        "limit": limit,
    }
    if next_token:
        params["nextToken"] = next_token

    r = requests.get(f"{WHOOP_V2_BASE}{WORKOUT_LIST_PATH}", headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def backfill_workouts_to_minio(start_date: str, end_date: str, sleep_s: float = 0.25):
    """
    Backfill workouts day-by-day:
      - pulls [00:00Z, 24:00Z) for each day
      - paginates using nextToken
      - writes each page response to MinIO bronze:
          s3://<bucket>/<prefix>/workout/dt=YYYY-MM-DD/workout_<ingested_at>_pN.json
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
            data = whoop_get_workouts(token, window_start, window_end, limit=25, next_token=next_token)

            ingested_at = iso_z(datetime.now(timezone.utc))
            payload = {
                "meta": {
                    "endpoint": "developer/v2/activity/workout",
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
            key = f"{prefix}/workout/dt={d.isoformat()}/workout_{safe_ts}_p{page}.json"

            put_json(bucket, key, payload)
            print(f"Wrote s3://{bucket}/{key}")

            # Whoop responses may use nextToken or next_token
            next_token = data.get("nextToken") or data.get("next_token")
            if not next_token:
                break

            time.sleep(sleep_s)

        time.sleep(sleep_s)


if __name__ == "__main__":
    # Set these env vars, or rely on defaults:
    # WHOOP_BACKFILL_START=2026-01-15
    # WHOOP_BACKFILL_END=2026-01-16
    start = os.getenv("WHOOP_BACKFILL_START", "2026-01-01")
    end = os.getenv("WHOOP_BACKFILL_END", datetime.now(timezone.utc).date().isoformat())

    backfill_workouts_to_minio(start, end)