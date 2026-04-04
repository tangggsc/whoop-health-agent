"""
Bronze -> Silver: Flatten raw workout JSON from MinIO into an Iceberg table.

Reads:  s3://whoop-lakehouse/bronze/whoop/workout/dt=YYYY-MM-DD/*.json
Writes: s3://whoop-lakehouse/warehouse/silver/workouts  (Iceberg, partitioned by date)

Usage:
    cd api && python ../transform/staging/flatten_workouts.py
"""

import json
import os
import sys
from datetime import datetime, timezone

import boto3
import pyarrow as pa
from botocore.client import Config
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Load environment variables from .env file (MINIO_ENDPOINT, credentials, etc.)
load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Where raw JSON lives in MinIO (bronze layer)
BUCKET = os.getenv("MINIO_BUCKET", "whoop-lakehouse")
BRONZE_PREFIX = os.getenv("WHOOP_BRONZE_PREFIX", "bronze/whoop") + "/workout/"

# MinIO connection settings (S3-compatible local object storage)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# PostgreSQL URI used by PyIceberg to store table metadata (catalog)
# The catalog tracks table schemas, partition specs, and snapshot history
PG_URI = os.getenv(
    "ICEBERG_CATALOG_URI",
    "postgresql+psycopg2://hive:hive@localhost:5433/metastore",
)

# Where Iceberg writes the actual Parquet data files in MinIO
WAREHOUSE = f"s3://{BUCKET}/warehouse"

# Iceberg namespace (like a database) and table name
# Final table path: whoop.silver.workouts
NAMESPACE = "silver"
TABLE_NAME = "workouts"

# ---------------------------------------------------------------------------
# Iceberg schema
# ---------------------------------------------------------------------------
# This defines the column names and types for the silver table.
# NestedField(id, name, type, required) — the id must be unique and never change,
# even if you rename a column later (Iceberg uses it for schema evolution).
ICEBERG_SCHEMA = Schema(
    NestedField(1,  "workout_id",            StringType(),      required=True),   # UUID from Whoop API
    NestedField(2,  "user_id",               LongType()),                         # Your Whoop user ID
    NestedField(3,  "created_at",            TimestamptzType()),                  # When Whoop created the record
    NestedField(4,  "updated_at",            TimestamptzType()),                  # Last update from Whoop
    NestedField(5,  "start_time",            TimestamptzType()),                  # Workout start (used for partitioning)
    NestedField(6,  "end_time",              TimestamptzType()),                  # Workout end
    NestedField(7,  "timezone_offset",       StringType()),                       # e.g. "-08:00"
    NestedField(8,  "sport_id",              IntegerType()),                      # Whoop sport code (e.g. 0 = Activity)
    NestedField(9,  "score_state",           StringType()),                       # "SCORED", "PENDING_SCORE", etc.
    # --- Flattened from record.score{} ---
    NestedField(10, "strain",                DoubleType()),                       # Strain score (0-21)
    NestedField(11, "average_heart_rate",    IntegerType()),                      # Avg HR during workout
    NestedField(12, "max_heart_rate",        IntegerType()),                      # Peak HR
    NestedField(13, "kilojoule",             DoubleType()),                       # Energy expended
    NestedField(14, "percent_recorded",      DoubleType()),                       # % of workout with HR data
    NestedField(15, "distance_meter",        DoubleType()),                       # Distance (if GPS)
    NestedField(16, "altitude_gain_meter",   DoubleType()),                       # Elevation gain
    NestedField(17, "altitude_change_meter", DoubleType()),                       # Net elevation change
    # --- Flattened from record.score.zone_duration{} ---
    # Heart rate zones in milliseconds spent in each zone
    NestedField(18, "zone_zero_milli",       LongType()),                         # Below 50% max HR
    NestedField(19, "zone_one_milli",        LongType()),                         # 50-60% max HR
    NestedField(20, "zone_two_milli",        LongType()),                         # 60-70% max HR
    NestedField(21, "zone_three_milli",      LongType()),                         # 70-80% max HR
    NestedField(22, "zone_four_milli",       LongType()),                         # 80-90% max HR
    NestedField(23, "zone_five_milli",       LongType()),                         # 90-100% max HR
    NestedField(24, "ingested_at",           TimestamptzType()),                  # When we pulled it from Whoop API
)

# Partition by day of start_time (source_id=5 matches workout_id field 5 above)
# This means Iceberg organizes data files by day — Trino skips irrelevant days on date queries
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="start_day")
)

# ---------------------------------------------------------------------------
# PyArrow schema
# ---------------------------------------------------------------------------
# PyArrow is the in-memory columnar format used to write data into Iceberg.
# This must match the Iceberg schema above — same fields, same order.
# PyIceberg converts this Arrow table into Parquet files on disk.
ARROW_SCHEMA = pa.schema([
    pa.field("workout_id",            pa.string(),                 nullable=False),
    pa.field("user_id",               pa.int64()),
    pa.field("created_at",            pa.timestamp("us", tz="UTC")),
    pa.field("updated_at",            pa.timestamp("us", tz="UTC")),
    pa.field("start_time",            pa.timestamp("us", tz="UTC")),
    pa.field("end_time",              pa.timestamp("us", tz="UTC")),
    pa.field("timezone_offset",       pa.string()),
    pa.field("sport_id",              pa.int32()),
    pa.field("score_state",           pa.string()),
    pa.field("strain",                pa.float64()),
    pa.field("average_heart_rate",    pa.int32()),
    pa.field("max_heart_rate",        pa.int32()),
    pa.field("kilojoule",             pa.float64()),
    pa.field("percent_recorded",      pa.float64()),
    pa.field("distance_meter",        pa.float64()),
    pa.field("altitude_gain_meter",   pa.float64()),
    pa.field("altitude_change_meter", pa.float64()),
    pa.field("zone_zero_milli",       pa.int64()),
    pa.field("zone_one_milli",        pa.int64()),
    pa.field("zone_two_milli",        pa.int64()),
    pa.field("zone_three_milli",      pa.int64()),
    pa.field("zone_four_milli",       pa.int64()),
    pa.field("zone_five_milli",       pa.int64()),
    pa.field("ingested_at",           pa.timestamp("us", tz="UTC")),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_s3_client():
    """
    Create a boto3 S3 client pointed at MinIO.
    MinIO is S3-compatible, so boto3 works with a custom endpoint_url.
    path-style addressing means: http://localhost:9100/bucket/key
    (vs virtual-hosted style: http://bucket.localhost:9100/key which MinIO doesn't support by default)
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_bronze_keys(s3) -> list[str]:
    """
    List all .json files under the bronze workout prefix in MinIO.
    Uses a paginator because list_objects_v2 only returns 1000 keys at a time.
    Returns keys sorted alphabetically (which is also chronological given the filename format).
    """
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return sorted(keys)


def parse_ts(value: str | None) -> datetime | None:
    """
    Parse an ISO 8601 timestamp string into a Python datetime (UTC-aware).
    Whoop API returns timestamps with a 'Z' suffix (e.g. "2025-12-16T10:00:00Z").
    Python's fromisoformat() doesn't handle 'Z' before 3.11, so we replace it with '+00:00'.
    """
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def flatten_record(record: dict, ingested_at: str) -> dict:
    """
    Take one raw Whoop workout record (nested JSON) and return a flat dict.

    The raw record looks like:
    {
        "id": "877f2118-...",
        "start": "2025-12-16T10:00:00Z",
        "score": {
            "strain": 12.3,
            "zone_duration": { "zone_zero_milli": 1000, ... }
        }
    }

    We pull score{} and zone_duration{} up to the top level so every
    column is a simple scalar — no nesting — which is what SQL engines expect.
    """
    score = record.get("score") or {}           # score may be None if not yet scored
    zones = score.get("zone_duration") or {}    # zone_duration may be missing too
    return {
        "workout_id":            str(record["id"]) if record.get("id") is not None else None,
        "user_id":               record.get("user_id"),
        "created_at":            parse_ts(record.get("created_at")),
        "updated_at":            parse_ts(record.get("updated_at")),
        "start_time":            parse_ts(record.get("start")),
        "end_time":              parse_ts(record.get("end")),
        "timezone_offset":       record.get("timezone_offset"),
        "sport_id":              record.get("sport_id"),
        "score_state":           record.get("score_state"),
        "strain":                score.get("strain"),
        "average_heart_rate":    score.get("average_heart_rate"),
        "max_heart_rate":        score.get("max_heart_rate"),
        "kilojoule":             score.get("kilojoule"),
        "percent_recorded":      score.get("percent_recorded"),
        "distance_meter":        score.get("distance_meter"),
        "altitude_gain_meter":   score.get("altitude_gain_meter"),
        "altitude_change_meter": score.get("altitude_change_meter"),
        "zone_zero_milli":       zones.get("zone_zero_milli"),
        "zone_one_milli":        zones.get("zone_one_milli"),
        "zone_two_milli":        zones.get("zone_two_milli"),
        "zone_three_milli":      zones.get("zone_three_milli"),
        "zone_four_milli":       zones.get("zone_four_milli"),
        "zone_five_milli":       zones.get("zone_five_milli"),
        "ingested_at":           parse_ts(ingested_at),
    }


def records_from_key(s3, key: str) -> list[dict]:
    """
    Download one bronze JSON file from MinIO and extract the workout records.

    The file structure written by whoop_workout_backfill.py is:
    {
        "meta": { "ingested_at": "...", ... },
        "data": { "records": [ {...}, {...} ] }
    }

    We pass ingested_at down so each record knows when it was fetched.
    """
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    payload = json.loads(obj["Body"].read())
    ingested_at = payload.get("meta", {}).get("ingested_at", "")
    raw_records = payload.get("data", {}).get("records", [])
    return [flatten_record(r, ingested_at) for r in raw_records]


def dicts_to_arrow(rows: list[dict]) -> pa.Table:
    """
    Convert a list of flat dicts into a PyArrow Table.

    PyArrow works column-by-column, so we first pivot the list of dicts
    into a dict of lists (one list per column), then cast each list to the
    correct Arrow type. This is what gets written to Parquet by PyIceberg.
    """
    if not rows:
        # Return an empty table with the correct schema if there's nothing to write
        return pa.table({f.name: pa.array([], type=f.type) for f in ARROW_SCHEMA}, schema=ARROW_SCHEMA)

    # Pivot: list of dicts -> dict of lists
    columns = {f.name: [] for f in ARROW_SCHEMA}
    for row in rows:
        for f in ARROW_SCHEMA:
            columns[f.name].append(row.get(f.name))

    # Cast each column to its declared Arrow type (handles None -> null automatically)
    arrays = {}
    for f in ARROW_SCHEMA:
        arrays[f.name] = pa.array(columns[f.name], type=f.type)

    return pa.table(arrays, schema=ARROW_SCHEMA)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    s3 = get_s3_client()

    # Step 1: Find all bronze workout JSON files in MinIO
    print("Listing bronze workout files...")
    keys = list_bronze_keys(s3)
    if not keys:
        print("No bronze workout files found. Run the backfill first.")
        sys.exit(0)
    print(f"Found {len(keys)} file(s)")

    # Step 2: Read and flatten every record from every file
    all_rows = []
    for key in keys:
        rows = records_from_key(s3, key)
        all_rows.extend(rows)
        print(f"  {key} -> {len(rows)} record(s)")

    print(f"\nTotal records to write: {len(all_rows)}")
    if not all_rows:
        print("No records found in bronze files.")
        sys.exit(0)

    # Step 3: Deduplicate by workout_id
    # The same workout can appear in multiple bronze files if the backfill was re-run.
    # We keep the version with the latest ingested_at (most recently fetched from API).
    seen: dict[str, dict] = {}
    for row in all_rows:
        wid = row["workout_id"]
        if wid is None:
            continue
        existing = seen.get(wid)
        if existing is None or (row["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)) > (existing["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)):
            seen[wid] = row

    # Sort by start_time so the table is written in chronological order
    deduped = sorted(seen.values(), key=lambda r: r["start_time"] or datetime.min.replace(tzinfo=timezone.utc))
    print(f"After dedup: {len(deduped)} unique workout(s)")

    # Step 4: Convert to Arrow columnar format (required by PyIceberg for writing)
    arrow_table = dicts_to_arrow(deduped)

    # Step 5: Connect to the Iceberg catalog (metadata stored in Postgres)
    # The catalog knows about all tables, their schemas, and where their files live in MinIO.
    catalog = load_catalog(
        "whoop",
        **{
            "type": "sql",                          # SQL-backed catalog (uses Postgres)
            "uri": PG_URI,                          # Postgres connection string
            "warehouse": WAREHOUSE,                 # Base path for data files in MinIO
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )

    # Step 6: Create the namespace (like a database schema) if it doesn't exist
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"Created namespace '{NAMESPACE}'")
    except NamespaceAlreadyExistsError:
        pass  # Already exists, nothing to do

    # Step 7: Write data to the Iceberg table
    full_name = f"{NAMESPACE}.{TABLE_NAME}"
    try:
        # Table already exists — overwrite it
        # overwrite() atomically replaces all data and records a new snapshot in the catalog.
        # The old snapshot is still in history (time travel still works).
        table = catalog.load_table(full_name)
        print(f"Loaded existing table '{full_name}'")
        table.overwrite(arrow_table)
    except NoSuchTableError:
        # First run — create the table with our schema and partition spec, then append data
        table = catalog.create_table(
            full_name,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )
        print(f"Created table '{full_name}'")
        table.append(arrow_table)

    print(f"\nDone. Wrote {len(deduped)} workout(s) to {full_name}")
    print(f"Query in Trino: SELECT * FROM whoop.silver.workouts LIMIT 10;")


if __name__ == "__main__":
    main()
