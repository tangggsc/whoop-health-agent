"""
Bronze -> Silver: Flatten raw cycle JSON from MinIO into an Iceberg table.

A Whoop cycle is the daily physiological container. Recovery and Sleep records
both reference a cycle_id, making this the join key across all Whoop data.

Reads:  s3://whoop-lakehouse/bronze/whoop/cycle/dt=YYYY-MM-DD/*.json
Writes: s3://whoop-lakehouse/warehouse/silver/cycles  (Iceberg, partitioned by day)

Usage:
    cd api && python ../transform/staging/flatten_cycles.py

Raw cycle record shape from Whoop API:
{
    "id": 12345678,
    "user_id": 123,
    "created_at": "2025-12-16T10:00:00.000Z",
    "updated_at": "2025-12-16T10:00:00.000Z",
    "start": "2025-12-16T10:00:00.000Z",
    "end": "2025-12-17T09:00:00.000Z",    # null if cycle is still open
    "timezone_offset": "-08:00",
    "score_state": "SCORED",
    "score": {
        "strain": 14.2,
        "kilojoule": 1800.0,
        "average_heart_rate": 72,
        "max_heart_rate": 165
    }
}
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

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BUCKET = os.getenv("MINIO_BUCKET", "whoop-lakehouse")
BRONZE_PREFIX = os.getenv("WHOOP_BRONZE_PREFIX", "bronze/whoop") + "/cycle/"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
PG_URI = os.getenv(
    "ICEBERG_CATALOG_URI",
    "postgresql+psycopg2://hive:hive@localhost:5433/metastore",
)
WAREHOUSE = f"s3://{BUCKET}/warehouse"
NAMESPACE = "silver"
TABLE_NAME = "cycles"

# ---------------------------------------------------------------------------
# Iceberg schema
# ---------------------------------------------------------------------------
# Cycles have a simpler score than workouts — no zone_duration, no GPS fields.
# cycle_id is a Long (integer) unlike workout_id which is a UUID string.
ICEBERG_SCHEMA = Schema(
    NestedField(1,  "cycle_id",           LongType(),        required=True),  # Integer ID (join key for recovery/sleep)
    NestedField(2,  "user_id",            LongType()),                        # Your Whoop user ID
    NestedField(3,  "created_at",         TimestamptzType()),                 # When Whoop created the record
    NestedField(4,  "updated_at",         TimestamptzType()),                 # Last update from Whoop
    NestedField(5,  "start_time",         TimestamptzType()),                 # Cycle start (used for partitioning)
    NestedField(6,  "end_time",           TimestamptzType()),                 # Cycle end (null if still open)
    NestedField(7,  "timezone_offset",    StringType()),                      # e.g. "-08:00"
    NestedField(8,  "score_state",        StringType()),                      # "SCORED", "PENDING_SCORE", etc.
    # --- Flattened from record.score{} ---
    NestedField(9,  "strain",             DoubleType()),                      # Daily strain score (0-21)
    NestedField(10, "kilojoule",          DoubleType()),                      # Total energy for the day
    NestedField(11, "average_heart_rate", IntegerType()),                     # Avg HR across the full day
    NestedField(12, "max_heart_rate",     IntegerType()),                     # Peak HR across the full day
    NestedField(13, "ingested_at",        TimestamptzType()),                 # When we fetched it from the API
)

# Partition by day of start_time — one partition per cycle day
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="start_day")
)

# PyArrow schema — must match Iceberg schema above, same field order
ARROW_SCHEMA = pa.schema([
    pa.field("cycle_id",           pa.int64(),                  nullable=False),
    pa.field("user_id",            pa.int64()),
    pa.field("created_at",         pa.timestamp("us", tz="UTC")),
    pa.field("updated_at",         pa.timestamp("us", tz="UTC")),
    pa.field("start_time",         pa.timestamp("us", tz="UTC")),
    pa.field("end_time",           pa.timestamp("us", tz="UTC")),
    pa.field("timezone_offset",    pa.string()),
    pa.field("score_state",        pa.string()),
    pa.field("strain",             pa.float64()),
    pa.field("kilojoule",          pa.float64()),
    pa.field("average_heart_rate", pa.int32()),
    pa.field("max_heart_rate",     pa.int32()),
    pa.field("ingested_at",        pa.timestamp("us", tz="UTC")),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_bronze_keys(s3) -> list[str]:
    """List all bronze cycle JSON files in MinIO, sorted chronologically."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return sorted(keys)


def parse_ts(value: str | None) -> datetime | None:
    """Parse ISO 8601 timestamp string to UTC-aware datetime."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def flatten_record(record: dict, ingested_at: str) -> dict:
    """
    Flatten one raw cycle record from nested JSON to a flat dict.

    Unlike workouts, cycles use an integer ID (not UUID) and have a simpler
    score with no zone breakdowns or GPS fields.
    """
    score = record.get("score") or {}  # score may be None if cycle is not yet scored
    return {
        "cycle_id":           record.get("id"),
        "user_id":            record.get("user_id"),
        "created_at":         parse_ts(record.get("created_at")),
        "updated_at":         parse_ts(record.get("updated_at")),
        "start_time":         parse_ts(record.get("start")),
        "end_time":           parse_ts(record.get("end")),   # null if cycle still open
        "timezone_offset":    record.get("timezone_offset"),
        "score_state":        record.get("score_state"),
        "strain":             score.get("strain"),
        "kilojoule":          score.get("kilojoule"),
        "average_heart_rate": score.get("average_heart_rate"),
        "max_heart_rate":     score.get("max_heart_rate"),
        "ingested_at":        parse_ts(ingested_at),
    }


def records_from_key(s3, key: str) -> list[dict]:
    """Download one bronze JSON file from MinIO and extract cycle records."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    payload = json.loads(obj["Body"].read())
    ingested_at = payload.get("meta", {}).get("ingested_at", "")
    raw_records = payload.get("data", {}).get("records", [])
    return [flatten_record(r, ingested_at) for r in raw_records]


def dicts_to_arrow(rows: list[dict]) -> pa.Table:
    """Convert list of flat dicts to a PyArrow Table for writing to Iceberg."""
    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in ARROW_SCHEMA}, schema=ARROW_SCHEMA)

    columns = {f.name: [] for f in ARROW_SCHEMA}
    for row in rows:
        for f in ARROW_SCHEMA:
            columns[f.name].append(row.get(f.name))

    arrays = {f.name: pa.array(columns[f.name], type=f.type) for f in ARROW_SCHEMA}
    return pa.table(arrays, schema=ARROW_SCHEMA)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    s3 = get_s3_client()

    # Step 1: Find all bronze cycle files in MinIO
    print("Listing bronze cycle files...")
    keys = list_bronze_keys(s3)
    if not keys:
        print("No bronze cycle files found. Run whoop_cycle_backfill.py first.")
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

    # Step 3: Deduplicate by cycle_id (keep latest ingested_at)
    seen: dict[int, dict] = {}
    for row in all_rows:
        cid = row["cycle_id"]
        if cid is None:
            continue
        existing = seen.get(cid)
        if existing is None or (row["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)) > (existing["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)):
            seen[cid] = row

    deduped = sorted(seen.values(), key=lambda r: r["start_time"] or datetime.min.replace(tzinfo=timezone.utc))
    print(f"After dedup: {len(deduped)} unique cycle(s)")

    # Step 4: Convert to PyArrow columnar format
    arrow_table = dicts_to_arrow(deduped)

    # Step 5: Connect to Iceberg catalog (metadata in Postgres)
    catalog = load_catalog(
        "whoop",
        **{
            "type": "sql",
            "uri": PG_URI,
            "warehouse": WAREHOUSE,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )

    # Step 6: Ensure silver namespace exists
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"Created namespace '{NAMESPACE}'")
    except NamespaceAlreadyExistsError:
        pass

    # Step 7: Write to Iceberg (overwrite on re-runs, create on first run)
    full_name = f"{NAMESPACE}.{TABLE_NAME}"
    try:
        table = catalog.load_table(full_name)
        print(f"Loaded existing table '{full_name}'")
        table.overwrite(arrow_table)
    except NoSuchTableError:
        table = catalog.create_table(
            full_name,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )
        print(f"Created table '{full_name}'")
        table.append(arrow_table)

    print(f"\nDone. Wrote {len(deduped)} cycle(s) to {full_name}")
    print(f"Query in Trino: SELECT * FROM whoop.silver.cycles ORDER BY start_time DESC LIMIT 10;")


if __name__ == "__main__":
    main()
