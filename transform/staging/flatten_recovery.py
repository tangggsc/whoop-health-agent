"""
Bronze -> Silver: Flatten raw recovery JSON from MinIO into an Iceberg table.

Reads:  s3://whoop-lakehouse/bronze/whoop/recovery/dt=YYYY-MM-DD/*.json
Writes: s3://whoop-lakehouse/warehouse/silver/recovery  (Iceberg, partitioned by day)

Usage:
    cd api && python ../transform/staging/flatten_recovery.py

Raw recovery record shape from Whoop API:
{
    "cycle_id": 12345678,       # Primary key — recovery has no separate id
    "sleep_id": "abc123",
    "user_id": 123,
    "created_at": "2026-01-01T08:00:00.000Z",
    "updated_at": "2026-01-01T08:00:00.000Z",
    "score_state": "SCORED",
    "score": {
        "user_calibrating": false,
        "recovery_score": 75.0,
        "resting_heart_rate": 52.0,
        "hrv_rmssd_milli": 68.5,
        "spo2_percentage": 97.0,
        "skin_temp_celsius": 34.2
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
    BooleanType,
    DoubleType,
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
BRONZE_PREFIX = os.getenv("WHOOP_BRONZE_PREFIX", "bronze/whoop") + "/recovery/"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
PG_URI = os.getenv(
    "ICEBERG_CATALOG_URI",
    "postgresql+psycopg2://hive:hive@localhost:5433/metastore",
)
WAREHOUSE = f"s3://{BUCKET}/warehouse"
NAMESPACE = "silver"
TABLE_NAME = "recovery"

# ---------------------------------------------------------------------------
# Iceberg schema
# ---------------------------------------------------------------------------
# Recovery has no standalone id — cycle_id is the primary key.
ICEBERG_SCHEMA = Schema(
    NestedField(1,  "cycle_id",           LongType(),   required=True),  # Primary key, join to silver.cycles
    NestedField(2,  "sleep_id",           StringType()),                  # Join to silver.sleep
    NestedField(3,  "user_id",            LongType()),
    NestedField(4,  "created_at",         TimestamptzType()),             # Used for partitioning
    NestedField(5,  "updated_at",         TimestamptzType()),
    NestedField(6,  "score_state",        StringType()),
    # --- Flattened from score{} ---
    NestedField(7,  "user_calibrating",   BooleanType()),
    NestedField(8,  "recovery_score",     DoubleType()),
    NestedField(9,  "resting_heart_rate", DoubleType()),
    NestedField(10, "hrv_rmssd_milli",    DoubleType()),
    NestedField(11, "spo2_percentage",    DoubleType()),
    NestedField(12, "skin_temp_celsius",  DoubleType()),
    NestedField(13, "ingested_at",        TimestamptzType()),
)

# Partition by day of created_at (source_id=4 matches created_at field above)
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="created_day")
)

# PyArrow schema — must match Iceberg schema above, same field order
ARROW_SCHEMA = pa.schema([
    pa.field("cycle_id",           pa.int64(),                  nullable=False),
    pa.field("sleep_id",           pa.string()),
    pa.field("user_id",            pa.int64()),
    pa.field("created_at",         pa.timestamp("us", tz="UTC")),
    pa.field("updated_at",         pa.timestamp("us", tz="UTC")),
    pa.field("score_state",        pa.string()),
    pa.field("user_calibrating",   pa.bool_()),
    pa.field("recovery_score",     pa.float64()),
    pa.field("resting_heart_rate", pa.float64()),
    pa.field("hrv_rmssd_milli",    pa.float64()),
    pa.field("spo2_percentage",    pa.float64()),
    pa.field("skin_temp_celsius",  pa.float64()),
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
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BRONZE_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return sorted(keys)


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def flatten_record(record: dict, ingested_at: str) -> dict:
    score = record.get("score") or {}
    return {
        "cycle_id":           record.get("cycle_id"),
        "sleep_id":           record.get("sleep_id"),
        "user_id":            record.get("user_id"),
        "created_at":         parse_ts(record.get("created_at")),
        "updated_at":         parse_ts(record.get("updated_at")),
        "score_state":        record.get("score_state"),
        "user_calibrating":   score.get("user_calibrating"),
        "recovery_score":     score.get("recovery_score"),
        "resting_heart_rate": score.get("resting_heart_rate"),
        "hrv_rmssd_milli":    score.get("hrv_rmssd_milli"),
        "spo2_percentage":    score.get("spo2_percentage"),
        "skin_temp_celsius":  score.get("skin_temp_celsius"),
        "ingested_at":        parse_ts(ingested_at),
    }


def records_from_key(s3, key: str) -> list[dict]:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    payload = json.loads(obj["Body"].read())
    ingested_at = payload.get("meta", {}).get("ingested_at", "")
    raw_records = payload.get("data", {}).get("records", [])
    return [flatten_record(r, ingested_at) for r in raw_records]


def dicts_to_arrow(rows: list[dict]) -> pa.Table:
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

    print("Listing bronze recovery files...")
    keys = list_bronze_keys(s3)
    if not keys:
        print("No bronze recovery files found. Run whoop_recovery_backfill.py first.")
        sys.exit(0)
    print(f"Found {len(keys)} file(s)")

    all_rows = []
    for key in keys:
        rows = records_from_key(s3, key)
        all_rows.extend(rows)
        print(f"  {key} -> {len(rows)} record(s)")

    print(f"\nTotal records to write: {len(all_rows)}")
    if not all_rows:
        print("No records found in bronze files.")
        sys.exit(0)

    # Deduplicate by cycle_id (recovery's primary key)
    seen: dict[int, dict] = {}
    for row in all_rows:
        cid = row["cycle_id"]
        if cid is None:
            continue
        existing = seen.get(cid)
        if existing is None or (row["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)) > (existing["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)):
            seen[cid] = row

    deduped = sorted(seen.values(), key=lambda r: r["created_at"] or datetime.min.replace(tzinfo=timezone.utc))
    print(f"After dedup: {len(deduped)} unique recovery record(s)")

    arrow_table = dicts_to_arrow(deduped)

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

    try:
        catalog.create_namespace(NAMESPACE)
        print(f"Created namespace '{NAMESPACE}'")
    except NamespaceAlreadyExistsError:
        pass

    full_name = f"{NAMESPACE}.{TABLE_NAME}"
    try:
        table = catalog.load_table(full_name)
        print(f"Loaded existing table '{full_name}'")

        existing_names = {f.name for f in table.schema().fields}
        missing = [f for f in ICEBERG_SCHEMA.fields if f.name not in existing_names]
        if missing:
            with table.update_schema() as upd:
                for f in missing:
                    upd.add_column(f.name, f.field_type)
            print(f"Schema evolved: added {[f.name for f in missing]}")

        table.overwrite(arrow_table)
    except NoSuchTableError:
        table = catalog.create_table(
            full_name,
            schema=ICEBERG_SCHEMA,
            partition_spec=PARTITION_SPEC,
        )
        print(f"Created table '{full_name}'")
        table.append(arrow_table)

    print(f"\nDone. Wrote {len(deduped)} recovery record(s) to {full_name}")
    print(f"Query in Trino: SELECT * FROM whoop.silver.recovery ORDER BY created_at DESC LIMIT 10;")


if __name__ == "__main__":
    main()
