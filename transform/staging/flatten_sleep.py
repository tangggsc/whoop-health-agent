"""
Bronze -> Silver: Flatten raw sleep JSON from MinIO into an Iceberg table.

Reads:  s3://whoop-lakehouse/bronze/whoop/sleep/dt=YYYY-MM-DD/*.json
Writes: s3://whoop-lakehouse/warehouse/silver/sleep  (Iceberg, partitioned by day)

Usage:
    cd api && python ../transform/staging/flatten_sleep.py

Raw sleep record shape from Whoop API:
{
    "id": "abc123",
    "cycle_id": 12345678,
    "user_id": 123,
    "created_at": "2026-01-01T08:00:00.000Z",
    "updated_at": "2026-01-01T08:00:00.000Z",
    "start": "2026-01-01T00:00:00.000Z",
    "end": "2026-01-01T08:00:00.000Z",
    "timezone_offset": "-08:00",
    "nap": false,
    "score_state": "SCORED",
    "score": {
        "stage_summary": {
            "total_in_bed_time_milli": 28800000,
            "total_awake_time_milli": 1800000,
            "total_no_data_time_milli": 0,
            "total_light_sleep_time_milli": 10800000,
            "total_slow_wave_sleep_time_milli": 7200000,
            "total_rem_sleep_time_milli": 9000000,
            "sleep_cycle_count": 4,
            "disturbance_count": 3
        },
        "sleep_needed": {
            "baseline_milli": 27060000,
            "need_from_sleep_debt_milli": 0,
            "need_from_recent_strain_milli": 1200000,
            "need_from_recent_nap_milli": 0
        },
        "respiratory_rate": 16.2,
        "sleep_performance_percentage": 88.0,
        "sleep_consistency_percentage": 72.0,
        "sleep_efficiency_percentage": 93.5
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
BRONZE_PREFIX = os.getenv("WHOOP_BRONZE_PREFIX", "bronze/whoop") + "/sleep/"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
PG_URI = os.getenv(
    "ICEBERG_CATALOG_URI",
    "postgresql+psycopg2://hive:hive@localhost:5433/metastore",
)
WAREHOUSE = f"s3://{BUCKET}/warehouse"
NAMESPACE = "silver"
TABLE_NAME = "sleep"

# ---------------------------------------------------------------------------
# Iceberg schema
# ---------------------------------------------------------------------------
ICEBERG_SCHEMA = Schema(
    NestedField(1,  "sleep_id",                         StringType(),      required=True),
    NestedField(2,  "cycle_id",                         LongType()),                      # Join key to silver.cycles
    NestedField(3,  "user_id",                          LongType()),
    NestedField(4,  "created_at",                       TimestamptzType()),
    NestedField(5,  "updated_at",                       TimestamptzType()),
    NestedField(6,  "start_time",                       TimestamptzType()),               # Used for partitioning
    NestedField(7,  "end_time",                         TimestamptzType()),
    NestedField(8,  "timezone_offset",                  StringType()),
    NestedField(9,  "nap",                              BooleanType()),                   # True if nap, not main sleep
    NestedField(10, "score_state",                      StringType()),
    # --- Flattened from score.stage_summary{} ---
    NestedField(11, "total_in_bed_time_milli",          LongType()),
    NestedField(12, "total_awake_time_milli",            LongType()),
    NestedField(13, "total_no_data_time_milli",          LongType()),
    NestedField(14, "total_light_sleep_time_milli",      LongType()),
    NestedField(15, "total_slow_wave_sleep_time_milli",  LongType()),
    NestedField(16, "total_rem_sleep_time_milli",        LongType()),
    NestedField(17, "sleep_cycle_count",                 IntegerType()),
    NestedField(18, "disturbance_count",                 IntegerType()),
    # --- Flattened from score.sleep_needed{} ---
    NestedField(19, "baseline_milli",                    LongType()),
    NestedField(20, "need_from_sleep_debt_milli",        LongType()),
    NestedField(21, "need_from_recent_strain_milli",     LongType()),
    NestedField(22, "need_from_recent_nap_milli",        LongType()),
    # --- From score{} directly ---
    NestedField(23, "respiratory_rate",                  DoubleType()),
    NestedField(24, "sleep_performance_percentage",      DoubleType()),
    NestedField(25, "sleep_consistency_percentage",      DoubleType()),
    NestedField(26, "sleep_efficiency_percentage",       DoubleType()),
    NestedField(27, "ingested_at",                       TimestamptzType()),
)

# Partition by day of start_time (source_id=6 matches start_time field above)
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="start_day")
)

# PyArrow schema — must match Iceberg schema above, same field order
ARROW_SCHEMA = pa.schema([
    pa.field("sleep_id",                        pa.string(),                 nullable=False),
    pa.field("cycle_id",                        pa.int64()),
    pa.field("user_id",                         pa.int64()),
    pa.field("created_at",                      pa.timestamp("us", tz="UTC")),
    pa.field("updated_at",                      pa.timestamp("us", tz="UTC")),
    pa.field("start_time",                      pa.timestamp("us", tz="UTC")),
    pa.field("end_time",                        pa.timestamp("us", tz="UTC")),
    pa.field("timezone_offset",                 pa.string()),
    pa.field("nap",                             pa.bool_()),
    pa.field("score_state",                     pa.string()),
    pa.field("total_in_bed_time_milli",         pa.int64()),
    pa.field("total_awake_time_milli",          pa.int64()),
    pa.field("total_no_data_time_milli",        pa.int64()),
    pa.field("total_light_sleep_time_milli",    pa.int64()),
    pa.field("total_slow_wave_sleep_time_milli", pa.int64()),
    pa.field("total_rem_sleep_time_milli",      pa.int64()),
    pa.field("sleep_cycle_count",               pa.int32()),
    pa.field("disturbance_count",               pa.int32()),
    pa.field("baseline_milli",                  pa.int64()),
    pa.field("need_from_sleep_debt_milli",      pa.int64()),
    pa.field("need_from_recent_strain_milli",   pa.int64()),
    pa.field("need_from_recent_nap_milli",      pa.int64()),
    pa.field("respiratory_rate",                pa.float64()),
    pa.field("sleep_performance_percentage",    pa.float64()),
    pa.field("sleep_consistency_percentage",    pa.float64()),
    pa.field("sleep_efficiency_percentage",     pa.float64()),
    pa.field("ingested_at",                     pa.timestamp("us", tz="UTC")),
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
    stages = score.get("stage_summary") or {}
    needed = score.get("sleep_needed") or {}
    return {
        "sleep_id":                         str(record["id"]) if record.get("id") is not None else None,
        "cycle_id":                         record.get("cycle_id"),
        "user_id":                          record.get("user_id"),
        "created_at":                       parse_ts(record.get("created_at")),
        "updated_at":                       parse_ts(record.get("updated_at")),
        "start_time":                       parse_ts(record.get("start")),
        "end_time":                         parse_ts(record.get("end")),
        "timezone_offset":                  record.get("timezone_offset"),
        "nap":                              record.get("nap"),
        "score_state":                      record.get("score_state"),
        "total_in_bed_time_milli":          stages.get("total_in_bed_time_milli"),
        "total_awake_time_milli":           stages.get("total_awake_time_milli"),
        "total_no_data_time_milli":         stages.get("total_no_data_time_milli"),
        "total_light_sleep_time_milli":     stages.get("total_light_sleep_time_milli"),
        "total_slow_wave_sleep_time_milli": stages.get("total_slow_wave_sleep_time_milli"),
        "total_rem_sleep_time_milli":       stages.get("total_rem_sleep_time_milli"),
        "sleep_cycle_count":                stages.get("sleep_cycle_count"),
        "disturbance_count":                stages.get("disturbance_count"),
        "baseline_milli":                   needed.get("baseline_milli"),
        "need_from_sleep_debt_milli":       needed.get("need_from_sleep_debt_milli"),
        "need_from_recent_strain_milli":    needed.get("need_from_recent_strain_milli"),
        "need_from_recent_nap_milli":       needed.get("need_from_recent_nap_milli"),
        "respiratory_rate":                 score.get("respiratory_rate"),
        "sleep_performance_percentage":     score.get("sleep_performance_percentage"),
        "sleep_consistency_percentage":     score.get("sleep_consistency_percentage"),
        "sleep_efficiency_percentage":      score.get("sleep_efficiency_percentage"),
        "ingested_at":                      parse_ts(ingested_at),
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

    print("Listing bronze sleep files...")
    keys = list_bronze_keys(s3)
    if not keys:
        print("No bronze sleep files found. Run whoop_sleep_backfill.py first.")
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

    seen: dict[str, dict] = {}
    for row in all_rows:
        sid = row["sleep_id"]
        if sid is None:
            continue
        existing = seen.get(sid)
        if existing is None or (row["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)) > (existing["ingested_at"] or datetime.min.replace(tzinfo=timezone.utc)):
            seen[sid] = row

    deduped = sorted(seen.values(), key=lambda r: r["start_time"] or datetime.min.replace(tzinfo=timezone.utc))
    print(f"After dedup: {len(deduped)} unique sleep record(s)")

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

    print(f"\nDone. Wrote {len(deduped)} sleep record(s) to {full_name}")
    print(f"Query in Trino: SELECT * FROM whoop.silver.sleep ORDER BY start_time DESC LIMIT 10;")


if __name__ == "__main__":
    main()
