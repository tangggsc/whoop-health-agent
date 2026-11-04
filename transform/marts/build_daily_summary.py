"""
Silver -> Gold: Build the conformed daily health summary.

This is the analytics-ready model the AI agent and UI query. It collapses the
four per-domain silver tables into ONE row per physiological cycle (a Whoop
"day"), so a question like "how did last night's sleep drive today's recovery
and strain?" is a single-row lookup instead of a four-way join.

Grain:   one row per cycle_id (one Whoop physiological day)
Reads:   silver.cycles, silver.recovery, silver.sleep, silver.workouts
Writes:  s3://whoop-lakehouse/warehouse/gold/daily_summary  (Iceberg, partitioned by month)

What this layer adds on top of silver:
  - Joins all four domains on cycle_id (the backbone key).
  - Normalizes units: sleep/need durations ms -> minutes; kilojoule -> calories.
  - Derives metrics: total_sleep_min, sleep_needed_min, workout rollups per day.

Join model:
  cycles      one per cycle_id            -> the base row
  recovery    one per cycle_id            -> left join on cycle_id
  sleep       many per cycle_id           -> pick the main sleep (recovery.sleep_id,
                                             else longest non-nap sleep for the cycle)
  workouts    many per cycle_id           -> aggregate (count + sums) per cycle_id

Usage:
    cd api && python ../transform/marts/build_daily_summary.py

    # Run the silver flatten scripts first so the source tables exist:
    #   python ../transform/process/flatten_cycles.py
    #   python ../transform/process/flatten_recovery.py
    #   python ../transform/process/flatten_sleep.py
    #   python ../transform/process/flatten_workouts.py
"""

import os
import sys
from datetime import datetime, timezone, timedelta, date

from dotenv import load_dotenv
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
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
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
PG_URI = os.getenv(
    "ICEBERG_CATALOG_URI",
    "postgresql+psycopg2://hive:hive@localhost:5433/metastore",
)
WAREHOUSE = f"s3://{BUCKET}/warehouse"
NAMESPACE = "gold"
TABLE_NAME = "daily_summary"

KJ_TO_KCAL = 0.239006  # 1 kilojoule = 0.239006 kilocalories

# ---------------------------------------------------------------------------
# Iceberg schema — one row per cycle (physiological day)
# ---------------------------------------------------------------------------
ICEBERG_SCHEMA = Schema(
    # --- identity / time ---
    NestedField(1,  "cycle_id",                 LongType(),        required=True),  # backbone join key
    NestedField(2,  "user_id",                  LongType()),
    NestedField(3,  "summary_date",             DateType()),                        # local calendar day of cycle start
    NestedField(4,  "cycle_start",              TimestamptzType()),
    NestedField(5,  "cycle_end",                TimestamptzType()),                 # null if cycle still open
    NestedField(6,  "timezone_offset",          StringType()),
    # --- strain / energy (from cycles) ---
    NestedField(7,  "day_strain",               DoubleType()),
    NestedField(8,  "day_kilojoule",            DoubleType()),
    NestedField(9,  "day_calories",             DoubleType()),                      # derived: kilojoule * 0.239006
    NestedField(10, "day_avg_heart_rate",       IntegerType()),
    NestedField(11, "day_max_heart_rate",       IntegerType()),
    # --- recovery (from recovery, joined on cycle_id) ---
    NestedField(12, "recovery_score",           DoubleType()),
    NestedField(13, "resting_heart_rate",       DoubleType()),
    NestedField(14, "hrv_ms",                   DoubleType()),                      # hrv_rmssd_milli (already ms)
    NestedField(15, "spo2_percentage",          DoubleType()),
    NestedField(16, "skin_temp_celsius",        DoubleType()),
    NestedField(17, "user_calibrating",         BooleanType()),
    # --- sleep (main sleep for the cycle) ---
    NestedField(18, "sleep_id",                 StringType()),
    NestedField(19, "sleep_start",              TimestamptzType()),
    NestedField(20, "sleep_end",                TimestamptzType()),
    NestedField(21, "is_nap",                   BooleanType()),
    NestedField(22, "sleep_performance_pct",    DoubleType()),
    NestedField(23, "sleep_consistency_pct",    DoubleType()),
    NestedField(24, "sleep_efficiency_pct",     DoubleType()),
    NestedField(25, "respiratory_rate",         DoubleType()),
    NestedField(26, "sleep_cycle_count",        IntegerType()),
    NestedField(27, "disturbance_count",        IntegerType()),
    # sleep durations normalized ms -> minutes
    NestedField(28, "time_in_bed_min",          DoubleType()),
    NestedField(29, "awake_min",                DoubleType()),
    NestedField(30, "light_sleep_min",          DoubleType()),
    NestedField(31, "deep_sleep_min",           DoubleType()),                      # slow-wave sleep
    NestedField(32, "rem_sleep_min",            DoubleType()),
    NestedField(33, "total_sleep_min",          DoubleType()),                      # derived: light + deep + rem
    NestedField(34, "sleep_needed_min",         DoubleType()),                      # derived: sum of 4 need components
    NestedField(35, "sleep_debt_min",           DoubleType()),
    # --- workout rollup (aggregated per cycle_id) ---
    NestedField(36, "workout_count",            IntegerType()),
    NestedField(37, "workout_total_strain",     DoubleType()),
    NestedField(38, "workout_total_kilojoule",  DoubleType()),
    NestedField(39, "workout_total_duration_min", DoubleType()),
    NestedField(40, "workout_total_distance_m", DoubleType()),
    # --- lineage ---
    NestedField(41, "built_at",                 TimestamptzType()),                 # when this gold row was built
)

# Daily grain -> partition by month so we don't create one tiny file per day.
PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=MonthTransform(), name="cycle_month")
)

# PyArrow schema — must match ICEBERG_SCHEMA above, same field order.
ARROW_SCHEMA = pa.schema([
    pa.field("cycle_id",                 pa.int64(), nullable=False),
    pa.field("user_id",                  pa.int64()),
    pa.field("summary_date",             pa.date32()),
    pa.field("cycle_start",              pa.timestamp("us", tz="UTC")),
    pa.field("cycle_end",                pa.timestamp("us", tz="UTC")),
    pa.field("timezone_offset",          pa.string()),
    pa.field("day_strain",               pa.float64()),
    pa.field("day_kilojoule",            pa.float64()),
    pa.field("day_calories",             pa.float64()),
    pa.field("day_avg_heart_rate",       pa.int32()),
    pa.field("day_max_heart_rate",       pa.int32()),
    pa.field("recovery_score",           pa.float64()),
    pa.field("resting_heart_rate",       pa.float64()),
    pa.field("hrv_ms",                   pa.float64()),
    pa.field("spo2_percentage",          pa.float64()),
    pa.field("skin_temp_celsius",        pa.float64()),
    pa.field("user_calibrating",         pa.bool_()),
    pa.field("sleep_id",                 pa.string()),
    pa.field("sleep_start",              pa.timestamp("us", tz="UTC")),
    pa.field("sleep_end",                pa.timestamp("us", tz="UTC")),
    pa.field("is_nap",                   pa.bool_()),
    pa.field("sleep_performance_pct",    pa.float64()),
    pa.field("sleep_consistency_pct",    pa.float64()),
    pa.field("sleep_efficiency_pct",     pa.float64()),
    pa.field("respiratory_rate",         pa.float64()),
    pa.field("sleep_cycle_count",        pa.int32()),
    pa.field("disturbance_count",        pa.int32()),
    pa.field("time_in_bed_min",          pa.float64()),
    pa.field("awake_min",                pa.float64()),
    pa.field("light_sleep_min",          pa.float64()),
    pa.field("deep_sleep_min",           pa.float64()),
    pa.field("rem_sleep_min",            pa.float64()),
    pa.field("total_sleep_min",          pa.float64()),
    pa.field("sleep_needed_min",         pa.float64()),
    pa.field("sleep_debt_min",           pa.float64()),
    pa.field("workout_count",            pa.int32()),
    pa.field("workout_total_strain",     pa.float64()),
    pa.field("workout_total_kilojoule",  pa.float64()),
    pa.field("workout_total_duration_min", pa.float64()),
    pa.field("workout_total_distance_m", pa.float64()),
    pa.field("built_at",                 pa.timestamp("us", tz="UTC")),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_catalog():
    return load_catalog(
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


def load_silver(catalog, table_name: str) -> list[dict]:
    """Load a full silver table as a list of row dicts (small, personal-scale data)."""
    try:
        table = catalog.load_table(f"silver.{table_name}")
    except NoSuchTableError:
        print(f"  WARNING: silver.{table_name} does not exist — treating as empty.")
        return []
    rows = table.scan().to_arrow().to_pylist()
    print(f"  silver.{table_name}: {len(rows)} row(s)")
    return rows


def ms_to_min(value) -> float | None:
    """Convert a millisecond duration to minutes, rounded to 1 decimal."""
    if value is None:
        return None
    return round(value / 60000.0, 1)


def minutes_between(start, end) -> float | None:
    """Minutes between two tz-aware datetimes, rounded to 1 decimal."""
    if start is None or end is None:
        return None
    return round((end - start).total_seconds() / 60.0, 1)


def local_date(dt: datetime | None, offset: str | None) -> date | None:
    """
    Calendar day the cycle belongs to, in the user's local time.

    Whoop timestamps are UTC; timezone_offset looks like "-08:00". Applying the
    offset before taking .date() keeps a cycle that starts at 11pm local on the
    correct calendar day instead of rolling into UTC tomorrow.
    """
    if dt is None:
        return None
    if offset and len(offset) >= 6 and offset[0] in "+-":
        try:
            sign = 1 if offset[0] == "+" else -1
            hours = int(offset[1:3])
            minutes = int(offset[4:6])
            dt = dt + sign * timedelta(hours=hours, minutes=minutes)
        except ValueError:
            pass  # malformed offset -> fall back to UTC date
    return dt.date()


def sum_opt(values) -> float | None:
    """Sum values, ignoring None. Returns None only if every value is None."""
    present = [v for v in values if v is not None]
    return sum(present) if present else None


def pick_main_sleep(cycle_id: int, recovery: dict | None,
                    sleep_by_id: dict, sleep_by_cycle: dict) -> dict | None:
    """
    Choose the one sleep that represents the cycle's main night.

    Preference order:
      1. The sleep recovery was scored from (recovery.sleep_id) — the ground truth.
      2. The longest non-nap sleep linked to this cycle.
      3. The longest sleep of any kind linked to this cycle.
    """
    if recovery and recovery.get("sleep_id") in sleep_by_id:
        return sleep_by_id[recovery["sleep_id"]]

    candidates = sleep_by_cycle.get(cycle_id, [])
    if not candidates:
        return None

    def duration(s: dict) -> int:
        return sum_opt([
            s.get("total_light_sleep_time_milli"),
            s.get("total_slow_wave_sleep_time_milli"),
            s.get("total_rem_sleep_time_milli"),
        ]) or 0

    non_naps = [s for s in candidates if not s.get("nap")]
    pool = non_naps or candidates
    return max(pool, key=duration)


def aggregate_workouts(workouts: list[dict]) -> dict[int, dict]:
    """Roll workouts up to one summary per cycle_id."""
    agg: dict[int, dict] = {}
    for w in workouts:
        cid = w.get("cycle_id")
        if cid is None:
            continue
        a = agg.setdefault(cid, {
            "workout_count": 0,
            "strain": [],
            "kilojoule": [],
            "duration_min": [],
            "distance_m": [],
        })
        a["workout_count"] += 1
        a["strain"].append(w.get("strain"))
        a["kilojoule"].append(w.get("kilojoule"))
        a["duration_min"].append(minutes_between(w.get("start_time"), w.get("end_time")))
        a["distance_m"].append(w.get("distance_meter"))
    return agg


def build_row(cycle: dict, recovery: dict | None, sleep: dict | None,
              wagg: dict | None, built_at: datetime) -> dict:
    """Assemble one conformed daily_summary row from the joined sources."""
    cid = cycle.get("cycle_id")
    rec = recovery or {}
    slp = sleep or {}

    total_sleep_min = sum_opt([
        ms_to_min(slp.get("total_light_sleep_time_milli")),
        ms_to_min(slp.get("total_slow_wave_sleep_time_milli")),
        ms_to_min(slp.get("total_rem_sleep_time_milli")),
    ])
    sleep_needed_min = sum_opt([
        ms_to_min(slp.get("baseline_milli")),
        ms_to_min(slp.get("need_from_sleep_debt_milli")),
        ms_to_min(slp.get("need_from_recent_strain_milli")),
        ms_to_min(slp.get("need_from_recent_nap_milli")),
    ])
    kj = cycle.get("kilojoule")

    return {
        "cycle_id":                 cid,
        "user_id":                  cycle.get("user_id"),
        "summary_date":             local_date(cycle.get("start_time"), cycle.get("timezone_offset")),
        "cycle_start":              cycle.get("start_time"),
        "cycle_end":                cycle.get("end_time"),
        "timezone_offset":          cycle.get("timezone_offset"),
        "day_strain":               cycle.get("strain"),
        "day_kilojoule":            kj,
        "day_calories":             round(kj * KJ_TO_KCAL, 1) if kj is not None else None,
        "day_avg_heart_rate":       cycle.get("average_heart_rate"),
        "day_max_heart_rate":       cycle.get("max_heart_rate"),
        "recovery_score":           rec.get("recovery_score"),
        "resting_heart_rate":       rec.get("resting_heart_rate"),
        "hrv_ms":                   rec.get("hrv_rmssd_milli"),
        "spo2_percentage":          rec.get("spo2_percentage"),
        "skin_temp_celsius":        rec.get("skin_temp_celsius"),
        "user_calibrating":         rec.get("user_calibrating"),
        "sleep_id":                 slp.get("sleep_id"),
        "sleep_start":              slp.get("start_time"),
        "sleep_end":                slp.get("end_time"),
        "is_nap":                   slp.get("nap"),
        "sleep_performance_pct":    slp.get("sleep_performance_percentage"),
        "sleep_consistency_pct":    slp.get("sleep_consistency_percentage"),
        "sleep_efficiency_pct":     slp.get("sleep_efficiency_percentage"),
        "respiratory_rate":         slp.get("respiratory_rate"),
        "sleep_cycle_count":        slp.get("sleep_cycle_count"),
        "disturbance_count":        slp.get("disturbance_count"),
        "time_in_bed_min":          ms_to_min(slp.get("total_in_bed_time_milli")),
        "awake_min":                ms_to_min(slp.get("total_awake_time_milli")),
        "light_sleep_min":          ms_to_min(slp.get("total_light_sleep_time_milli")),
        "deep_sleep_min":           ms_to_min(slp.get("total_slow_wave_sleep_time_milli")),
        "rem_sleep_min":            ms_to_min(slp.get("total_rem_sleep_time_milli")),
        "total_sleep_min":          total_sleep_min,
        "sleep_needed_min":         sleep_needed_min,
        "sleep_debt_min":           ms_to_min(slp.get("need_from_sleep_debt_milli")),
        "workout_count":            (wagg or {}).get("workout_count", 0),
        "workout_total_strain":     sum_opt((wagg or {}).get("strain", [])),
        "workout_total_kilojoule":  sum_opt((wagg or {}).get("kilojoule", [])),
        "workout_total_duration_min": sum_opt((wagg or {}).get("duration_min", [])),
        "workout_total_distance_m": sum_opt((wagg or {}).get("distance_m", [])),
        "built_at":                 built_at,
    }


def dicts_to_arrow(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in ARROW_SCHEMA}, schema=ARROW_SCHEMA)
    columns = {f.name: [row.get(f.name) for row in rows] for f in ARROW_SCHEMA}
    arrays = {f.name: pa.array(columns[f.name], type=f.type) for f in ARROW_SCHEMA}
    return pa.table(arrays, schema=ARROW_SCHEMA)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    catalog = get_catalog()

    # Step 1: Load the four silver tables.
    print("Loading silver tables...")
    cycles = load_silver(catalog, "cycles")
    recovery = load_silver(catalog, "recovery")
    sleep = load_silver(catalog, "sleep")
    workouts = load_silver(catalog, "workouts")

    if not cycles:
        print("No cycles in silver — run the flatten scripts first. Nothing to build.")
        sys.exit(0)

    # Step 2: Index the dimensions for lookup.
    recovery_by_cycle = {r["cycle_id"]: r for r in recovery if r.get("cycle_id") is not None}
    sleep_by_id = {s["sleep_id"]: s for s in sleep if s.get("sleep_id") is not None}
    sleep_by_cycle: dict[int, list[dict]] = {}
    for s in sleep:
        cid = s.get("cycle_id")
        if cid is not None:
            sleep_by_cycle.setdefault(cid, []).append(s)
    workout_agg = aggregate_workouts(workouts)

    # Step 3: Build one conformed row per cycle.
    built_at = datetime.now(timezone.utc)
    rows = []
    matched_recovery = matched_sleep = 0
    for cycle in cycles:
        cid = cycle.get("cycle_id")
        if cid is None:
            continue
        rec = recovery_by_cycle.get(cid)
        slp = pick_main_sleep(cid, rec, sleep_by_id, sleep_by_cycle)
        if rec:
            matched_recovery += 1
        if slp:
            matched_sleep += 1
        rows.append(build_row(cycle, rec, slp, workout_agg.get(cid), built_at))

    rows.sort(key=lambda r: r["cycle_start"] or datetime.min.replace(tzinfo=timezone.utc))
    print(f"\nBuilt {len(rows)} daily row(s): "
          f"{matched_recovery} with recovery, {matched_sleep} with sleep, "
          f"{len(workout_agg)} day(s) with workouts.")

    arrow_table = dicts_to_arrow(rows)

    # Step 4: Ensure the gold namespace exists.
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"Created namespace '{NAMESPACE}'")
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Write to Iceberg. Full rebuild from silver, so overwrite on re-runs.
    full_name = f"{NAMESPACE}.{TABLE_NAME}"
    try:
        table = catalog.load_table(full_name)
        print(f"Loaded existing table '{full_name}'")

        # Evolve the schema if we've added columns since the table was created.
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

    print(f"\nDone. Wrote {len(rows)} row(s) to {full_name}")
    print("Query in Trino: "
          "SELECT summary_date, day_strain, recovery_score, total_sleep_min "
          "FROM whoop.gold.daily_summary ORDER BY summary_date DESC LIMIT 10;")


if __name__ == "__main__":
    main()
