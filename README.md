# whoop-health-agent
An AI-powered personal health analytics agent built on exported Whoop wearable data.
The project follows a **local-first architecture**, enabling full data ownership,
fast iteration, and optional cloud deployment later.

---

## 🎯 Project Goals
- Export and own my Whoop health data
- Build a local analytics pipeline for wearable telemetry
- Embed an AI agent that answers health questions in natural language
- Generate explainable insights with visualizations

---

## 🧠 What This Agent Can Do
Example questions the agent is designed to answer:

- Why was my recovery low last week?
- How does strain impact my HRV over time?
- Compare my sleep quality this month vs last month
- Show correlations between sleep duration and recovery score

---

## 🏗️ Architecture (Local-First)
```
Whoop API -> Local Ingestion (Python)
  -> Bronze Layer — raw JSON (MinIO object storage)
  -> Silver Layer — flattened, typed, deduped per-domain Iceberg tables
  -> Gold Layer  — conformed, analytics-ready Iceberg models
  -> FastAPI Data Access Layer (MCP-style)
  -> LangGraph AI Agent (LLM-powered)
  -> Streamlit / Web UI (Chat + Visualizations)
```

**Data Flow (medallion: Bronze → Silver → Gold):**
- **Bronze** — raw Whoop v2 API JSON, one file per endpoint page per day. Written by `api/whoop_*_backfill.py`.
- **Silver** — one flat Iceberg table per domain (`cycles`, `recovery`, `sleep`, `workouts`): nested JSON flattened, types cast, deduped by id. Written by `transform/process/flatten_*.py`.
- **Gold** — conformed models that join domains for the agent/UI (`daily_summary`: one row per cycle, units normalized, workouts rolled up). Written by `transform/marts/build_daily_summary.py`.
- **Iceberg**: table format for ACID transactions, schema evolution, and time travel.
- **Storage**: MinIO (S3-compatible) for Parquet data + Iceberg metadata.
- **Query**: Trino for SQL access to Iceberg tables.
- **Serve**: FastAPI exposes data to the AI agent via MCP protocol.

---

## 🛠️ Tech Stack
- Python
- FastAPI
- LangGraph
- Parquet / Iceberg (MinIO)
- Trino (SQL query engine)
- Streamlit
- LLM (local or API-based)

---

## 🚧 Project Status

### ✅ Completed
- [x] Lakehouse infrastructure — MinIO + Hive metastore + Trino + Iceberg (`orchestration/docker-compose.yml`)
- [x] Bronze layer — Whoop v2 API client + auth, raw JSON ingestion for cycles, recovery, sleep, and workouts
- [x] Silver layer — flatten / clean / dedupe into per-domain Iceberg tables (`cycles`, `recovery`, `sleep`, `workouts`)
- [x] Gold layer — `gold.daily_summary` conformed daily model (one row per cycle: strain + recovery + sleep + workout rollup, units normalized)

### 📋 Planned
- [ ] FastAPI data access layer (MCP-style)
- [ ] LangGraph AI agent (LLM-powered)
- [ ] Streamlit / Web UI
- [ ] Orchestration (Airflow) + incremental daily ingestion

### ⚠️ Known Gaps
- Backfill date ranges differ across domains (workouts cover 2025-10 → 2026-01, cycles 2025-12 → 2026-04), so only the overlapping window has complete cross-domain coverage. Re-running the cycle / recovery / sleep backfills over the full workout window will close this.

### Current Focus
Transform pipeline foundation is complete (Bronze → Silver → Gold). Next: the FastAPI data access layer so the AI agent can query `gold.daily_summary`.

