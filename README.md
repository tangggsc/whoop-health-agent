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
Whoop API -> Local Ingestion (Python / Cron) 
  -> Raw Layer (JSON storage)
  -> Process Layer (flattened/cleaned)
  -> Staging Layer (normalized schemas)
  -> Marts Layer (analytics-ready models)
  -> Iceberg Tables (Parquet files + metadata)
  -> FastAPI Data Access Layer (MCP-style) 
  -> LangGraph AI Agent (LLM-powered)
  -> Streamlit / Web UI (Chat + Visualizations)
```

**Data Flow:**
- **Raw → Process → Staging → Marts**: Python-based transformation pipeline
- **Iceberg**: Table format for ACID transactions, schema evolution, and time travel
- **Storage**: MinIO (S3-compatible) for Parquet files and Iceberg metadata
- **Query**: Trino for SQL access to Iceberg tables
- **Serve**: FastAPI exposes data to AI agent via MCP protocol

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
- [x] API ingestion layer (raw layer)
  - Whoop API client with authentication
  - Raw JSON storage for workout data

### 🔨 In Progress
- [ ] Process layer
  - Flatten and transform raw JSON
  - Data type conversion and cleaning

### 📋 Planned
- [ ] Staging layer (normalized schemas)
- [ ] Marts layer (analytics models)
- [ ] Data storage (Parquet/Iceberg setup)
- [ ] FastAPI data access layer
- [ ] LangGraph AI agent
- [ ] Streamlit UI
- [ ] Query engine integration (Trino)

### Current Focus
Building data pipeline foundation (raw → process → staging → marts) before implementing AI agent and UI.

