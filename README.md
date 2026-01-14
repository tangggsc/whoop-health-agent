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

Whoop API -> Local Ingestion (Python / Cron) -> Parquet / Iceberg (Local Storage)
-> FastAPI Data Access Layer (MCP-style) -> LangGraph AI Agent (LLM-powered)-> Streamlit / Web UI (Chat + Visualizations)

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
**In progress.**  
Initial focus: data ingestion and daily health metrics.
