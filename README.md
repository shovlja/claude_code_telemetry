# Claude Code Usage Analytics Platform

An end-to-end analytics platform for processing Claude Code telemetry data and transforming raw event streams into actionable insights about developer behavior, usage trends, cost drivers, reliability, and tool performance.

## Overview

This project processes synthetic Claude Code telemetry logs and builds a complete analytics pipeline that:

- ingests nested JSONL telemetry batches
- cleans, validates, and structures raw event data
- enriches telemetry with employee metadata
- creates curated analytics-ready datasets
- stores processed data in DuckDB
- exposes insights through an interactive Streamlit dashboard

The goal is to help stakeholders understand how Claude Code is being used across engineering teams, which models drive cost, when usage peaks, how tools perform, and where high-intensity sessions or users emerge.

---

## Architecture

The solution is organized into five layers:

### 1. Data Generation
Synthetic telemetry data is produced using the provided generator:

- `generate_fake_data.py`

This creates:
- `telemetry_logs.jsonl`
- `employees.csv`

### 2. Ingestion Layer
- `src/ingestion/parse_logs.py`

This layer:
- reads CloudWatch-style JSONL log batches
- extracts nested event payloads
- flattens telemetry data into a structured event table
- saves parsed events as parquet

### 3. Processing Layer
- `src/processing/transform_events.py`

This layer:
- validates required columns
- normalizes timestamps, booleans, text, and numeric fields
- enriches events with employee metadata
- engineers derived analytics fields
- creates curated datasets:
  - `events_clean.parquet`
  - `api_requests.parquet`
  - `tool_results.parquet`
  - `sessions.parquet`
  - `users.parquet`
- generates `data_quality_report.json`

### 4. Storage Layer
- `src/database/db.py`
- `src/database/schema.sql`

This layer:
- loads curated parquet datasets into DuckDB
- creates reusable SQL analytics views
- provides efficient local analytical storage and querying

### 5. Analytics & Visualization Layer
- `src/analytics/metrics.py`
- `src/dashboard/app.py`

This layer:
- exports analytics-ready summary datasets
- powers the Streamlit dashboard
- visualizes usage, cost, reliability, and developer behavior trends

---

## Project Structure

```text
claude_code_telemetry/
│
├── data/
│   ├── raw/
│   │   ├── telemetry_logs.jsonl
│   │   └── employees.csv
│   └── processed/
│       ├── events.parquet
│       ├── events_clean.parquet
│       ├── api_requests.parquet
│       ├── tool_results.parquet
│       ├── sessions.parquet
│       ├── users.parquet
│       ├── data_quality_report.json
│       ├── claude_code_analytics.duckdb
│       └── metrics/
│
├── src/
│   ├── ingestion/
│   │   └── parse_logs.py
│   ├── processing/
│   │   └── transform_events.py
│   ├── database/
│   │   ├── db.py
│   │   └── schema.sql
│   ├── analytics/
│   │   └── metrics.py
│   └── dashboard/
│       └── app.py
│
├── generate_fake_data.py
├── README.md
├── LLM_USAGE.md
└── requirements.txt