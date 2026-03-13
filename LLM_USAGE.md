# LLM Usage Log

## Overview

This project was developed with selective AI assistance to accelerate implementation, improve code structure, and refine the overall analytics architecture.

The primary AI tool used during development was **ChatGPT**.

AI support was used mainly for:

- project planning and architecture suggestions
- code scaffolding for ingestion, processing, analytics, and dashboard layers
- debugging and refining data transformation logic
- proposing SQL views and DuckDB integration
- improving dashboard layout and presentation quality
- drafting project documentation

All generated outputs were manually reviewed, adapted, tested, and integrated into the final solution.

---

## Tools Used

- **ChatGPT**  
  Used for implementation support, architectural guidance, debugging assistance, dashboard refinement, and documentation drafting.

---

## How AI Was Used

### 1. Project Architecture
AI was used to propose a modular analytics project structure with separate layers for:

- ingestion
- processing
- database/storage
- analytics
- dashboard
- documentation

This helped shape the final repository layout and workflow.

### 2. Data Ingestion
AI assisted in designing the ingestion logic for parsing nested telemetry JSONL batches and flattening them into structured tabular data.

This contributed to the implementation of:

- `src/ingestion/parse_logs.py`

### 3. Data Processing
AI was used to refine the transformation pipeline, including:

- timestamp normalization
- numeric and boolean type conversion
- feature engineering
- employee metadata enrichment
- session-level and user-level aggregations
- data quality reporting

This contributed to the implementation of:

- `src/processing/transform_events.py`

### 4. Analytical Storage
AI helped evaluate storage design options and supported the decision to use **DuckDB** as the analytical storage layer.

AI also helped draft reusable SQL views for business-ready analytics.

This contributed to:

- `src/database/db.py`
- `src/database/schema.sql`

### 5. Dashboard Design
AI was used to suggest:

- KPI selection
- chart structure
- layout improvements
- filtering behavior
- presentation polish for a dashboard-oriented analytics experience

This contributed to:

- `src/dashboard/app.py`

### 6. Documentation
AI helped draft and refine:

- `README.md`
- `LLM_USAGE.md`
- presentation/storyline structure for the final submission

---

## Example Prompts

### Ingestion
> Generate a Python script that reads nested telemetry JSONL log batches, extracts event payloads, and saves the output as a parquet dataset.

### Data Processing
> Help design a processing layer that cleans telemetry events, normalizes data types, enriches employee metadata, and creates session- and user-level aggregates.

### Storage Layer
> Suggest a lightweight SQL-based analytical storage approach for a local telemetry analytics project and provide example DuckDB views for usage, cost, and reliability metrics.

### Dashboard
> Propose a Streamlit dashboard layout for telemetry analytics focused on usage trends, cost drivers, reliability, and developer behavior.

### Documentation
> Draft a README for an end-to-end telemetry analytics platform with ingestion, transformation, DuckDB storage, analytics exports, and a Streamlit dashboard.

---

## Validation Process

AI-generated suggestions were not used blindly.

Validation steps included:

- running all scripts locally
- checking parquet outputs and DuckDB tables
- verifying row counts between layers
- confirming that aggregate metrics matched source data
- manually reviewing sample records
- debugging schema and type issues during development
- refining generated code to match project requirements and expected outputs

Examples of manual validation include:

- verifying processed event counts against parsed input counts
- confirming total tokens and total cost consistency across processing, DuckDB, and metrics exports
- validating dashboard charts against exported analytics tables
- fixing type conversion issues such as boolean normalization for `success`

---

## Notes

AI was used as a development accelerator and ideation tool, while the final implementation, integration, debugging, and validation were completed manually.

The final repository reflects manually verified code and project-specific adaptations rather than raw AI-generated output.