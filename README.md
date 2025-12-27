# Week 2 – ETL Pipeline & EDA

This repository contains a complete, reproducible ETL (Extract, Transform, Load)
pipeline built during Week 2 of the AI Professionals Bootcamp.  
The project processes raw orders and users data into clean, analysis-ready
datasets, generates run metadata, and supports reproducible exploratory data
analysis (EDA).

---

## Project Overview

The work was developed incrementally across multiple days and finalized as a
production-style ETL pipeline:

- **Day 1:** Load raw data
- **Day 2:** Data quality checks and cleaning
- **Day 3:** Build analytics table
- **Day 5:** Ship a job-ready ETL pipeline with metadata and reporting handoff

The final pipeline is implemented in `src/bootcamp_data/etl.py` and executed via
`scripts/run_etl.py`.

---

## Project Structure

```text
.
├── data/
│   ├── raw/                    # Raw input data (CSV)
│   ├── processed/              # ETL outputs (Parquet + metadata)
│   ├── cache/                  # Optional cached data
│   └── external/               # Optional external data
├── notebooks/
│   └── eda.ipynb               # Exploratory data analysis notebook
├── reports/
│   ├── figures/                # Exported figures
│   └── summary.md              # Written findings and caveats
├── scripts/
│   ├── run_day1_load.py        # Day 1: load raw data
│   ├── run_day2_clean.py       # Day 2: cleaning and quality checks
│   ├── run_day3_build_analytics.py  # Day 3: analytics table
│   └── run_etl.py              # Final ETL entrypoint (recommended)
├── src/
│   └── bootcamp_data/
│       ├── etl.py              # Final ETL pipeline
│       ├── io.py               # I/O helpers
│       ├── quality.py          # Data quality checks
│       ├── transforms.py       # Cleaning and transformations
│       ├── joins.py            # Safe join utilities
│       └── config.py           # Configuration helpers
├── requirements.txt
├── pyproject.toml
└── README.md
```
## Setup

### 1️⃣ Create a Virtual Environment
```bash
python -m venv .venv
```

### 2️⃣ Activate the Environment
**macOS / Linux**
```bash
source .venv/bin/activate
```
**Windows (PowerShell)**
```powershell
.venv\Scripts\Activate.ps1
```
### 3️⃣ Install Dependencies
**Install dependencies and the project in editable mode:**
```pash
pip install -r requirements.txt
pip install -e .
```

## Running the ETL
### ✅ Recommended (Final Pipeline)
**Run the complete ETL pipeline:**
```pash
python scripts/run_etl.py
```
**What This Will Do**

- Read raw data from **data/raw/**

- Apply fail-fast data quality checks

- Clean and transform the data

- Safely join orders with users

- Write processed datasets to **data/processed/**

- Generate run metadata for reproducibility

## Incremental Scripts (Development History)

### The following scripts reflect the step-by-step development during Week 2:

**Day 1 – Load raw data**
```pash
python scripts/run_day1_load.py
```
**Day 2 – Cleaning and quality checks**
```pash
python scripts/run_day2_clean.py
```

**Day 3 – Build analytics table**
```pash
python scripts/run_day3_build_analytics.py
```

These scripts are kept for **learning and reference.**
They are **not required** once **run_etl.py** is available.

## Outputs

### After running the final ETL pipeline, the following artifacts will be created:
```pash 
data/processed/orders_clean.parquet
```
**Cleaned orders table (order-level only)**
```pash
data/processed/users.parquet
```
**Cleaned users table**
```pash
data/processed/analytics_table.parquet
```
**Final analytics table enriched with user and time features**
```pash
data/processed/_run_meta.json
```
**Run metadata including row counts and key data quality statistics**
```pash
reports/figures/*.png
```
**Exported figures generated during EDA**
----
### All outputs are idempotent and safe to regenerate by re-running the pipeline.

## EDA (Exploratory Data Analysis)

### Open the EDA notebook:
```pash
notebooks/eda.ipynb
```
**Run all cells to reproduce the analysis and figures.**

## **⚠️ Important**
The notebook reads **only** from processed data in **data/processed/** to ensure
reproducibility.

Data Quality & Reproducibility

The ETL pipeline includes:

Required column validation

Non-empty dataset checks

Unique key validation for users

Safe joins with cardinality enforcement

Missing value flags

Outlier detection and winsorization

Run metadata logging for auditing and debugging

Quick Reproduce Checklist

After cloning the repository:

pip install -r requirements.txt
pip install -e .
python scripts/run_etl.py


You should see:

Processed data in data/processed/

Figures in reports/figures/

_run_meta.json generated successfully

