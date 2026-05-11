# Smart Facility Monitoring Mini Data Platform

## Overview
This repository implements a lightweight end-to-end data pipeline for facility monitoring data.
It includes local data generation, raw ingestion, transformation to cleaned/silver datasets, curated gold views, and a simple report.

## Architecture
- `src/generate_data.py`: generates realistic sensor and reference data for the last 7 days.
- `src/ingest.py`: stages source files into a raw bronze layer.
- `src/transform.py`: cleans data and produces silver and gold outputs.
- `src/report.py`: builds analytical summaries and charts.
- `sql/`: contains SQL logic for silver processing and gold views.
- `data/`: stores generated source data, raw ingestion files, silver outputs, gold views, and report samples.

## Tools and Stack
- Python 3.11+ (local execution)
- pandas, numpy, matplotlib, seaborn
- Local filesystem storage to simulate cloud layers

## Setup
1. Create a Python environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

## How to run
1. Generate source data:
   ```bash
   python src/generate_data.py
   ```
2. Ingest into raw layer:
   ```bash
   python src/ingest.py
   ```
3. Transform into silver/gold layers:
   ```bash
   python src/transform.py
   ```
4. Build the report:
   ```bash
   python src/report.py
   ```

## Tests
Run the unit tests with:
```bash
python -m pytest -q
```

## Data organization
- `data/source/`: generated source CSV files
- `data/raw/`: ingested bronze files
- `data/silver/`: cleaned and enriched datasets
- `data/gold/`: curated analytical outputs
- `screenshots/`: generated report charts

## Assumptions
See `docs/assumptions.md` for details on data quality, business logic, and transformation decisions.

## Notes
- This project is designed to run locally. Cloud components are represented by folder layers and SQL artifacts.
- `sql/silver_layer.sql` and `sql/gold_views.sql` contain transformation logic suitable for AWS Athena/Redshift.
