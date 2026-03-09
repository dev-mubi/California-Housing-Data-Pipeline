# California Housing Analysis
### Medallion Architecture Data Pipeline · Databricks + PySpark + Tableau

---

## Overview

An end-to-end data engineering pipeline built on Databricks that processes the California Housing dataset through a three-layer Medallion Architecture (Bronze → Silver → Gold) and delivers analytical insights via a Tableau dashboard.

The pipeline is designed for incremental ingestion — raw CSV files land in a Databricks Volume and are processed on each manual trigger. A watermark mechanism ensures records are never reprocessed and Silver state remains consistent across runs.

| | |
|---|---|
| **Dataset** | California Housing — 20,640 records, 10 features |
| **Platform** | Databricks (Apache Spark / PySpark) |
| **Storage Format** | Parquet at every layer |
| **Architecture** | Medallion: Bronze → Silver → Gold |
| **Visualization** | Tableau Desktop |
| **Trigger** | Manual |

---

## Architecture

```
Raw Volume (CSV)
      │
      ▼
   Bronze ── raw ingestion, arrival_ts appended, no transforms
      │
      ▼
   Silver ── watermark filter → schema enforcement → dedup → upsert
      │
      ▼
    Gold  ── 5 aggregation tables, recomputed each run
      │
      ▼
  Tableau ── manual CSV export → dashboard
```

---

## Bronze Layer

Ingests raw CSV files from the volume into Parquet with zero transformations. Appends `arrival_ts` (ingestion timestamp) to every record — this timestamp drives all downstream incremental logic.

- Write mode: **append**
- Guard: skips ingestion if no raw files are found

---

## Silver Layer

Applies the incremental filter, enforces schema, removes duplicates, and upserts into the Silver Parquet table.

- **Watermark filter** — only records with `arrival_ts > last_watermark` are processed
- **Schema enforcement** — `try_cast()` on all columns; malformed values become null rather than failing the run
- **Deduplication** — `ROW_NUMBER()` window partitioned by `id`, ordered by `arrival_ts DESC`; latest record per ID is kept
- **Upsert logic** — first run overwrites; subsequent runs union existing Silver with the new batch, deduplicate, then overwrite

### Schema

| Column | Type | Notes |
|---|---|---|
| `id` | INT | Primary key; rows with null id are dropped |
| `longitude` | DOUBLE | |
| `latitude` | DOUBLE | |
| `housing_median_age` | INT | |
| `total_rooms` | INT | |
| `total_bedrooms` | INT | |
| `population` | INT | |
| `households` | INT | |
| `median_income` | DOUBLE | Scaled, not raw dollar value |
| `median_house_value` | INT | Target variable |
| `ocean_proximity` | STRING | INLAND / NEAR BAY / NEAR OCEAN / <1H OCEAN / ISLAND |
| `arrival_ts` | TIMESTAMP | Appended at Bronze ingestion |

---

## Gold Layer

Reads from Silver and produces five aggregation tables, each purpose-built for a specific dashboard view. Gold is always fully recomputed from the current Silver state and triggered automatically at the end of the Silver notebook via `dbutils.notebook.run()`.

- Write mode: **overwrite** per table

| Table | Description |
|---|---|
| `house_price_by_ocean` | Average price, average income, and count grouped by ocean proximity |
| `income_vs_house_price` | Average price and count by income bucket (0–2, 2–4, 4–6, 6–8, 8+) |
| `population_analysis` | Average price and count by population density bucket |
| `house_age_analysis` | Average price and count by housing age bucket |
| `geo_price_distribution` | Latitude, longitude, house value, population — for geographic map |

---

## Incremental Logic

1. **First run** — no watermark exists; all Bronze records are processed into Silver. `max(arrival_ts)` is persisted to `/metadata`.
2. **Subsequent runs** — Bronze is filtered to `arrival_ts > last_watermark`. Silver is updated via union + deduplication. Watermark is advanced.
3. **No new data** — filter returns zero rows; Silver and watermark are unchanged.
4. **Gold** — fully recomputed from Silver on every run regardless of whether new data arrived.

---

## Orchestration

The Silver notebook is the single entry point for the full pipeline.

```
Trigger: Raw_to_Silver_Pipeline
    ├── Bronze ingestion
    ├── Silver transforms + upsert
    ├── Watermark update
    └── dbutils.notebook.run(gold_notebook) ← triggers Gold automatically
```

After Gold completes, Parquet outputs are downloaded manually, converted to CSV, and connected to Tableau Desktop.

---

## Dashboard

Built in Tableau Desktop from Gold layer CSV exports. Covers six analytical views:

- Average house price by ocean proximity
- Income vs house price scatter (by proximity category)
- Average house price by income level
- Housing supply distribution by income level (bubble chart)
- Average house price by population density
- Geographic distribution of house prices (map layer)

---

## Intellectual Property

© 2025 Mubashir Shahzaib. All Rights Reserved.

All pipeline source code, architectural design, and dashboard layout in this repository are the original work of the author. Reproduction or redistribution without explicit written permission is not permitted.

The California Housing dataset is sourced from Kaggle and is not owned by the author. Apache Spark, PySpark, Databricks, and Tableau are trademarks of their respective owners.
