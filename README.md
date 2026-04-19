# User Activity Batch Data Pipeline

A production-style batch ETL pipeline that processes user activity data through the **Medallion Architecture** (Bronze → Silver → Gold), with PySpark as the processing engine and PostgreSQL as the data warehouse.

Built to demonstrate real-world data engineering concepts: data cleaning, transformation, partitioning, incremental loading, and analytics enablement.

---

## Architecture

```
CSV  ──▶  Bronze (Raw Parquet)  ──▶  Silver (Clean, Partitioned)  ──▶  Gold (Metrics)  ──▶  PostgreSQL  ──▶  Power BI
```

See [docs/architecture.md](docs/architecture.md) for the full architecture diagram.

---

## Project Structure

```
UserAcitvity/
├── docker-compose.yml          # PySpark + PostgreSQL services
├── requirements.txt            # Python dependencies
├── scripts/
│   └── setup.sh                # Container environment setup
├── data_generator/
│   └── generate_data.py        # Synthetic data generator (stdlib only)
├── src/
│   ├── config.py               # Centralized configuration
│   ├── pipeline.py             # Main orchestrator
│   ├── bronze_layer.py         # Raw CSV → Parquet
│   ├── silver_layer.py         # Cleaning & transformation
│   ├── gold_layer.py           # Business metric aggregation
│   ├── warehouse_loader.py     # Gold → PostgreSQL
│   └── incremental_loader.py   # Checkpoint-based incremental ETL
├── sql/
│   └── init.sql                # Warehouse DDL
├── docs/
│   ├── architecture.md         # Architecture & data flow
│   ├── sample_queries.md       # SQL & PySpark query examples
│   └── powerbi_setup.md        # Power BI connection guide
├── data/                       # Generated at runtime
│   ├── raw/                    # Source CSV files
│   └── lake/                   # Bronze / Silver / Gold layers
└── .gitignore
```

---

## Quick Start

### 1. Generate Synthetic Data

Run on your local machine (uses only Python stdlib — no pip install needed):

```bash
cd UserAcitvity
python3 data_generator/generate_data.py          # 2M rows (~200 MB)
python3 data_generator/generate_data.py 5000000  # or 5M rows (~500 MB)
```

### 2. Start Docker Services

```bash
docker-compose up -d
```

This starts:
- **pyspark-pipeline** — PySpark + Jupyter Lab on port `8888`
- **activity-postgres** — PostgreSQL 17 on port `5432`

### 3. Setup Container Environment

```bash
docker exec pyspark-pipeline bash /app/scripts/setup.sh
```

Installs Python packages and downloads the PostgreSQL JDBC driver.

### 4. Run the Full Pipeline

```bash
docker exec pyspark-pipeline spark-submit \
  --jars /opt/spark/jars/postgresql-42.7.4.jar \
  /app/src/pipeline.py
```

This executes: **Bronze → Silver → Gold → PostgreSQL**

### 5. (Optional) Run Incremental Load

After adding new CSV data to `data/raw/`:

```bash
docker exec pyspark-pipeline spark-submit /app/src/incremental_loader.py
```

---

## Data Quality Issues (Intentionally Injected)

The data generator creates realistic dirty data for the pipeline to clean:

| Issue | Rate | Pipeline Action |
|-------|------|-----------------|
| Null `watch_time` | ~2% | Fill with 0.0 |
| Null `device` | ~1% | Fill with `'unknown'` |
| Null `region` | ~0.5% | Fill with `'unknown'` |
| Duplicate rows | ~1% | Deduplicate on all columns |
| Negative `watch_time` | ~0.5% | Remove row |
| Mixed timestamp formats | 4 formats | Parse with `coalesce()` |

---

## Gold Layer Metrics

| Metric | Table | Key Columns |
|--------|-------|-------------|
| Daily Active Users | `gold_dau` | `activity_date`, `unique_users`, `total_events` |
| Watch Time per Content | `gold_watch_time_per_content` | `content_id`, `total_watch_time`, `unique_viewers` |
| Region Engagement | `gold_region_engagement` | `activity_date`, `region`, `unique_users` |
| Device Distribution | `gold_device_distribution` | `device`, `total_events`, `unique_users` |

---

## Performance Optimizations

1. **Parquet + Snappy** — 60-80% smaller than CSV; columnar access
2. **Partition Pruning** — Silver partitioned by `(date, region)` eliminates full scans
3. **Incremental Loading** — Checkpoint-based; processes only new data
4. **Shuffle Tuning** — `spark.sql.shuffle.partitions` set to match data scale

---

## Power BI Dashboard

Connect Power BI to the PostgreSQL warehouse or directly to Gold Parquet files.
See [docs/powerbi_setup.md](docs/powerbi_setup.md) for step-by-step instructions.

Suggested dashboard panels:
- **DAU Trend** — line chart of daily unique users
- **Top Content** — bar chart of watch time by content
- **Region Activity** — stacked area chart by region
- **Device Breakdown** — donut chart of device distribution

---

## Sample Queries

See [docs/sample_queries.md](docs/sample_queries.md) for ready-to-run SQL and PySpark queries.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Processing | Apache Spark (PySpark) |
| Storage | Parquet (Snappy) |
| Warehouse | PostgreSQL 17 |
| Containers | Docker Compose |
| Visualization | Power BI |
| Language | Python 3 |
