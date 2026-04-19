# Architecture — User Activity Batch Pipeline

## High-Level Architecture

```
┌──────────────┐     ┌─────────────────────────────────────────────────────────┐
│  CSV Source   │     │              PySpark ETL Engine (Docker)                │
│  (Raw Data)   │────▶│                                                         │
└──────────────┘     │  ┌───────────┐   ┌───────────┐   ┌───────────────────┐  │
                     │  │  BRONZE   │──▶│  SILVER   │──▶│       GOLD        │  │
                     │  │ Raw       │   │ Cleaned   │   │ DAU, Watch-time,  │  │
                     │  │ Parquet   │   │ Partitioned│  │ Region, Device    │  │
                     │  └───────────┘   │ Parquet   │   └────────┬──────────┘  │
                     │                  └───────────┘            │             │
                     └───────────────────────────────────────────┼─────────────┘
                                                                 │
                                      ┌──────────────────────────┼──────────┐
                                      │                          ▼          │
                                      │  ┌──────────────┐  ┌──────────┐    │
                                      │  │  PostgreSQL   │  │ Parquet  │    │
                                      │  │  Warehouse    │  │ Files    │    │
                                      │  └──────┬───────┘  └────┬─────┘    │
                                      │         │               │          │
                                      │         ▼               ▼          │
                                      │     ┌────────────────────────┐     │
                                      │     │      Power BI          │     │
                                      │     │  (Dashboard/Reports)   │     │
                                      │     └────────────────────────┘     │
                                      └────────────────────────────────────┘
```

## Data Flow

| Step | Component | Input | Output | Format |
|------|-----------|-------|--------|--------|
| 1 | Data Generator | Parameters | `user_activity.csv` | CSV |
| 2 | Bronze Layer | CSV file | Raw archive | Parquet |
| 3 | Silver Layer | Bronze Parquet | Cleaned, enriched data | Partitioned Parquet |
| 4 | Gold Layer | Silver Parquet | Business metrics (4 datasets) | Parquet |
| 5 | Warehouse Loader | Gold Parquet | Queryable tables | PostgreSQL |
| 6 | Power BI | Postgres / Parquet | Visual dashboards | PBIX |

## Medallion Architecture Layers

### Bronze (Raw)
- **Purpose**: Immutable archive of source data
- **Transformations**: None — only format conversion (CSV → Parquet) and metadata
- **Metadata added**: `_ingestion_ts`, `_source_file`
- **Storage**: `/data/lake/bronze/user_activity/`

### Silver (Cleaned)
- **Purpose**: Single source of truth — clean, validated, enriched
- **Transformations**:
  - Deduplicate rows
  - Parse mixed timestamp formats → canonical `TimestampType`
  - Fill null categoricals (`device`, `region`) with `'unknown'`
  - Remove negative `watch_time` values
  - Derive: `activity_date`, `hour`, `day_of_week`, `day_name`, `is_weekend`
- **Partitioning**: By `activity_date` and `region`
- **Storage**: `/data/lake/silver/user_activity/`

### Gold (Aggregated)
- **Purpose**: Pre-computed business metrics ready for BI consumption
- **Datasets**:
  1. `dau` — Daily Active Users + event counts
  2. `watch_time_per_content` — Total/avg watch-time, play count, unique viewers
  3. `region_engagement` — Daily engagement per region
  4. `device_distribution` — Usage metrics per device type
- **Storage**: `/data/lake/gold/{metric_name}/`

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Processing | Apache Spark (PySpark) | Distributed data processing |
| Storage | Parquet (Snappy compression) | Columnar storage |
| Warehouse | PostgreSQL 17 | Queryable analytical store |
| Containerization | Docker Compose | Reproducible environment |
| Visualization | Power BI | Business dashboards |

## Performance Optimizations

1. **Columnar Storage (Parquet)**: 60-80% compression vs CSV; predicate pushdown for selective reads
2. **Snappy Compression**: Fast encode/decode with good compression ratio
3. **Partition Pruning**: Date + region partitioning eliminates full-table scans
4. **Incremental Loading**: Checkpoint-based processing skips already-processed data
5. **Shuffle Tuning**: `spark.sql.shuffle.partitions` tuned for dataset size
