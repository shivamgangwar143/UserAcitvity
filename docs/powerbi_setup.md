# Connecting Power BI to the Pipeline Output

## Option 1: Connect to PostgreSQL (Recommended)

This gives Power BI live access to the gold-layer warehouse tables.

### Steps

1. Open Power BI Desktop
2. Click **Get Data** → **PostgreSQL database**
3. Enter connection details:
   - **Server**: `localhost`  (or your Docker host IP)
   - **Port**: `5432`
   - **Database**: `user_activity_dw`
4. Authentication:
   - **User**: `dataengineer`
   - **Password**: `pipeline123`
5. Select tables:
   - `gold_dau`
   - `gold_watch_time_per_content`
   - `gold_region_engagement`
   - `gold_device_distribution`
6. Click **Load**

### Suggested Visualizations

| Visual Type | Data Source | X Axis | Values |
|-------------|-----------|--------|--------|
| Line Chart | `gold_dau` | `activity_date` | `unique_users` |
| Bar Chart | `gold_watch_time_per_content` | `content_id` (Top 10) | `total_watch_time` |
| Stacked Area | `gold_region_engagement` | `activity_date` | `total_events` by `region` |
| Donut Chart | `gold_device_distribution` | `device` | `total_events` |

---

## Option 2: Connect to Parquet Files Directly

If you prefer to bypass PostgreSQL:

1. Open Power BI Desktop
2. Click **Get Data** → **Parquet**
3. Navigate to the gold-layer Parquet folders:
   - `data/lake/gold/dau/`
   - `data/lake/gold/watch_time_per_content/`
   - `data/lake/gold/region_engagement/`
   - `data/lake/gold/device_distribution/`
4. Power BI will auto-detect the schema from the Parquet metadata

> **Note**: Parquet files are inside the Docker volume. Copy them to a
> local folder first, or mount a shared volume visible to your host OS.

---

## Dashboard Layout Recommendation

```
┌─────────────────────────────────────────────────────────────┐
│                  USER ACTIVITY DASHBOARD                     │
├─────────────────────────────────┬───────────────────────────┤
│                                 │                           │
│    DAU Trend (Line Chart)       │  Device Usage (Donut)     │
│    X: date  Y: unique_users    │  Segment: device          │
│                                 │  Value: total_events      │
│                                 │                           │
├─────────────────────────────────┼───────────────────────────┤
│                                 │                           │
│  Top Content (Horizontal Bar)   │  Region Activity          │
│  Y: content_id (Top 10)        │  (Stacked Area)           │
│  X: total_watch_time           │  X: date                  │
│                                 │  Y: events by region      │
│                                 │                           │
└─────────────────────────────────┴───────────────────────────┘
```
