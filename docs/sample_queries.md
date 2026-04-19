# Sample Queries

## PostgreSQL — Gold Layer Queries

These queries run against the PostgreSQL warehouse after the pipeline loads
the Gold-layer data.

### 1. Daily Active Users (Last 30 Days)

```sql
SELECT activity_date,
       unique_users,
       total_events,
       ROUND(total_events::numeric / unique_users, 2) AS events_per_user
FROM   gold_dau
WHERE  activity_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER  BY activity_date;
```

### 2. Top 10 Content by Total Watch Time

```sql
SELECT content_id,
       ROUND(total_watch_time::numeric / 3600, 2) AS watch_hours,
       play_count,
       unique_viewers,
       ROUND(avg_watch_time::numeric, 2)          AS avg_seconds
FROM   gold_watch_time_per_content
ORDER  BY total_watch_time DESC
LIMIT  10;
```

### 3. Region Engagement — Weekly Summary

```sql
SELECT region,
       DATE_TRUNC('week', activity_date)::date AS week_start,
       SUM(unique_users)                       AS weekly_users,
       SUM(total_events)                       AS weekly_events,
       ROUND(SUM(total_watch_time)::numeric / 3600, 2) AS watch_hours
FROM   gold_region_engagement
GROUP  BY region, week_start
ORDER  BY week_start, region;
```

### 4. Device Usage — Market Share

```sql
SELECT device,
       total_events,
       unique_users,
       ROUND(100.0 * total_events / SUM(total_events) OVER (), 2) AS event_pct,
       ROUND(avg_watch_time::numeric, 2) AS avg_watch_sec
FROM   gold_device_distribution
ORDER  BY total_events DESC;
```

### 5. Pipeline Run History

```sql
SELECT run_id,
       run_timestamp,
       layer,
       rows_processed,
       status,
       ROUND(duration_sec::numeric, 2) AS duration_sec
FROM   pipeline_runs
ORDER  BY run_timestamp DESC
LIMIT  20;
```

---

## PySpark — Ad-hoc Silver Layer Queries

These can be run inside a Jupyter notebook connected to the PySpark container.

### Peak Usage Hours

```python
from pyspark.sql import functions as F

silver = spark.read.parquet("/app/data/lake/silver/user_activity")

(silver
 .groupBy("hour")
 .agg(F.count("*").alias("events"),
      F.countDistinct("user_id").alias("users"))
 .orderBy("hour")
 .show(24))
```

### Weekend vs Weekday Engagement

```python
(silver
 .groupBy("is_weekend")
 .agg(F.count("*").alias("events"),
      F.avg("watch_time").alias("avg_watch"))
 .show())
```
