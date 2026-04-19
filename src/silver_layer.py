"""
Silver Layer — Data Cleaning & Transformation
===============================================
Reads from Bronze, applies data-quality rules, derives analytical
columns, and writes partitioned Parquet to the Silver layer.

Transformations applied:
  1. Drop exact-duplicate rows
  2. Parse mixed-format timestamps into a canonical TimestampType
  3. Fill null categoricals with 'unknown'
  4. Filter out invalid watch_time (negative values)
  5. Cast watch_time to DoubleType
  6. Derive: activity_date, hour, day_of_week, is_weekend
  7. Partition output by (activity_date, region)
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

from config import Config


# ── Timestamp parsing helper ──────────────────────────────────
# The generator produces four different formats; we try each in order.
_TS_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",      # 2025-01-15 14:30:00
    "dd/MM/yyyy HH:mm:ss",      # 15/01/2025 14:30:00
    "yyyy-MM-dd'T'HH:mm:ss",    # 2025-01-15T14:30:00
    "MM-dd-yyyy HH:mm:ss",      # 01-15-2025 14:30:00
]


def _coalesce_timestamps(df, col_name="timestamp"):
    """Try each format and keep the first non-null parse."""
    parsed_cols = [
        F.to_timestamp(F.col(col_name), fmt) for fmt in _TS_FORMATS
    ]
    return df.withColumn("parsed_ts", F.coalesce(*parsed_cols))


def clean_and_transform(spark: SparkSession) -> int:
    """Read Bronze → clean → transform → write Silver.

    Returns:
        Number of rows written.
    """
    print("=" * 60)
    print("  SILVER LAYER — Cleaning & Transformation")
    print("=" * 60)
    t0 = time.time()

    # ── 1. Read Bronze ───────────────────────────────────────
    bronze_df = spark.read.parquet(Config.BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"  Bronze rows     : {bronze_count:,}")

    # ── 2. Drop exact duplicates ─────────────────────────────
    deduped_df = bronze_df.dropDuplicates(
        ["user_id", "event_type", "timestamp", "device",
         "region", "watch_time", "content_id"]
    )
    dup_count = bronze_count - deduped_df.count()
    print(f"  Duplicates removed : {dup_count:,}")

    # ── 3. Parse timestamps ──────────────────────────────────
    ts_df = _coalesce_timestamps(deduped_df)
    # Drop rows where timestamp could not be parsed at all
    ts_df = ts_df.filter(F.col("parsed_ts").isNotNull())
    unparsed = deduped_df.count() - ts_df.count()
    if unparsed:
        print(f"  Unparseable timestamps dropped : {unparsed:,}")

    # ── 4. Handle nulls in categoricals ──────────────────────
    filled_df = (
        ts_df
        .withColumn("device",
                     F.when(F.col("device") == "", "unknown")
                      .when(F.col("device").isNull(), "unknown")
                      .otherwise(F.col("device")))
        .withColumn("region",
                     F.when(F.col("region") == "", "unknown")
                      .when(F.col("region").isNull(), "unknown")
                      .otherwise(F.col("region")))
    )

    # ── 5. Cast & filter watch_time ──────────────────────────
    casted_df = (
        filled_df
        .withColumn("watch_time",
                     F.when(F.col("watch_time") == "", None)
                      .otherwise(F.col("watch_time").cast(DoubleType())))
    )
    # Remove negative watch_time rows
    neg_count = casted_df.filter(F.col("watch_time") < 0).count()
    clean_df = casted_df.filter(
        (F.col("watch_time").isNull()) | (F.col("watch_time") >= 0)
    )
    # Fill remaining null watch_time with 0
    clean_df = clean_df.fillna({"watch_time": 0.0})
    print(f"  Negative watch_time removed : {neg_count:,}")

    # ── 6. Derive analytical columns ─────────────────────────
    silver_df = (
        clean_df
        .withColumn("activity_date", F.to_date(F.col("parsed_ts")))
        .withColumn("hour",          F.hour(F.col("parsed_ts")))
        .withColumn("day_of_week",   F.dayofweek(F.col("parsed_ts")))
        .withColumn("day_name",      F.date_format(F.col("parsed_ts"), "EEEE"))
        .withColumn("is_weekend",
                     F.when(F.dayofweek(F.col("parsed_ts")).isin(1, 7), True)
                      .otherwise(False))
        .withColumn("event_ts", F.col("parsed_ts").cast(TimestampType()))
        .drop("timestamp", "parsed_ts", "_ingestion_ts", "_source_file")
    )

    silver_count = silver_df.count()

    # ── 7. Write partitioned Parquet ─────────────────────────
    # Coalesce to reduce file count per partition (critical for
    # single-node Spark with limited memory)
    (
        silver_df
        .coalesce(4)
        .write
        .mode("overwrite")
        .partitionBy("activity_date")
        .parquet(Config.SILVER_PATH)
    )

    elapsed = time.time() - t0
    print(f"  Silver rows     : {silver_count:,}")
    print(f"  Partitioned by  : {Config.SILVER_PARTITION_COLS}")
    print(f"  Written to      : {Config.SILVER_PATH}")
    print(f"  Duration        : {elapsed:.1f}s")
    print()
    return silver_count


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{Config.APP_NAME}_silver")
        .getOrCreate()
    )
    clean_and_transform(spark)
    spark.stop()
