"""
Gold Layer — Business Metric Aggregation
==========================================
Reads from the Silver layer and computes four key business metrics,
each written to its own Parquet dataset under the Gold path.

Metrics
-------
1. DAU (Daily Active Users)  — unique users + total events per day
2. Watch-time per content    — total & avg watch-time, play count, viewers
3. Region engagement         — events, users, watch-time per region per day
4. Device distribution       — events, users, watch-time per device type
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import Config


def _read_silver(spark: SparkSession):
    return spark.read.parquet(Config.SILVER_PATH)


# ── 1. Daily Active Users ────────────────────────────────────
def compute_dau(spark: SparkSession):
    print("  [gold] Computing DAU …")
    df = _read_silver(spark)

    dau_df = (
        df.groupBy("activity_date")
        .agg(
            F.countDistinct("user_id").alias("unique_users"),
            F.count("*").alias("total_events"),
        )
        .orderBy("activity_date")
    )

    dau_df.write.mode("overwrite").parquet(Config.GOLD_DAU_PATH)
    print(f"    → {dau_df.count()} date rows  →  {Config.GOLD_DAU_PATH}")
    return dau_df


# ── 2. Watch-time per content ────────────────────────────────
def compute_watchtime_per_content(spark: SparkSession):
    print("  [gold] Computing watch-time per content …")
    df = _read_silver(spark)

    wt_df = (
        df.filter(F.col("event_type") == "play")
        .groupBy("content_id")
        .agg(
            F.sum("watch_time").alias("total_watch_time"),
            F.avg("watch_time").alias("avg_watch_time"),
            F.count("*").alias("play_count"),
            F.countDistinct("user_id").alias("unique_viewers"),
        )
        .orderBy(F.desc("total_watch_time"))
    )

    wt_df.write.mode("overwrite").parquet(Config.GOLD_WATCHTIME_PATH)
    print(f"    → {wt_df.count()} content rows  →  {Config.GOLD_WATCHTIME_PATH}")
    return wt_df


# ── 3. Region-wise engagement ────────────────────────────────
def compute_region_engagement(spark: SparkSession):
    print("  [gold] Computing region engagement …")
    df = _read_silver(spark)

    reg_df = (
        df.groupBy("activity_date", "region")
        .agg(
            F.countDistinct("user_id").alias("unique_users"),
            F.count("*").alias("total_events"),
            F.sum("watch_time").alias("total_watch_time"),
        )
        .orderBy("activity_date", "region")
    )

    reg_df.write.mode("overwrite").parquet(Config.GOLD_REGION_PATH)
    print(f"    → {reg_df.count()} rows  →  {Config.GOLD_REGION_PATH}")
    return reg_df


# ── 4. Device usage distribution ─────────────────────────────
def compute_device_distribution(spark: SparkSession):
    print("  [gold] Computing device distribution …")
    df = _read_silver(spark)

    dev_df = (
        df.groupBy("device")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.sum("watch_time").alias("total_watch_time"),
            F.avg("watch_time").alias("avg_watch_time"),
        )
        .orderBy(F.desc("total_events"))
    )

    dev_df.write.mode("overwrite").parquet(Config.GOLD_DEVICE_PATH)
    print(f"    → {dev_df.count()} device rows  →  {Config.GOLD_DEVICE_PATH}")
    return dev_df


def build_gold_layer(spark: SparkSession):
    """Run all gold-layer aggregations."""
    print("=" * 60)
    print("  GOLD LAYER — Business Metrics")
    print("=" * 60)
    t0 = time.time()

    compute_dau(spark)
    compute_watchtime_per_content(spark)
    compute_region_engagement(spark)
    compute_device_distribution(spark)

    elapsed = time.time() - t0
    print(f"\n  Gold layer duration : {elapsed:.1f}s")
    print()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{Config.APP_NAME}_gold")
        .getOrCreate()
    )
    build_gold_layer(spark)
    spark.stop()
