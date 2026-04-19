"""
Bronze Layer — Raw Ingestion
=============================
Reads the raw CSV and persists it as Parquet **without** any cleaning.
Adds ingestion metadata columns so we can track data lineage.

Why a Bronze layer?
  - Immutable archive of raw data exactly as received
  - Columnar Parquet is cheaper to store and faster to scan than CSV
  - Ingestion metadata enables auditability
"""

import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import Config


def ingest_to_bronze(spark: SparkSession) -> int:
    """Read raw CSV → write Parquet to the Bronze layer.

    Returns:
        Number of rows written.
    """
    print("=" * 60)
    print("  BRONZE LAYER — Raw Ingestion")
    print("=" * 60)
    t0 = time.time()

    # ── 1. Read raw CSV ──────────────────────────────────────
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # keep everything as string
        .csv(f"{Config.RAW_DATA_PATH}/user_activity.csv")
    )

    raw_count = raw_df.count()
    print(f"  Raw CSV rows : {raw_count:,}")

    # ── 2. Add ingestion metadata ────────────────────────────
    bronze_df = (
        raw_df
        .withColumn("_ingestion_ts", F.lit(datetime.utcnow().isoformat()))
        .withColumn("_source_file", F.input_file_name())
    )

    # ── 3. Write as Parquet (overwrite for full-refresh) ─────
    (
        bronze_df.write
        .mode("overwrite")
        .parquet(Config.BRONZE_PATH)
    )

    elapsed = time.time() - t0
    print(f"  Written to   : {Config.BRONZE_PATH}")
    print(f"  Duration     : {elapsed:.1f}s")
    print()
    return raw_count


# Allow standalone execution for testing
if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{Config.APP_NAME}_bronze")
        .getOrCreate()
    )
    ingest_to_bronze(spark)
    spark.stop()
