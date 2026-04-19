"""
Pipeline Orchestrator
======================
Runs the full ETL pipeline end-to-end:

    CSV  →  Bronze (raw Parquet)
         →  Silver (cleaned, partitioned Parquet)
         →  Gold   (aggregated business metrics)
         →  PostgreSQL warehouse

Usage (inside the pyspark-pipeline container):
    spark-submit --jars /spark/jars/postgresql-42.7.4.jar /app/src/pipeline.py
"""

import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession

# Add src to path so imports work with spark-submit
sys.path.insert(0, "/app/src")

from config import Config
from bronze_layer import ingest_to_bronze
from silver_layer import clean_and_transform
from gold_layer import build_gold_layer
from warehouse_loader import load_to_warehouse


def run_pipeline():
    """Execute the full Bronze → Silver → Gold → Warehouse pipeline."""
    print("\n" + "█" * 60)
    print("  USER ACTIVITY BATCH PIPELINE")
    print(f"  Started at : {datetime.utcnow().isoformat()}")
    print("█" * 60 + "\n")

    pipeline_start = time.time()

    # ── Spark Session ────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName(Config.APP_NAME)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars",
                "/spark/jars/postgresql-42.7.4.jar")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # ── Layer 1: Bronze ──────────────────────────────────
        bronze_rows = ingest_to_bronze(spark)

        # ── Layer 2: Silver ──────────────────────────────────
        silver_rows = clean_and_transform(spark)

        # ── Layer 3: Gold ────────────────────────────────────
        build_gold_layer(spark)

        # ── Layer 4: Warehouse ───────────────────────────────
        try:
            load_to_warehouse(spark)
        except Exception as e:
            print(f"  ⚠ Warehouse load failed (Postgres may not be ready): {e}")
            print("    Gold Parquet files are still available for Power BI.")

        # ── Summary ──────────────────────────────────────────
        total_time = time.time() - pipeline_start
        print("\n" + "█" * 60)
        print("  PIPELINE COMPLETE")
        print(f"  Bronze rows  : {bronze_rows:,}")
        print(f"  Silver rows  : {silver_rows:,}")
        print(f"  Total time   : {total_time:.1f}s")
        print(f"  Finished at  : {datetime.utcnow().isoformat()}")
        print("█" * 60 + "\n")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
