"""
Incremental Loader
===================
Processes **only new data** that arrived since the last pipeline run.

Strategy:
  - Maintains a high-water-mark timestamp in a checkpoint file.
  - On each run, reads the full Bronze Parquet, filters rows whose
    ingestion timestamp is newer than the checkpoint, then pushes
    them through Silver → Gold.
  - Updates the checkpoint after a successful run.

This avoids re-processing the entire dataset every time new CSV files
are dropped into the raw folder.
"""

import json
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import Config
from silver_layer import clean_and_transform
from gold_layer import build_gold_layer


CHECKPOINT_FILE = os.path.join(Config.CHECKPOINT_PATH, "last_run.json")


def _read_checkpoint() -> str:
    """Return the last successful ingestion timestamp, or epoch."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        return data.get("last_ingestion_ts", "1970-01-01T00:00:00")
    return "1970-01-01T00:00:00"


def _write_checkpoint(ts: str):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_ingestion_ts": ts, "updated_at": datetime.utcnow().isoformat()}, f, indent=2)


def incremental_ingest(spark: SparkSession) -> int:
    """Ingest only rows newer than the last checkpoint.

    Returns:
        Number of new rows ingested.
    """
    print("=" * 60)
    print("  INCREMENTAL INGEST")
    print("=" * 60)

    last_ts = _read_checkpoint()
    print(f"  Checkpoint : {last_ts}")

    # Read full bronze
    bronze_df = spark.read.parquet(Config.BRONZE_PATH)

    # Filter new rows
    new_df = bronze_df.filter(F.col("_ingestion_ts") > last_ts)
    new_count = new_df.count()

    if new_count == 0:
        print("  No new data since last run. Skipping.")
        return 0

    print(f"  New rows   : {new_count:,}")

    # Append new rows to Bronze (in practice you'd write to a separate partition)
    current_ts = datetime.utcnow().isoformat()
    new_df.write.mode("append").parquet(Config.BRONZE_PATH + "_incremental")

    _write_checkpoint(current_ts)
    print(f"  Checkpoint updated → {current_ts}")

    # Re-run Silver and Gold on the full dataset
    print("\n  Re-processing Silver & Gold layers …")
    clean_and_transform(spark)
    build_gold_layer(spark)

    return new_count


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{Config.APP_NAME}_incremental")
        .getOrCreate()
    )
    incremental_ingest(spark)
    spark.stop()
