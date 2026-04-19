"""
Warehouse Loader
=================
Loads Gold-layer Parquet data into PostgreSQL so it can be queried
by Power BI, SQL tools, or any JDBC/ODBC client.

Uses Spark's built-in JDBC writer with the PostgreSQL driver.
"""

import time

from pyspark.sql import SparkSession

from config import Config


_TABLE_MAP = {
    Config.GOLD_DAU_PATH:       "gold_dau",
    Config.GOLD_WATCHTIME_PATH: "gold_watch_time_per_content",
    Config.GOLD_REGION_PATH:    "gold_region_engagement",
    Config.GOLD_DEVICE_PATH:    "gold_device_distribution",
}


def load_to_warehouse(spark: SparkSession):
    """Read each Gold Parquet dataset and write to Postgres."""
    print("=" * 60)
    print("  WAREHOUSE LOAD — Gold → PostgreSQL")
    print("=" * 60)
    t0 = time.time()

    for parquet_path, table_name in _TABLE_MAP.items():
        print(f"  Loading {table_name} …")
        df = spark.read.parquet(parquet_path)
        row_count = df.count()

        (
            df.write
            .mode("overwrite")
            .jdbc(
                url=Config.PG_JDBC_URL,
                table=table_name,
                properties=Config.PG_PROPERTIES,
            )
        )
        print(f"    → {row_count:,} rows  →  {table_name}")

    elapsed = time.time() - t0
    print(f"\n  Warehouse load duration : {elapsed:.1f}s")
    print()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{Config.APP_NAME}_warehouse")
        .config("spark.jars",
                "/spark/jars/postgresql-42.7.4.jar")
        .getOrCreate()
    )
    load_to_warehouse(spark)
    spark.stop()
