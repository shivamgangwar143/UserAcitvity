"""
Pipeline Configuration
======================
Central configuration for the User Activity ETL pipeline.
All paths and credentials are collected here so the pipeline
scripts stay free of magic strings.
"""

import os


class Config:
    """Immutable pipeline settings."""

    # ── Spark ────────────────────────────────────────────────
    APP_NAME = "UserActivityPipeline"

    # ── Data Lake paths (inside Docker container) ────────────
    RAW_DATA_PATH   = "/app/data/raw"
    BRONZE_PATH     = "/app/data/lake/bronze/user_activity"
    SILVER_PATH     = "/app/data/lake/silver/user_activity"
    GOLD_BASE_PATH  = "/app/data/lake/gold"
    CHECKPOINT_PATH = "/app/data/checkpoints"

    # Gold sub-paths
    GOLD_DAU_PATH             = f"{GOLD_BASE_PATH}/dau"
    GOLD_WATCHTIME_PATH       = f"{GOLD_BASE_PATH}/watch_time_per_content"
    GOLD_REGION_PATH          = f"{GOLD_BASE_PATH}/region_engagement"
    GOLD_DEVICE_PATH          = f"{GOLD_BASE_PATH}/device_distribution"

    # ── PostgreSQL ───────────────────────────────────────────
    PG_HOST     = os.getenv("PG_HOST", "activity-postgres")
    PG_PORT     = os.getenv("PG_PORT", "5432")
    PG_DATABASE = os.getenv("PG_DATABASE", "user_activity_dw")
    PG_USER     = os.getenv("PG_USER", "dataengineer")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "pipeline123")

    PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    PG_PROPERTIES = {
        "user":     PG_USER,
        "password": PG_PASSWORD,
        "driver":   "org.postgresql.Driver",
    }

    # ── Silver-layer partitioning ────────────────────────────
    SILVER_PARTITION_COLS = ["activity_date"]
