#!/bin/bash
# =============================================================
# Setup script — run INSIDE the pyspark-pipeline container
# Usage: docker exec pyspark-pipeline bash /app/scripts/setup.sh
# =============================================================

set -e

echo "============================================="
echo "  User Activity Pipeline — Environment Setup"
echo "============================================="

# 1. Install Python deps
echo "[1/3] Installing Python dependencies..."
pip install --quiet psycopg2-binary pandas pyarrow sqlalchemy

# 2. Download PostgreSQL JDBC driver for Spark
JDBC_JAR="postgresql-42.7.4.jar"
JDBC_URL="https://jdbc.postgresql.org/download/${JDBC_JAR}"

# Try common Spark jars directories
for SPARK_JARS_DIR in /spark/jars /usr/local/spark/jars /usr/lib/spark/jars; do
    if [ -d "$SPARK_JARS_DIR" ]; then
        if [ ! -f "$SPARK_JARS_DIR/$JDBC_JAR" ]; then
            echo "[2/3] Downloading PostgreSQL JDBC driver..."
            wget -q -P "$SPARK_JARS_DIR" "$JDBC_URL" 2>/dev/null || \
            curl -sL -o "$SPARK_JARS_DIR/$JDBC_JAR" "$JDBC_URL" 2>/dev/null || \
            echo "  ⚠ Could not download JDBC driver. Warehouse loading may not work."
        else
            echo "[2/3] JDBC driver already present."
        fi
        break
    fi
done

# 3. Create data lake directory structure
echo "[3/3] Creating data lake directory structure..."
mkdir -p /app/data/raw
mkdir -p /app/data/lake/bronze/user_activity
mkdir -p /app/data/lake/silver/user_activity
mkdir -p /app/data/lake/gold/dau
mkdir -p /app/data/lake/gold/watch_time_per_content
mkdir -p /app/data/lake/gold/region_engagement
mkdir -p /app/data/lake/gold/device_distribution
mkdir -p /app/data/checkpoints

echo ""
echo "✅ Setup complete! You can now run the pipeline:"
echo "   docker exec pyspark-pipeline spark-submit /app/src/pipeline.py"
