-- =============================================================
-- PostgreSQL Data Warehouse — Gold Layer Tables
-- Auto-executed on first container start via docker-entrypoint
-- =============================================================

-- Daily Active Users
CREATE TABLE IF NOT EXISTS gold_dau (
    activity_date   DATE NOT NULL,
    unique_users    BIGINT NOT NULL,
    total_events    BIGINT NOT NULL,
    PRIMARY KEY (activity_date)
);

-- Total watch time per content
CREATE TABLE IF NOT EXISTS gold_watch_time_per_content (
    content_id          VARCHAR(20) NOT NULL,
    total_watch_time    DOUBLE PRECISION NOT NULL,
    avg_watch_time      DOUBLE PRECISION NOT NULL,
    play_count          BIGINT NOT NULL,
    unique_viewers      BIGINT NOT NULL,
    PRIMARY KEY (content_id)
);

-- Region-wise engagement
CREATE TABLE IF NOT EXISTS gold_region_engagement (
    activity_date   DATE NOT NULL,
    region          VARCHAR(10) NOT NULL,
    unique_users    BIGINT NOT NULL,
    total_events    BIGINT NOT NULL,
    total_watch_time DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (activity_date, region)
);

-- Device usage distribution
CREATE TABLE IF NOT EXISTS gold_device_distribution (
    device          VARCHAR(20) NOT NULL,
    total_events    BIGINT NOT NULL,
    unique_users    BIGINT NOT NULL,
    total_watch_time DOUBLE PRECISION NOT NULL,
    avg_watch_time  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (device)
);

-- Pipeline run metadata (for tracking incremental loads)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    run_timestamp   TIMESTAMP DEFAULT NOW(),
    layer           VARCHAR(10) NOT NULL,
    rows_processed  BIGINT,
    status          VARCHAR(20) NOT NULL,
    duration_sec    DOUBLE PRECISION,
    notes           TEXT
);
