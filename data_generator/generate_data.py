#!/usr/bin/env python3
"""
User Activity Data Generator
=============================
Generates synthetic user-activity CSV data with intentional data-quality
issues (nulls, duplicates, negative values, mixed timestamp formats) so
the ETL pipeline has realistic cleaning work to demonstrate.

Usage:
    python3 generate_data.py              # default 2 000 000 rows (~200 MB)
    python3 generate_data.py 5000000      # custom row count
"""

import csv
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── Configuration ────────────────────────────────────────────
NUM_ROWS       = 2_000_000   # default row count
NUM_USERS      = 50_000
NUM_CONTENT    = 500
DATE_RANGE_DAYS = 90

OUTPUT_DIR  = Path(__file__).resolve().parent.parent / "data" / "raw"
OUTPUT_FILE = OUTPUT_DIR / "user_activity.csv"

# ── Dimension values ─────────────────────────────────────────
EVENT_TYPES = [
    "play", "pause", "stop", "skip",
    "search", "like", "share", "comment",
    "login", "logout",
]
DEVICES = ["mobile", "desktop", "tablet", "smart_tv"]
REGIONS = ["NA", "EU", "APAC", "LATAM", "MEA"]

# Weighted event distribution (play is most common)
EVENT_WEIGHTS = [35, 10, 15, 8, 7, 8, 5, 4, 5, 3]

# ── Data-quality issue rates ─────────────────────────────────
NULL_WATCH_TIME   = 0.02    # 2 %
NULL_DEVICE       = 0.01    # 1 %
NULL_REGION       = 0.005   # 0.5 %
DUPLICATE_RATE    = 0.01    # 1 %
NEGATIVE_WT_RATE  = 0.005   # 0.5 %

# ── Mixed timestamp formats (for cleaning challenge) ─────────
TS_FORMATS  = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
               "%Y-%m-%dT%H:%M:%S", "%m-%d-%Y %H:%M:%S"]
TS_WEIGHTS  = [0.80, 0.10, 0.05, 0.05]


def _user_ids(n):
    return [f"user_{str(i).zfill(6)}" for i in range(1, n + 1)]


def _content_ids(n):
    return [f"content_{str(i).zfill(4)}" for i in range(1, n + 1)]


def _random_ts(start, days):
    dt = start + timedelta(seconds=random.random() * days * 86400)
    fmt = random.choices(TS_FORMATS, weights=TS_WEIGHTS, k=1)[0]
    return dt.strftime(fmt)


def _make_row(users, contents, start, days):
    uid   = random.choice(users)
    event = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    ts    = _random_ts(start, days)
    cid   = random.choice(contents)

    device = random.choice(DEVICES) if random.random() > NULL_DEVICE else ""
    region = random.choice(REGIONS) if random.random() > NULL_REGION else ""

    if random.random() < NULL_WATCH_TIME:
        wt = ""
    elif random.random() < NEGATIVE_WT_RATE:
        wt = str(round(-random.uniform(1, 100), 2))
    else:
        wt = str(round(min(random.expovariate(1 / 300), 7200), 2))

    return [uid, event, ts, device, region, wt, cid]


def main():
    num_rows = int(sys.argv[1]) if len(sys.argv) > 1 else NUM_ROWS
    print(f"🔧 Generating {num_rows:,} rows of user activity data …")
    print(f"   Output → {OUTPUT_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    users    = _user_ids(NUM_USERS)
    contents = _content_ids(NUM_CONTENT)
    start    = datetime.now() - timedelta(days=DATE_RANGE_DAYS)

    headers = ["user_id", "event_type", "timestamp",
               "device", "region", "watch_time", "content_id"]

    dup_pool = []
    total    = 0
    batch_sz = 100_000

    with open(OUTPUT_FILE, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)

        for b_start in range(0, num_rows, batch_sz):
            b_end = min(b_start + batch_sz, num_rows)
            batch = []

            for _ in range(b_start, b_end):
                row = _make_row(users, contents, start, DATE_RANGE_DAYS)
                batch.append(row)
                if random.random() < DUPLICATE_RATE:
                    dup_pool.append(row)

            # sprinkle duplicates
            n_dups = min(len(dup_pool), len(batch) // 100)
            if n_dups:
                batch.extend(random.sample(dup_pool, n_dups))

            writer.writerows(batch)
            total += len(batch)
            pct = min(b_end / num_rows * 100, 100)
            print(f"   Progress: {pct:5.1f}%  ({total:,} rows)", end="\r")

    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\n✅ Done — {total:,} rows written ({size_mb:.1f} MB)")
    print(f"   Quality issues injected:")
    print(f"     • ~{NULL_WATCH_TIME*100:.1f}% null watch_time")
    print(f"     • ~{NULL_DEVICE*100:.1f}% null device")
    print(f"     • ~{NULL_REGION*100:.0f}% null region")
    print(f"     • ~{DUPLICATE_RATE*100:.0f}% duplicates")
    print(f"     • ~{NEGATIVE_WT_RATE*100:.1f}% negative watch_time")
    print(f"     • Mixed timestamp formats")


if __name__ == "__main__":
    main()
