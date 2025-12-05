# db_writer.py
# Writes business rows + latency stamps for the rush-hour Kafka pipeline.
# Robustly maps record_id via trace_id (no reliance on AUTO_INCREMENT math).

import os
from typing import List, Dict, Tuple
import pymysql
from pymysql.cursors import DictCursor
from dotenv import load_dotenv
import certifi      # ← REQUIRED for Azure SSL

load_dotenv()

# =========================
# DB configuration (env)
# =========================
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "capstone")

# Azure MySQL *requires* SSL
DB_SSL = True
DB_SSL_CA = certifi.where()

BUSINESS_TABLE = "kafka_pipeline_rush"
LAT_TABLE = "kafka_latencies_rush"


# =========================
# DDL (tables + indexes)
# =========================
CREATE_BUSINESS_SQL = f"""
CREATE TABLE IF NOT EXISTS {BUSINESS_TABLE} (
  record_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id VARCHAR(32) NOT NULL,
  trace_id CHAR(36) NOT NULL,
  created_at DATE NOT NULL,
  ts TIME(3) NOT NULL,
  peakspeed DOUBLE NOT NULL,
  pmgid VARCHAR(32) NOT NULL,
  direction INT NOT NULL,
  location VARCHAR(64) NOT NULL,
  vehiclecount INT NOT NULL,
  UNIQUE KEY uq_{BUSINESS_TABLE}_trace (trace_id)
) ENGINE=InnoDB;
"""

CREATE_LATENCY_SQL = f"""
CREATE TABLE IF NOT EXISTS {LAT_TABLE} (
  trace_id CHAR(36) PRIMARY KEY,
  run_id VARCHAR(32) NOT NULL,
  record_id BIGINT NOT NULL,
  sensor_created_at DATETIME(3) NOT NULL,
  ingest_received_at DATETIME(3) NOT NULL,
  sql_written_at DATETIME(3) NOT NULL,
  dashboard_emitted_at DATETIME(3) DEFAULT NULL,
  dashboard_rendered_at DATETIME(3) DEFAULT NULL,
  CONSTRAINT fk_rush_record
    FOREIGN KEY (record_id) REFERENCES {BUSINESS_TABLE}(record_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;
"""

CREATE_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS idx_{BUSINESS_TABLE}_pmgid ON {BUSINESS_TABLE}(pmgid);",
    f"CREATE INDEX IF NOT EXISTS idx_{BUSINESS_TABLE}_time  ON {BUSINESS_TABLE}(created_at, ts);",
    f"CREATE INDEX IF NOT EXISTS idx_{LAT_TABLE}_run       ON {LAT_TABLE}(run_id);",
    f"CREATE INDEX IF NOT EXISTS idx_{LAT_TABLE}_record    ON {LAT_TABLE}(record_id);",
]

# =========================
# Connection helper
# =========================
def get_conn():
    ssl_params = {"ca": DB_SSL_CA} if DB_SSL else None

    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=False,
        cursorclass=DictCursor,
        charset="utf8mb4",
        ssl=ssl_params,        # <--- CRITICAL FIX FOR AZURE
    )

# =========================
# Ensure schema (idempotent)
# =========================
def ensure_tables():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_BUSINESS_SQL)
            cur.execute(CREATE_LATENCY_SQL)
            for stmt in CREATE_INDEXES:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
        conn.commit()

# =========================
# Write helpers
# =========================
INSERT_BUSINESS = f"""
INSERT INTO {BUSINESS_TABLE}
(run_id, trace_id, created_at, ts, peakspeed, pmgid, direction, location, vehiclecount)
VALUES (%(run_id)s, %(trace_id)s, %(created_at)s, %(ts)s, %(peakspeed)s, %(pmgid)s, %(direction)s, %(location)s, %(vehiclecount)s)
"""

INSERT_LATENCY = f"""
INSERT INTO {LAT_TABLE}
(trace_id, run_id, record_id, sensor_created_at, ingest_received_at, sql_written_at)
VALUES (%s, %s, %s, %s, %s, NOW(3))
"""

SELECT_RECORD_IDS_BY_TRACE = f"""
SELECT trace_id, record_id
FROM {BUSINESS_TABLE}
WHERE trace_id IN ({'{placeholders}'})
"""

def insert_business_batch(conn, business_rows: List[Dict]):
    if not business_rows:
        return
    with conn.cursor() as cur:
        cur.executemany(INSERT_BUSINESS, business_rows)

def fetch_record_ids_by_trace(conn, trace_ids: List[str]) -> Dict[str, int]:
    if not trace_ids:
        return {}
    placeholders = ",".join(["%s"] * len(trace_ids))
    sql = SELECT_RECORD_IDS_BY_TRACE.format(placeholders=placeholders)
    with conn.cursor() as cur:
        cur.execute(sql, trace_ids)
        return {r["trace_id"]: r["record_id"] for r in cur.fetchall()}

def insert_latency_batch(conn, run_id: str, ingest_ts_utc: str, rows: List[Dict], trace_to_id: Dict[str, int]):
    vals = []
    for row in rows:
        rec_id = trace_to_id.get(row["trace_id"])
        if rec_id is None:
            raise RuntimeError(f"Missing record_id for trace_id {row['trace_id']}")
        vals.append(
            (row["trace_id"], run_id, rec_id, row["sensor_created_at"], ingest_ts_utc)
        )

    if vals:
        with conn.cursor() as cur:
            cur.executemany(INSERT_LATENCY, vals)

# =========================
# Write batch
# =========================
def write_batch(run_id: str, ingest_ts_utc: str, rows: List[Dict]) -> Tuple[int, int]:
    if not rows:
        return (0, 0)

    ensure_tables()

    with get_conn() as conn:
        insert_business_batch(conn, rows)

        trace_ids = [r["trace_id"] for r in rows]
        trace_to_id = fetch_record_ids_by_trace(conn, trace_ids)

        insert_latency_batch(conn, run_id, ingest_ts_utc, rows, trace_to_id)

        conn.commit()

        rec_ids = list(trace_to_id.values())
        return (min(rec_ids) if rec_ids else 0, len(rec_ids))
