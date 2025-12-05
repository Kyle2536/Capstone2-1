# tracing.py
import uuid
from datetime import datetime, timezone

def now_mysql_ms() -> str:
    """UTC timestamp formatted for MySQL DATETIME(3) precision"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def new_trace() -> dict:
    """
    Create a unique trace_id and timestamp for one record.
    Included in every row from generator → producer → consumer → DB.
    """
    return {
        "trace_id": str(uuid.uuid4()),
        "sensor_created_at": now_mysql_ms()
    }
