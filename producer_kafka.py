# --- robust shim for environments where kafka.vendor.six is missing ---
import sys
try:
    import six  # provided by requirements.txt
    sys.modules['kafka.vendor.six'] = six
    sys.modules['kafka.vendor.six.moves'] = six.moves
except Exception:
    pass
# ----------------------------------------------------------------------

import os
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# ⭐ UPDATED: import from src.generator_fixed
from generator_fixed import (
    generate_records,
    generate_burst_for_sensor,
    daypart_multipliers,
    now_local_chicago,
)

from dotenv import load_dotenv
load_dotenv()

# ----------------- Env -----------------
BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC       = os.getenv("KAFKA_TOPIC", "traffic.raw.v2")
NUM_RECORDS = int(os.getenv("NUM_RECORDS", "1000"))
LINGER_MS   = int(os.getenv("KAFKA_LINGER_MS", "10"))
COMPRESSION = os.getenv("KAFKA_COMPRESSION", "lz4")
ACKS        = os.getenv("KAFKA_ACKS", "all")

RUN_FOREVER = os.getenv("RUN_FOREVER", "0") == "1"
BASE_SLEEP  = float(os.getenv("BASE_SLEEP", "0.20"))
BURST_MIN   = int(os.getenv("BURST_MIN", "3"))
BURST_MAX   = int(os.getenv("BURST_MAX", "5"))

RUN_ID = os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S")


def _to_db_schema(rec: dict) -> dict:
    """
    Generator yields:
      created_at, timestamp, peakspeed, pmgid, direction, location, vehiclecount
      + trace_id, sensor_created_at
    DB wants: ts, run_id
    """
    out = rec.copy()
    out["ts"] = out.pop("timestamp")
    out["run_id"] = RUN_ID
    return out


def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks=ACKS,
        compression_type=COMPRESSION,
        linger_ms=LINGER_MS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if isinstance(k, str) else k),
        retries=5,
        max_in_flight_requests_per_connection=5,
        api_version_auto_timeout_ms=60000,
    )


def run_finite():
    """Send exactly NUM_RECORDS using generator's finite stream."""
    producer = _make_producer()
    sent = 0
    try:
        for rec in generate_records(NUM_RECORDS):
            payload = _to_db_schema(rec)
            key = payload["pmgid"]
            producer.send(TOPIC, key=key, value=payload)
            sent += 1

        producer.flush()
        print(f"[producer] Sent {sent} records to '{TOPIC}' with RUN_ID={RUN_ID}")

    finally:
        try: producer.flush()
        except: pass
        try: producer.close()
        except: pass


def run_forever():
    """
    Stream bursts indefinitely.
    Sleep time varies by daypart:
      rush  -> faster
      late  -> slower
    """
    producer = _make_producer()
    sent = 0
    pmg_index = 0

    def next_pmgid():
        nonlocal pmg_index
        pmg_index += 1
        return f"PMG{pmg_index:05d}"

    print(f"[producer] RUN_FOREVER=1 topic='{TOPIC}' RUN_ID={RUN_ID} BASE_SLEEP={BASE_SLEEP}s")

    try:
        while True:
            pmgid = next_pmgid()
            base_cars = max(BURST_MIN, min(BURST_MAX, BURST_MIN + (pmg_index % (BURST_MAX - BURST_MIN + 1))))

            now_utc = datetime.now(timezone.utc)
            burst = generate_burst_for_sensor(pmgid, now_utc, cars_in_burst=base_cars)

            for rec in burst:
                payload = _to_db_schema(rec)
                key = payload["pmgid"]
                producer.send(TOPIC, key=key, value=payload)
                sent += 1

            producer.flush()

            local_now = now_local_chicago()
            _, volume_mult = daypart_multipliers(local_now)
            pause = BASE_SLEEP / max(0.1, volume_mult)

            time.sleep(pause)

    except KeyboardInterrupt:
        print("\n[producer] Stopped by user (Ctrl+C).")

    finally:
        try: producer.flush()
        except: pass
        try: producer.close()
        except: pass
        print(f"[producer] Total sent this session: {sent} records RUN_ID={RUN_ID}")


def main():
    if RUN_FOREVER:
        run_forever()
    else:
        run_finite()


if __name__ == "__main__":
    main()
