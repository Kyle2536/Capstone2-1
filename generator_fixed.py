# generator_fixed.py
# - Fixed lon/lat per PMGID (no per-row drift)
# - Rush/Late/Normal daypart logic for speed and volume
# - Compatible with existing producer/consumer pipeline (trace_id, sensor_created_at)

import random, hashlib
from datetime import datetime, timezone, timedelta

# ⭐ UPDATED: use src.tracing instead of local import
from tracing import new_trace


# ---- realistic sensor placement: Poisson-disk in the Dallas box ----
import math
from functools import lru_cache

# Dallas-ish bounding box
LON_MIN, LON_MAX = -97.05, -96.55
LAT_MIN, LAT_MAX =  32.60,  33.10
BOX_COS = math.cos(math.radians(33.0))

def km_to_deg_lat(km: float) -> float:
    return km / 111.0

def km_to_deg_lon(km: float) -> float:
    return km / (111.0 * BOX_COS)

def _poisson_disk_points(n_target: int, min_sep_km: float, seed: int = 4372):
    """
    Bridson-ish Poisson-disk sampler (simple version) in lat/lon degrees.
    Deterministic given (n_target, min_sep_km, seed).
    """
    rnd = random.Random(seed)
    min_sep_lat = km_to_deg_lat(min_sep_km)
    min_sep_lon = km_to_deg_lon(min_sep_km)

    cell_lat = min_sep_lat / math.sqrt(2)
    cell_lon = min_sep_lon / math.sqrt(2)

    grid = {}
    points = []

    def to_cell(lat, lon):
        i = int((lat - LAT_MIN) / cell_lat)
        j = int((lon - LON_MIN) / cell_lon)
        return i, j

    def in_box(lat, lon):
        return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX

    def far_enough(lat, lon):
        ci, cj = to_cell(lat, lon)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                key = (ci + di, cj + dj)
                if key in grid:
                    for (plat, plon) in grid[key]:
                        dlat = (lat - plat) * 111.0
                        dlon = (lon - plon) * 111.0 * BOX_COS
                        if math.hypot(dlat, dlon) < min_sep_km:
                            return False
        return True

    # seed with initial point
    lat0 = rnd.uniform(LAT_MIN, LAT_MAX)
    lon0 = rnd.uniform(LON_MIN, LON_MAX)
    points.append((lat0, lon0))
    grid[to_cell(lat0, lon0)] = [(lat0, lon0)]
    active = [(lat0, lon0)]

    k = 20
    while active and len(points) < n_target:
        idx = rnd.randrange(len(active))
        plat, plon = active[idx]
        found = False
        for _ in range(k):
            r_km = min_sep_km * (1 + rnd.random())
            r_lat = km_to_deg_lat(r_km)
            r_lon = km_to_deg_lon(r_km)
            theta = rnd.uniform(0, 2*math.pi)
            lat = plat + r_lat * math.sin(theta)
            lon = plon + r_lon * math.cos(theta)
            if in_box(lat, lon) and far_enough(lat, lon):
                points.append((lat, lon))
                active.append((lat, lon))
                cell = to_cell(lat, lon)
                grid.setdefault(cell, []).append((lat, lon))
                found = True
                break
        if not found:
            active.pop(idx)

        if len(points) >= n_target:
            break

    return points

@lru_cache(maxsize=8)
def sensor_points(n_target: int = 1000, min_sep_km: float = 0.7, seed: int = 4372):
    return _poisson_disk_points(n_target, min_sep_km, seed)

def pmgid_base_lon_lat(pmgid: str, n_target: int = 1000, min_sep_km: float = 0.7, seed: int = 4372):
    pts = sensor_points(n_target=n_target, min_sep_km=min_sep_km, seed=seed)
    try:
        idx = int(pmgid[-5:]) - 1
    except Exception:
        idx = 0
    lat, lon = pts[idx % len(pts)]
    return lon, lat


# ---- timezone helpers ----
try:
    from zoneinfo import ZoneInfo
    TZ_CHICAGO = ZoneInfo("America/Chicago")
except Exception:
    TZ_CHICAGO = None  # fallback

def utc_today_iso_date():
    return datetime.now(timezone.utc).date().isoformat()

def time_of_day_with_ms(base: datetime, ms_jitter: int = 0) -> str:
    t = base + timedelta(milliseconds=ms_jitter)
    return t.strftime("%H:%M:%S.") + f"{int(t.microsecond/1000):03d}"

def now_local_chicago() -> datetime:
    utc_now = datetime.now(timezone.utc)
    if TZ_CHICAGO:
        return utc_now.astimezone(TZ_CHICAGO)
    return utc_now


# ---- deterministic geo via hash ----
def stable_hash(s: str) -> int:
    return int(hashlib.sha256(s.encode()).hexdigest(), 16)

LON_MIN, LON_MAX = -97.05, -96.55
LAT_MIN, LAT_MAX =  32.60,  33.10

def pmgid_base_lon_lat(pmgid: str):
    h = stable_hash(pmgid)
    lon = LON_MIN + (h % 10_000) / 10_000 * (LON_MAX - LON_MIN)
    lat = LAT_MIN + ((h // 10_000) % 10_000) / 10_000 * (LAT_MAX - LAT_MIN)
    return lon, lat


# ---- daypart logic ----
def daypart_multipliers(local_dt: datetime):
    hr = local_dt.hour

    if hr >= 23 or hr <= 5:
        speed_mult  = random.uniform(1.10, 1.20)
        volume_mult = random.uniform(0.30, 0.50)
        return speed_mult, volume_mult

    rush_blocks = [(7,10), (11,14), (16,19)]
    in_rush = any(start <= hr < end for (start, end) in rush_blocks)
    if in_rush:
        speed_mult  = random.uniform(0.60, 0.80)
        volume_mult = random.uniform(1.50, 2.00)
        return speed_mult, volume_mult

    speed_mult  = random.uniform(0.90, 1.10)
    volume_mult = 1.00
    return speed_mult, volume_mult


# ---- burst generator ----
def generate_burst_for_sensor(pmgid: str, base_dt_utc: datetime, cars_in_burst: int):
    lon0, lat0 = pmgid_base_lon_lat(pmgid)
    fixed_location = f"{lat0:.6f},{lon0:.6f}"

    direction = 1 if random.random() < 0.5 else 0
    base_speed = random.gauss(40, 5)

    rows = []
    for i in range(cars_in_burst):
        ms_jitter = i * random.choice([5, 7, 9, 11])

        rec = {
            "created_at": utc_today_iso_date(),
            "timestamp":  time_of_day_with_ms(base_dt_utc, ms_jitter),
            "peakspeed":  max(1.0, random.gauss(base_speed, 2.0)),
            "pmgid":      pmgid,
            "direction":  direction,
            "location":   fixed_location,
            "vehiclecount": 1,
        }

        rec = new_trace() | rec
        rows.append(rec)

    return rows


# ---- main generator ----
def generate_records(num_records: int):
    emitted = 0
    pmg_index = 0

    while emitted < num_records:
        pmg_index += 1
        pmgid = f"PMG{pmg_index:05d}"

        base_cars = random.randint(3, 5)

        local_now = now_local_chicago()
        speed_mult, volume_mult = daypart_multipliers(local_now)

        cars = max(1, int(round(base_cars * volume_mult)))

        now_utc = datetime.now(timezone.utc)

        burst_rows = generate_burst_for_sensor(pmgid, now_utc, cars)

        for row in burst_rows:
            row["peakspeed"] = max(1.0, row["peakspeed"] * speed_mult)
            yield row
            emitted += 1
            if emitted >= num_records:
                break
