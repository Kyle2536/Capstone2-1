# KafKanem — Local Run Guide (Windows + VS Code)

This README documents **exactly how I ran the project end‑to‑end** on my Windows machine from the folder:

- `C:\Capstone2-1\KafKanem`

It includes the same run commands I used during setup/troubleshooting and the final run order for a working demo.

---

## What this project does
- **Producer** generates simulated traffic detections and publishes them to **Kafka**
- **Consumer** reads Kafka messages and writes them to **MySQL**
- **Analytics API** serves `/api/*` endpoints used by the dashboard
- **Dashboard** is served as a static page (`index.html`)

---

## Prerequisites
Install / have running:
- **Python 3.x**
- **Docker Desktop** (for Kafka + Zookeeper via `docker compose`)
- **VS Code** (recommended)

---

## 1) Open the project folder
Open a terminal and go to the repo:

```powershell
cd C:\Capstone2-1\KafKanem
```

---

## 2) Create + activate a virtual environment
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

---

## 3) Install dependencies
Install from `requirements.txt`:

```powershell
python -m pip install -r requirements.txt
```

If you get missing-package errors later, install these (these were required in my run):
```powershell
python -m pip install lz4 pymysql
```

---

## 4) Configure `.env`
Create (or edit) `C:\Capstone2-1\KafKanem\.env` with your settings.

Typical keys used by the scripts:

```env
# Kafka
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=traffic_raw.v2
KAFKA_GROUP=kafka-rush-consumer
KAFKA_AUTO_OFFSET_RESET=latest

# Producer
RUN_FOREVER=1

# API
API_PORT=5001

# MySQL
DB_HOST=your_mysql_host
DB_PORT=3306
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=kafka
```

> Note: The project expects the producer + consumer to use the **same** topic name.
> I used `traffic_raw.v2` (underscore).

---

## 5) Start Kafka (Docker)
From the repo folder:

```powershell
docker compose up -d
```

Confirm containers are up:

```powershell
docker compose ps
```

---

# Run Everything (Start → Finish)

I ran the following commands (each in its **own terminal**) while the virtualenv was active.

> **Important:** I used the dashboard at **http://localhost:8080/** because the other approach didn’t work reliably in my environment.

---

## Terminal 1 — Start the dashboard static server
This serves `index.html` (the dashboard).

### Command I ran during setup
```powershell
python -m http.server 5001
```

### What I used for the demo (recommended)
Port **5001** can conflict with the Analytics API (which also uses 5001). For my working demo I used:

```powershell
python -m http.server 8080
```

Open:
- **http://localhost:8080/**

---

## Terminal 2 — Start the Analytics API
This serves `/api/*` endpoints used by the dashboard.

```powershell
python analytics_api.py
```

If you need to verify it’s up:
- http://localhost:5001/api/health
- http://localhost:5001/api/kpi?mins=15

---

## Terminal 3 — Start the Consumer (Kafka → MySQL)
```powershell
python consumer_kafka.py
```

You should see logs like:
- `[consumer] wrote 200 rows ...`

---

## Terminal 4 — Start the Producer (Generate → Kafka)
```powershell
python producer_kafka.py
```

With `RUN_FOREVER=1`, the producer should keep running continuously.

---

# Quick Demo Checklist
1. `docker compose up -d` (Kafka online)
2. Start `python analytics_api.py` (API online)
3. Start `python consumer_kafka.py` (writing to MySQL)
4. Start `python producer_kafka.py` (publishing to Kafka)
5. Start `python -m http.server 8080` (dashboard)
6. Open **http://localhost:8080/**

---

# Troubleshooting

## Dashboard shows “No live detections / data is stale”
This usually means the DB isn’t getting *new* rows recently.
Check:
- producer is still running
- consumer is still running
- Kafka topic is advancing

---

## Kafka topic mismatch (dot vs underscore)
If the producer writes to a different topic than the consumer reads, nothing looks “live”.
In `.env`, keep both aligned, e.g.:

```env
KAFKA_TOPIC=traffic_raw.v2
```

---

## Consumer error: Duplicate entry for `trace_id`
Example:
```
Duplicate entry '...' for key 'kafka_pipeline_rush.trace_id'
```

This happens if older Kafka messages are replayed and the DB already contains that `trace_id`.

### Demo reset (clean start)
1) Stop producer + consumer
2) Truncate the RUSH tables in MySQL:

```sql
TRUNCATE TABLE kafka_latencies_rush;
TRUNCATE TABLE kafka_pipeline_rush;
```

3) (Optional) Use a fresh consumer group for the demo:
```env
KAFKA_GROUP=kafka-rush-consumer-demo
KAFKA_AUTO_OFFSET_RESET=latest
```

4) Start consumer, then producer again.

---

## “requirements.txt not found”
Make sure you run installs from:
```powershell
cd C:\Capstone2-1\KafKanem
```

---

# Stop Everything
- Stop Python scripts with **Ctrl+C** in each terminal
- Stop Kafka containers:

```powershell
docker compose down
```
