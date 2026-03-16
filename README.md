# Streaming with PyFlink and Redpanda

Module 7 of the DataTalksClub Data Engineering Zoomcamp 2026.

This project builds a real-time streaming pipeline using PyFlink and Redpanda (Kafka-compatible broker), processing NYC Green Taxi trip data through tumbling and session windows, and writing results to PostgreSQL.

## Pipeline Architecture

```
NYC Green Taxi Data (CSV)
        |
        v
   Python Producer
        |
        v
  Redpanda (Kafka)
   [green-trips]
        |
        v
   Apache Flink
  (PyFlink Jobs)
        |
        v
   PostgreSQL
```

## Stack

| Component | Tool | Version |
|-----------|------|---------|
| Message Broker | Redpanda | v25.3.9 |
| Stream Processor | Apache Flink (PyFlink) | 2.2.0 |
| Sink | PostgreSQL | 18 |
| Python Client | kafka-python | 2.3.0 |
| Container Runtime | Docker + Docker Compose | - |

## Dataset

NYC Green Taxi trips for October 2019 sourced from the DataTalksClub NYC TLC data releases.

- Rows: 476,386
- Columns used: `lpep_pickup_datetime`, `lpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `passenger_count`, `trip_distance`, `tip_amount`, `total_amount`

## Project Structure

```
.
├── docker-compose.yml          # Redpanda, Flink, PostgreSQL, pgAdmin services
├── Dockerfile.flink            # Custom Flink image with PyFlink + connector JARs
├── flink-config.yaml           # Flink JVM metaspace config
├── pyproject.flink.toml        # PyFlink dependencies
├── pyproject.toml              # Project dependencies
├── StreamingHomework.md        # Homework answers
├── data/                       # Cached local data files
└── src/
    ├── producers/
    │   ├── producer_green.py   # Sends green taxi data to Kafka
    │   └── test_connection.py  # Verifies Kafka connectivity
    ├── consumers/
    │   ├── consumer.py         # Console consumer (prints messages)
    │   ├── consumer_postgres.py        # Writes messages to PostgreSQL
    │   └── consumer_count.py   # Counts trips with distance > 5km
    └── job/
        ├── pass_through_job.py         # Flink pass-through: Kafka -> PostgreSQL
        ├── tumbling_location_job.py    # 5-min tumbling window by PULocationID
        ├── session_location_job.py     # Session window by PULocationID
        └── tip_per_hour_job.py         # 1-hour tumbling window for tip totals
```

## Prerequisites

- Docker and Docker Compose
- Python 3.12
- uv (Python package manager)

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/Derrick-Ryan-Giggs/Streaming-Homework.git
cd Streaming-Homework
```

### 2. Install Python dependencies

```bash
UV_HTTP_TIMEOUT=300 uv add kafka-python pandas pyarrow psycopg2-binary
uv pip install -e .
```

### 3. Build the custom Flink Docker image and start all services

```bash
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop/Dockerfile.flink
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop/pyproject.flink.toml
wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop/flink-config.yaml

docker compose up --build -d
```

First build takes 5-10 minutes — it installs Python, PyFlink, and downloads the Kafka and JDBC connector JARs inside the image.

### 4. Verify all services are running

```bash
docker compose ps
```

Expected output:

```
pyflink-workshop-jobmanager-1    Up    0.0.0.0:8081->8081/tcp
pyflink-workshop-taskmanager-1   Up
pyflink-workshop-postgres-1      Up    0.0.0.0:5432->5432/tcp
pyflink-workshop-redpanda-1      Up    0.0.0.0:9092->9092/tcp
pyflink-workshop-pgadmin-1       Up    0.0.0.0:8080->80/tcp
```

### 5. Create the Kafka topic

```bash
docker compose exec redpanda rpk topic create green-trips --partitions 1
```

### 6. Create PostgreSQL tables

```bash
docker compose exec postgres psql -U postgres -d postgres -c "
CREATE TABLE trips_per_location_5min (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

CREATE TABLE trips_session_window (
    PULocationID INTEGER,
    num_trips BIGINT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PRIMARY KEY (PULocationID, window_start)
);

CREATE TABLE tip_per_hour (
    window_start TIMESTAMP,
    total_tip DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);"
```

### 7. Send data to Kafka

```bash
python src/producers/producer_green.py
```

Downloads and caches the dataset on first run (~476k rows, ~120 seconds to send).

## Running the Flink Jobs

Submit jobs to the Flink cluster using the jobmanager container. The `src/` directory is mounted at `/opt/src` inside the containers.

### Pass-through job (Kafka to PostgreSQL)

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/pass_through_job.py \
    --pyFiles /opt/src -d
```

### 5-minute tumbling window by pickup location

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/tumbling_location_job.py \
    --pyFiles /opt/src -d
```

### Session window by pickup location

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/session_location_job.py \
    --pyFiles /opt/src -d
```

### 1-hour tumbling window for tip totals

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/tip_per_hour_job.py \
    --pyFiles /opt/src -d
```

### Monitor running jobs

```bash
docker compose exec jobmanager ./bin/flink list
```

### Cancel a job

```bash
docker compose exec jobmanager ./bin/flink cancel <JOB_ID>
```

## Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| Flink Dashboard | http://localhost:8081 | - |
| pgAdmin | http://localhost:8080 | admin@admin.com / admin |

## Key Concepts Covered

**Redpanda** — Kafka-compatible broker written in C++. No JVM, no ZooKeeper. Single binary with much lower resource overhead than Kafka, making it ideal for local development.

**PyFlink Table API** — SQL-style stream processing using Flink's Table API. Defines sources and sinks as DDL tables and expresses pipelines as SQL INSERT INTO ... SELECT statements.

**Watermarks** — Tell Flink when to close a time window. The watermark trails the latest event timestamp by a configurable tolerance (5 seconds here), allowing late-arriving events to still be counted in the correct window.

**Tumbling Windows** — Fixed-size, non-overlapping time windows. Every event belongs to exactly one window. Used here for 5-minute trip counts and 1-hour tip totals.

**Session Windows** — Dynamic windows that close after a configurable gap of inactivity. Unlike tumbling windows, session windows have no fixed size — they grow as long as events keep arriving within the gap threshold. Used here to find unbroken streaks of taxi activity per pickup location.

**Upsert sink** — PostgreSQL sink tables with a PRIMARY KEY and NOT ENFORCED enable upsert behavior. When Flink emits a corrected result for a window (due to late-arriving events), the existing row is updated rather than duplicated.

**Checkpointing** — Flink periodically snapshots job state (Kafka offsets + open windows) to disk. If a job fails and restarts, it resumes from the last checkpoint rather than reprocessing from the beginning. Disabled for the session job in this project due to large state size.

## Teardown

```bash
docker compose down
```

To also remove the PostgreSQL data volume:

```bash
docker compose down -v
```

## Homework Results

See [StreamingHomework.md](StreamingHomework.md) for full answers with commands and outputs.

