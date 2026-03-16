# Module 7 Homework — Streaming with PyFlink

## Setup

- **Broker:** Redpanda v25.3.9 (Kafka-compatible, single binary, no ZooKeeper)
- **Stream processor:** Apache Flink 2.2.0 (PyFlink)
- **Sink:** PostgreSQL 18
- **Data:** NYC Green Taxi trips, October 2019 (476,386 rows)
- **Environment:** Docker Compose running locally on Ubuntu Linux

---

## Question 1: Redpanda Version

Run `rpk version` inside the Redpanda container to find the version:

```bash
docker compose exec redpanda rpk version
```

Output:

```
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

**Answer: v25.3.9**

---

## Question 2: Creating a Topic

Before producing messages, a topic needs to be created using the `rpk` CLI tool inside the Redpanda container:

```bash
docker compose exec redpanda rpk topic create green-trips
```

Output:

```
TOPIC        STATUS
green-trips  OK
```

**Answer: TOPIC: green-trips, STATUS: OK**

---

## Question 3: Connecting to the Kafka Server

To verify connectivity to the Redpanda broker using the Kafka Python client:

```python
import json
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print(producer.bootstrap_connected())
```

Output:

```
True

```

**Answer: True**

---

## Question 4: Sending the Trip Data

Read the full Green Taxi October 2019 dataset and send all rows to the `green-trips` topic, keeping only these columns:

- `lpep_pickup_datetime`
- `lpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `tip_amount`
- `total_amount`

NaN values in `passenger_count` are replaced with `None` before serialization — Flink's JSON parser rejects the non-standard `NaN` token:

```python
import json
import math
import pandas as pd
from kafka import KafkaProducer
from time import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'PULocationID', 'DOLocationID', 'passenger_count',
    'trip_distance', 'tip_amount', 'total_amount'
]

df = pd.read_csv('data/green_tripdata_2019-10.csv.gz', usecols=columns)
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

t0 = time()

for _, row in df.iterrows():
    message = {k: (None if isinstance(v, float) and math.isnan(v) else v)
               for k, v in row.to_dict().items()}
    producer.send('green-trips', value=message)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

Output:

```
Loaded 476386 rows
Done! Took 119.48 seconds to send 476386 messages
```

**Answer: ~120 seconds**

---

## Question 5: Consumer — Trip Distance

A Kafka consumer reads all messages from `green-trips` with `auto_offset_reset='earliest'` and counts how many trips have `trip_distance > 5.0`:

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='trip-distance-counter',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000
)

total = 0
count_gt5 = 0

for message in consumer:
    trip = message.value
    total += 1
    if float(trip.get('trip_distance', 0) or 0) > 5.0:
        count_gt5 += 1

print(f'Total trips processed: {total}')
print(f'Trips with trip_distance > 5.0: {count_gt5}')
```

Output:

```
Total trips processed: 476386
Trips with trip_distance > 5.0: 99164
```

**Answer: 99,164 trips**

---

## Question 6: Tumbling Window — Pickup Location

A PyFlink job reads from `green-trips` and applies a 5-minute tumbling window to count trips per `PULocationID`. Results are written to PostgreSQL.

The source table uses `lpep_pickup_datetime` as event time with a 5-second watermark tolerance:

```sql
event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
```

Window aggregation:

```sql
INSERT INTO trips_per_location_5min
SELECT
    window_start,
    PULocationID,
    COUNT(*) AS num_trips
FROM TABLE(
    TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
)
GROUP BY window_start, PULocationID
```

Job submitted with parallelism 1:

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/tumbling_location_job.py \
    --pyFiles /opt/src -d
```

Query results:

```sql
SELECT PULocationID, num_trips
FROM trips_per_location_5min
ORDER BY num_trips DESC
LIMIT 3;
```

```
 pulocationid | num_trips
--------------+-----------
           74 |        28
           74 |        26
           74 |        26
```

**Answer: PULocationID = 74**

---

## Question 7: Session Window — Longest Streak

A PyFlink job uses a session window with a 5-minute gap on `PULocationID`, grouping consecutive trips with less than 5 minutes of inactivity into a single session.

Window aggregation:

```sql
INSERT INTO trips_session_window
SELECT
    PULocationID,
    COUNT(*) AS num_trips,
    window_start,
    window_end
FROM TABLE(
    SESSION(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
)
GROUP BY PULocationID, window_start, window_end
```

The sink table uses a composite primary key on `(PULocationID, window_start)` with `NOT ENFORCED` to enable upsert behavior for late-arriving events.

Job submitted with parallelism 1 and checkpointing disabled to avoid checkpoint timeout errors on large session state:

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/session_location_job.py \
    --pyFiles /opt/src -d
```

Query results:

```sql
SELECT PULocationID, num_trips
FROM trips_session_window
ORDER BY num_trips DESC
LIMIT 5;
```

```
 pulocationid | num_trips
--------------+-----------
           74 |       852
           75 |       772
           75 |       716
           74 |       689
           75 |       675
```

**Answer: 852 trips in the longest session (PULocationID = 74)**

---

## Question 8: Tumbling Window — Largest Tip

A PyFlink job uses a 1-hour tumbling window to compute the total `tip_amount` across all pickup locations per hour.

Window aggregation:

```sql
INSERT INTO tip_per_hour
SELECT
    window_start,
    SUM(tip_amount) AS total_tip
FROM TABLE(
    TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
)
GROUP BY window_start
```

Job submitted:

```bash
docker compose exec jobmanager ./bin/flink run \
    -py /opt/src/job/tip_per_hour_job.py \
    --pyFiles /opt/src -d
```

Query results:

```sql
SELECT window_start, ROUND(total_tip::numeric, 2) AS total_tip
FROM tip_per_hour
ORDER BY total_tip DESC
LIMIT 5;
```

```
    window_start     | total_tip
---------------------+-----------
 2019-10-11 17:00:00 |   1524.61
 2019-10-16 17:00:00 |   1472.59
 2019-10-16 19:00:00 |   1450.95
 2019-10-18 18:00:00 |   1441.16
 2019-10-16 18:00:00 |   1428.15
```

**Answer: 2019-10-11 17:00:00 with total tip of $1,524.61**
